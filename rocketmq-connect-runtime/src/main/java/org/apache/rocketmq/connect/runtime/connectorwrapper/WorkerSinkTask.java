/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.rocketmq.connect.runtime.connectorwrapper;

import com.alibaba.fastjson.JSON;
import io.openmessaging.KeyValue;
import io.openmessaging.connector.api.component.task.sink.SinkTask;
import io.openmessaging.connector.api.component.task.sink.SinkTaskContext;
import io.openmessaging.connector.api.data.ConnectRecord;
import io.openmessaging.connector.api.data.Converter;
import io.openmessaging.connector.api.data.RecordOffset;
import io.openmessaging.connector.api.data.RecordPartition;
import io.openmessaging.connector.api.data.Schema;
import io.openmessaging.connector.api.errors.RetriableException;
import io.openmessaging.connector.api.storage.OffsetStorageReader;
import io.openmessaging.internal.DefaultKeyValue;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.client.consumer.DefaultMQPullConsumer;
import org.apache.rocketmq.client.consumer.PullResult;
import org.apache.rocketmq.client.consumer.PullStatus;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.connect.runtime.common.ConnectKeyValue;
import org.apache.rocketmq.connect.runtime.common.LoggerName;
import org.apache.rocketmq.connect.runtime.config.RuntimeConfigDefine;
import org.apache.rocketmq.connect.runtime.converter.RocketMQConverter;
import org.apache.rocketmq.connect.runtime.service.PositionManagementService;
import org.apache.rocketmq.connect.runtime.store.PositionStorageReaderImpl;
import org.apache.rocketmq.connect.runtime.utils.ConnectUtil;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A wrapper of {@link SinkTask} for runtime.
 */
public class WorkerSinkTask implements WorkerTask {

    private static final Logger log = LoggerFactory.getLogger(LoggerName.ROCKETMQ_RUNTIME);

    /**
     * The configuration key that provides the list of topicNames that are inputs for this SinkTask.
     */
    public static final String QUEUENAMES_CONFIG = "topicNames";

    /**
     * The configuration key that provide the list of topicQueues that are inputs for this SinkTask;
     * The config value format is topicName1,brokerName1,queueId1;topicName2,brokerName2,queueId2,
     * use topicName1, brokerName1, queueId1 can construct {@link MessageQueue}
     */
    public static final String TOPIC_QUEUES_CONFIG = "topicQueues";

    /**
     * Connector name of current task.
     */
    private String connectorName;

    /**
     * The implements of the sink task.
     */
    private SinkTask sinkTask;

    /**
     * The configs of current sink task.
     */
    private ConnectKeyValue taskConfig;


    /**
     * Atomic state variable
     */
    private AtomicReference<WorkerTaskState> state;

    /**
     * Stop retry limit
     */



    /**
     * A RocketMQ consumer to pull message from MQ.
     */
    private final DefaultMQPullConsumer consumer;

    private final PositionManagementService offsetManagementService;

    /**
     *
     */
    private final OffsetStorageReader offsetStorageReader;

    /**
     * A converter to parse sink data entry to object.
     */
    private Converter recordConverter;

    private final ConcurrentHashMap<MessageQueue, Long> messageQueuesOffsetMap;

    private final ConcurrentHashMap<MessageQueue, QueueState> messageQueuesStateMap;

    private static final Integer TIMEOUT = 3 * 1000;

    private static final Integer MAX_MESSAGE_NUM = 32;

    private static final String COMMA = ",";
    private static final String SEMICOLON = ";";

    public static final String OFFSET_COMMIT_TIMEOUT_MS_CONFIG = "offset.flush.timeout.ms";

    private long nextCommitTime = 0;

    private final AtomicReference<WorkerState> workerState;

    private final TransformChain<ConnectRecord> transformChain;

    public static final String BROKER_NAME = "brokerName";
    public static final String QUEUE_ID = "queueId";
    public static final String TOPIC = "topic";
    public static final String QUEUE_OFFSET = "queueOffset";

    public WorkerSinkTask(String connectorName,
        SinkTask sinkTask,
        ConnectKeyValue taskConfig,
        PositionManagementService offsetManagementService,
        Converter recordConverter,
        DefaultMQPullConsumer consumer,
        AtomicReference<WorkerState> workerState,
        TransformChain<ConnectRecord> transformChain) {
        this.connectorName = connectorName;
        this.sinkTask = sinkTask;
        this.taskConfig = taskConfig;
        this.consumer = consumer;
        this.offsetManagementService = offsetManagementService;
        this.offsetStorageReader = new PositionStorageReaderImpl(offsetManagementService);
        this.recordConverter = recordConverter;
        this.messageQueuesOffsetMap = new ConcurrentHashMap<>(256);
        this.messageQueuesStateMap = new ConcurrentHashMap<>(256);
        this.state = new AtomicReference<>(WorkerTaskState.NEW);
        this.workerState = workerState;
        this.transformChain = transformChain;
    }

    /**
     * Start a sink task, and receive data entry from MQ cyclically.
     */
    @Override
    public void run() {
        try {
            consumer.start();
            log.info("Sink task consumer start.");
            state.compareAndSet(WorkerTaskState.NEW, WorkerTaskState.PENDING);
            sinkTask.init(taskConfig);
            String topicNamesStr = taskConfig.getString(QUEUENAMES_CONFIG);
            String topicQueuesStr = taskConfig.getString(TOPIC_QUEUES_CONFIG);

            if (!StringUtils.isEmpty(topicNamesStr)) {
                String[] topicNames = topicNamesStr.split(COMMA);
                for (String topicName : topicNames) {
                    final Set<MessageQueue> messageQueues = consumer.fetchSubscribeMessageQueues(topicName);
                    for (MessageQueue messageQueue : messageQueues) {
                        final long offset = consumer.searchOffset(messageQueue, TIMEOUT);
                        messageQueuesOffsetMap.put(messageQueue, offset);
                    }
                    messageQueues.addAll(messageQueues);
                }
                log.debug("{} Initializing and starting task for topicNames {}", this, topicNames);
            } else if (!StringUtils.isEmpty(topicQueuesStr)) {
                String[] topicQueues = topicQueuesStr.split(SEMICOLON);
                for (String messageQueueStr : topicQueues) {
                    String[] items = messageQueueStr.split(COMMA);
                    if (items.length != 3) {
                        log.error("Topic queue format error, topicQueueStr : " + topicNamesStr);
                        return;
                    }
                    MessageQueue messageQueue = new MessageQueue(items[0], items[1], Integer.valueOf(items[2]));
                    final long offset = consumer.searchOffset(messageQueue, TIMEOUT);
                    messageQueuesOffsetMap.put(messageQueue, offset);
                }
            } else {
                log.error("Lack of sink comsume topicNames config");
                state.set(WorkerTaskState.ERROR);
                return;
            }

            for (Map.Entry<MessageQueue, Long> entry : messageQueuesOffsetMap.entrySet()) {
                MessageQueue messageQueue = entry.getKey();
                RecordOffset recordOffset = offsetStorageReader.readOffset(ConnectUtil.convertToRecordPartition(messageQueue));
                if (null != recordOffset) {
                    messageQueuesOffsetMap.put(messageQueue, ConnectUtil.convertToOffset(recordOffset));
                }
            }

            sinkTask.start(new SinkTaskContext() {
                @Override
                public void resetOffset(RecordPartition recordPartition, RecordOffset recordOffset) {
                    if (null == recordPartition || null == recordPartition.getPartition() || null == recordOffset || null == recordOffset.getOffset()) {
                        log.warn("recordPartition {} info is null or recordOffset {} info is null", recordPartition, recordOffset);
                        return;
                    }
                    String brokerName = (String) recordPartition.getPartition().get(BROKER_NAME);
                    String topic = (String) recordPartition.getPartition().get(TOPIC);
                    Integer queueId = Integer.valueOf((String) recordPartition.getPartition().get(QUEUE_ID));
                    if (StringUtils.isEmpty(brokerName) || StringUtils.isEmpty(topic) || null == queueId) {
                        log.warn("brokerName is null or queueId is null or queueName is null, brokerName {}, queueId {} queueId {}", brokerName, queueId, topic);
                        return;
                    }
                    MessageQueue messageQueue = new MessageQueue(topic, brokerName, queueId);
                    if (!messageQueuesOffsetMap.containsKey(messageQueue)) {
                        log.warn("sink task current messageQueuesOffsetMap {} not contain messageQueue {}", messageQueuesOffsetMap, messageQueue);
                        return;
                    }
                    Long offset = Long.valueOf((String) recordOffset.getOffset().get(QUEUE_OFFSET));
                    if (null == offset) {
                        log.warn("resetOffset, offset is null");
                        return;
                    }
                    messageQueuesOffsetMap.put(messageQueue, offset);
                    try {
                        consumer.updateConsumeOffset(messageQueue, offset);
                    } catch (MQClientException e) {
                        log.warn("updateConsumeOffset MQClientException, messageQueue {}, offset {}", JSON.toJSONString(messageQueue), offset, e);
                    }
                }

                @Override
                public void resetOffset(Map<RecordPartition, RecordOffset> offsets) {
                    if (MapUtils.isEmpty(offsets)) {
                        log.warn("resetOffset, offsets {} is null", offsets);
                        return;
                    }
                    for (Map.Entry<RecordPartition, RecordOffset> entry : offsets.entrySet()) {
                        if (null == entry || null == entry.getKey() || null == entry.getKey().getPartition() || null == entry.getValue() || null == entry.getValue().getOffset()) {
                            log.warn("recordPartition {} info is null or recordOffset {} info is null, entry {}", entry);
                            continue;
                        }
                        RecordPartition recordPartition = entry.getKey();
                        String brokerName = (String) recordPartition.getPartition().get(BROKER_NAME);
                        String topic = (String) recordPartition.getPartition().get(TOPIC);
                        Integer queueId = Integer.valueOf((String) recordPartition.getPartition().get(QUEUE_ID));
                        if (StringUtils.isEmpty(brokerName) || StringUtils.isEmpty(topic) || null == queueId) {
                            log.warn("brokerName is null or queueId is null or queueName is null, brokerName {}, queueId {} queueId {}", brokerName, queueId, topic);
                            continue;
                        }
                        MessageQueue messageQueue = new MessageQueue(topic, brokerName, queueId);
                        if (!messageQueuesOffsetMap.containsKey(messageQueue)) {
                            log.warn("sink task current messageQueuesOffsetMap {} not contain messageQueue {}", messageQueuesOffsetMap, messageQueue);
                            continue;
                        }
                        RecordOffset recordOffset = entry.getValue();
                        Long offset = Long.valueOf((String) recordOffset.getOffset().get(QUEUE_OFFSET));
                        if (null == offset) {
                            log.warn("resetOffset, offset is null");
                            continue;
                        }
                        messageQueuesOffsetMap.put(messageQueue, offset);
                        try {
                            consumer.updateConsumeOffset(messageQueue, offset);
                        } catch (MQClientException e) {
                            log.warn("updateConsumeOffset MQClientException, messageQueue {}, offset {}", JSON.toJSONString(messageQueue), entry.getValue(), e);
                        }
                    }
                }

                @Override
                public void pause(List<RecordPartition> recordPartitions) {
                    if (recordPartitions == null || recordPartitions.size() == 0) {
                        log.warn("recordPartitions is null or recordPartitions.size() is zero. recordPartitions {}", JSON.toJSONString(recordPartitions));
                        return;
                    }
                    for (RecordPartition recordPartition : recordPartitions) {
                        if (null == recordPartition || null == recordPartition.getPartition()) {
                            log.warn("recordPartition {} info is null", recordPartition);
                            continue;
                        }
                        String brokerName = (String) recordPartition.getPartition().get(BROKER_NAME);
                        String topic = (String) recordPartition.getPartition().get(TOPIC);
                        Integer queueId = Integer.valueOf((String) recordPartition.getPartition().get(QUEUE_ID));
                        if (StringUtils.isEmpty(brokerName) || StringUtils.isEmpty(topic) || null == queueId) {
                            log.warn("brokerName is null or queueId is null or queueName is null, brokerName {}, queueId {} queueId {}", brokerName, queueId, topic);
                            continue;
                        }
                        MessageQueue messageQueue = new MessageQueue(topic, brokerName, queueId);
                        if (!messageQueuesOffsetMap.containsKey(messageQueue)) {
                            log.warn("sink task current messageQueuesOffsetMap {} not contain messageQueue {}", messageQueuesOffsetMap, messageQueue);
                            continue;
                        }
                        messageQueuesStateMap.put(messageQueue, QueueState.PAUSE);
                    }
                }

                @Override
                public void resume(List<RecordPartition> recordPartitions) {
                    if (recordPartitions == null || recordPartitions.size() == 0) {
                        log.warn("recordPartitions is null or recordPartitions.size() is zero. recordPartitions {}", JSON.toJSONString(recordPartitions));
                        return;
                    }
                    for (RecordPartition recordPartition : recordPartitions) {
                        if (null == recordPartition || null == recordPartition.getPartition()) {
                            log.warn("recordPartition {} info is null", recordPartition);
                            continue;
                        }
                        String brokerName = (String) recordPartition.getPartition().get(BROKER_NAME);
                        String topic = (String) recordPartition.getPartition().get(TOPIC);
                        Integer queueId = Integer.valueOf((String) recordPartition.getPartition().get(QUEUE_ID));
                        if (StringUtils.isEmpty(brokerName) || StringUtils.isEmpty(topic) || null == queueId) {
                            log.warn("brokerName is null or queueId is null or queueName is null, brokerName {}, queueId {} queueId {}", brokerName, queueId, topic);
                            continue;
                        }
                        MessageQueue messageQueue = new MessageQueue(topic, brokerName, queueId);
                        if (!messageQueuesOffsetMap.containsKey(messageQueue)) {
                            log.warn("sink task current messageQueuesOffsetMap {} not contain messageQueue {}", messageQueuesOffsetMap, messageQueue);
                            continue;
                        }
                        messageQueuesStateMap.remove(messageQueue);
                    }
                }

                @Override public Set<RecordPartition> assignment() {
                    return null;
                }

                @Override public String getConnectorName() {
                    return taskConfig.getString("connectorName");
                }

                @Override public String getTaskName() {
                    return taskConfig.getString("taskId");
                }
            });
            // we assume executed here means we are safe
            log.info("Sink task start, config:{}", JSON.toJSONString(taskConfig));
            state.compareAndSet(WorkerTaskState.PENDING, WorkerTaskState.RUNNING);

            while (WorkerState.STARTED == workerState.get() && WorkerTaskState.RUNNING == state.get()) {
                // this method can block up to 3 minutes long
                pullMessageFromQueues();
            }

            sinkTask.stop();
            state.compareAndSet(WorkerTaskState.STOPPING, WorkerTaskState.STOPPED);
            log.info("Sink task stop, config:{}", JSON.toJSONString(taskConfig));

        } catch (Exception e) {
            log.error("Run task failed.", e);
            state.set(WorkerTaskState.ERROR);
        } finally {
            if (consumer != null) {
                consumer.shutdown();
                log.info("Sink task consumer shutdown.");
            }
        }
    }

    private void pullMessageFromQueues() throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        long startTimeStamp = System.currentTimeMillis();
        log.info("START pullMessageFromQueues, time started : {}", startTimeStamp);
        for (Map.Entry<MessageQueue, Long> entry : messageQueuesOffsetMap.entrySet()) {
            if (messageQueuesStateMap.containsKey(entry.getKey())) {
                continue;
            }
            log.info("START pullBlockIfNotFound, time started : {}", System.currentTimeMillis());

            if (WorkerTaskState.RUNNING != state.get()) {
                break;
            }
            final PullResult pullResult = consumer.pullBlockIfNotFound(entry.getKey(), "*", entry.getValue(), MAX_MESSAGE_NUM);
            long currentTime = System.currentTimeMillis();

            log.info("INSIDE pullMessageFromQueues, time elapsed : {}", currentTime - startTimeStamp);
            if (pullResult.getPullStatus().equals(PullStatus.FOUND)) {
                final List<MessageExt> messages = pullResult.getMsgFoundList();
                receiveMessages(messages);
                messageQueuesOffsetMap.put(entry.getKey(), pullResult.getNextBeginOffset());
                offsetManagementService.putPosition(ConnectUtil.convertToRecordPartition(entry.getKey()), ConnectUtil.convertToRecordOffset(pullResult.getNextBeginOffset()));
                preCommit();
            }
        }
    }

    private void preCommit() {
        long commitInterval = taskConfig.getLong(OFFSET_COMMIT_TIMEOUT_MS_CONFIG, 1000);
        if (nextCommitTime <= 0) {
            long now = System.currentTimeMillis();
            nextCommitTime = now + commitInterval;
        }
        if (nextCommitTime < System.currentTimeMillis()) {
            Map<RecordPartition, RecordOffset> queueMetaDataLongMap = new HashMap<>(512);
            if (messageQueuesOffsetMap.size() > 0) {
                for (Map.Entry<MessageQueue, Long> messageQueueLongEntry : messageQueuesOffsetMap.entrySet()) {
                    RecordPartition recordPartition = ConnectUtil.convertToRecordPartition(messageQueueLongEntry.getKey());
                    RecordOffset recordOffset = ConnectUtil.convertToRecordOffset(messageQueueLongEntry.getValue());
                    queueMetaDataLongMap.put(recordPartition, recordOffset);
                }
            }
            sinkTask.preCommit(queueMetaDataLongMap);
            nextCommitTime = 0;
        }
    }

    private void removePauseQueueMessage(MessageQueue messageQueue, List<MessageExt> messages) {
        if (null != messageQueuesStateMap.get(messageQueue)) {
            final Iterator<MessageExt> iterator = messages.iterator();
            while (iterator.hasNext()) {
                final MessageExt message = iterator.next();
                String msgId = message.getMsgId();
                log.info("BrokerName {}, topicName {}, queueId {} is pause, Discard the message {}", messageQueue.getBrokerName(), messageQueue.getTopic(), message.getQueueId(), msgId);
                iterator.remove();
            }
        }
    }


    @Override
    public void stop() {
        state.compareAndSet(WorkerTaskState.RUNNING, WorkerTaskState.STOPPING);
    }

    @Override
    public void cleanup() {
        if (state.compareAndSet(WorkerTaskState.STOPPED, WorkerTaskState.TERMINATED) ||
            state.compareAndSet(WorkerTaskState.ERROR, WorkerTaskState.TERMINATED))
            consumer.shutdown();
        else {
            log.error("[BUG] cleaning a task but it's not in STOPPED or ERROR state");
        }
    }

    /**
     * receive message from MQ.
     *
     * @param messages
     */
    private void receiveMessages(List<MessageExt> messages) {
        List<ConnectRecord> sinkDataEntries = new ArrayList<>(32);
        for (MessageExt message : messages) {
            ConnectRecord sinkDataEntry = convertToSinkDataEntry(message);
            sinkDataEntries.add(sinkDataEntry);
            String msgId = message.getMsgId();
            log.info("Received one message success : msgId {}", msgId);
        }
        List<ConnectRecord> connectRecordList = new ArrayList<>(32);
        for (ConnectRecord connectRecord : sinkDataEntries) {
            ConnectRecord connectRecord1 = this.transformChain.doTransforms(connectRecord);
            if (null != connectRecord1) {
                connectRecordList.add(connectRecord1);
            }
        }
        if (CollectionUtils.isEmpty(connectRecordList)) {
            log.info("after transforms connectRecordList is null");
            return;
        }
        try {
            sinkTask.put(connectRecordList);
            return;
        } catch (RetriableException e) {
            log.error("task {} put sink recode RetriableException", this, e.getMessage(), e);
            throw e;
        } catch (Throwable t) {
            log.error("task {} put sink recode Throwable", this, t.getMessage(), t);
            throw t;
        }

    }

    private ConnectRecord convertToSinkDataEntry(MessageExt message) {
        Map<String, String> properties = message.getProperties();
        Schema schema;
        Long timestamp;
        ConnectRecord sinkDataEntry = null;
        if (null == recordConverter || recordConverter instanceof RocketMQConverter) {
            String connectTimestamp = properties.get(RuntimeConfigDefine.CONNECT_TIMESTAMP);
            timestamp = StringUtils.isNotEmpty(connectTimestamp) ? Long.valueOf(connectTimestamp) : null;
            String connectSchema = properties.get(RuntimeConfigDefine.CONNECT_SCHEMA);
            schema = StringUtils.isNotEmpty(connectSchema) ? JSON.parseObject(connectSchema, Schema.class) : null;
            byte[] body = message.getBody();
            RecordPartition recordPartition = ConnectUtil.convertToRecordPartition(message.getTopic(), message.getBrokerName(), message.getQueueId());

            RecordOffset recordOffset = ConnectUtil.convertToRecordOffset(message.getQueueOffset());

            String bodyStr = new String(body, StandardCharsets.UTF_8);
            sinkDataEntry = new ConnectRecord(recordPartition, recordOffset, timestamp, schema, bodyStr);
            KeyValue keyValue = new DefaultKeyValue();
            if (MapUtils.isNotEmpty(properties)) {
                for (Map.Entry<String, String> entry : properties.entrySet()) {
                    keyValue.put(entry.getKey(), entry.getValue());
                }
            }
            sinkDataEntry.addExtension(keyValue);
        } else {
            final byte[] messageBody = message.getBody();
            String s = new String(messageBody);
            sinkDataEntry = JSON.parseObject(s, ConnectRecord.class);
        }
        return sinkDataEntry;
    }

    @Override
    public String getConnectorName() {
        return connectorName;
    }

    @Override
    public WorkerTaskState getState() {
        return state.get();
    }

    @Override
    public ConnectKeyValue getTaskConfig() {
        return taskConfig;
    }


    /**
     * Further we cant try to log what caused the error
     */
    @Override
    public void timeout() {
        this.state.set(WorkerTaskState.ERROR);
    }
    @Override
    public String toString() {

        StringBuilder sb = new StringBuilder();
        sb.append("connectorName:" + connectorName)
            .append("\nConfigs:" + JSON.toJSONString(taskConfig))
            .append("\nState:" + state.get().toString());
        return sb.toString();
    }

    @Override
    public Object getJsonObject() {
        HashMap obj = new HashMap<String, Object>();
        obj.put("connectorName", connectorName);
        obj.put("configs", JSON.toJSONString(taskConfig));
        obj.put("state", state.get().toString());
        return obj;
    }

    private enum QueueState {
        PAUSE
    }

    private ByteBuffer convertToByteBufferKey(MessageQueue messageQueue) {
        return ByteBuffer.wrap((messageQueue.getTopic() + COMMA + messageQueue.getBrokerName() + COMMA + messageQueue.getQueueId()).getBytes());
    }

    private MessageQueue convertToMessageQueue(ByteBuffer byteBuffer) {
        byte[] array = byteBuffer.array();
        String s = String.valueOf(array);
        String[] split = s.split(COMMA);
        return new MessageQueue(split[0], split[1], Integer.valueOf(split[2]));
    }

    private ByteBuffer convertToByteBufferValue(Long offset) {
        return ByteBuffer.wrap(String.valueOf(offset).getBytes());
    }

    private Long convertToOffset(ByteBuffer byteBuffer) {
        return Long.valueOf(new String(byteBuffer.array()));
    }
}
