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
import io.openmessaging.connector.api.data.ConnectRecord;
import io.openmessaging.connector.api.data.Converter;
import io.openmessaging.connector.api.data.RecordOffset;
import io.openmessaging.connector.api.data.RecordPartition;
import io.openmessaging.connector.api.data.Schema;
import io.openmessaging.connector.api.errors.ConnectException;
import io.openmessaging.connector.api.errors.RetriableException;
import io.openmessaging.internal.DefaultKeyValue;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.client.consumer.DefaultMQPullConsumer;
import org.apache.rocketmq.client.consumer.MessageQueueListener;
import org.apache.rocketmq.client.consumer.PullResult;
import org.apache.rocketmq.client.consumer.PullStatus;
import org.apache.rocketmq.client.consumer.store.ReadOffsetType;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.connect.runtime.common.ConnectKeyValue;
import org.apache.rocketmq.connect.runtime.common.LoggerName;
import org.apache.rocketmq.connect.runtime.common.QueueState;
import org.apache.rocketmq.connect.runtime.config.RuntimeConfigDefine;
import org.apache.rocketmq.connect.runtime.config.SinkConnectorConfig;
import org.apache.rocketmq.connect.runtime.converter.RocketMQConverter;
import org.apache.rocketmq.connect.runtime.stats.ConnectStatsManager;
import org.apache.rocketmq.connect.runtime.stats.ConnectStatsService;
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
     * The configuration key that provide the list of topicQueues that are inputs for this SinkTask; The config value
     * format is topicName1,brokerName1,queueId1;topicName2,brokerName2,queueId2, use topicName1, brokerName1, queueId1
     * can construct {@link MessageQueue}
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

    private Set<RecordPartition> recordPartitions = new CopyOnWriteArraySet<>();

    private long pullMsgErrorCount = 0;

    private static final long PULL_MSG_ERROR_BACKOFF_MS = 1000 * 10;

    private static final long PULL_MSG_ERROR_THRESHOLD = 16;

    private final AtomicReference<WorkerState> workerState;

    private final ConnectStatsManager connectStatsManager;

    private final ConnectStatsService connectStatsService;

    private final CountDownLatch stopPullMsgLatch;

    private WorkerSinkTaskContext sinkTaskContext;

    private final TransformChain<ConnectRecord> transformChain;

    public static final String BROKER_NAME = "brokerName";
    public static final String QUEUE_ID = "queueId";
    public static final String TOPIC = "topic";
    public static final String QUEUE_OFFSET = "queueOffset";

    private static final Set<String> MQ_SYS_KEYS = new HashSet<String>() {
        {
            add("MIN_OFFSET");
            add("TRACE_ON");
            add("MAX_OFFSET");
            add("MSG_REGION");
            add("UNIQ_KEY");
            add("WAIT");
            add("TAGS");
        }
    };

    public WorkerSinkTask(String connectorName,
        SinkTask sinkTask,
        ConnectKeyValue taskConfig,
        Converter recordConverter,
        DefaultMQPullConsumer consumer,
        AtomicReference<WorkerState> workerState,
        ConnectStatsManager connectStatsManager,
        ConnectStatsService connectStatsService,
        TransformChain<ConnectRecord> transformChain) {
        this.connectorName = connectorName;
        this.sinkTask = sinkTask;
        this.taskConfig = taskConfig;
        this.consumer = consumer;
        this.recordConverter = recordConverter;
        this.messageQueuesOffsetMap = new ConcurrentHashMap<>(256);
        this.messageQueuesStateMap = new ConcurrentHashMap<>(256);
        this.state = new AtomicReference<>(WorkerTaskState.NEW);
        this.workerState = workerState;
        this.connectStatsManager = connectStatsManager;
        this.connectStatsService = connectStatsService;
        this.stopPullMsgLatch = new CountDownLatch(1);
        this.transformChain = transformChain;
    }

    /**
     * Start a sink task, and receive data entry from MQ cyclically.
     */
    @Override
    public void run() {
        try {
            registTopics();
            consumer.start();
            log.info("Sink task consumer start. taskConfig {}", JSON.toJSONString(taskConfig));
            state.compareAndSet(WorkerTaskState.NEW, WorkerTaskState.PENDING);
            this.sinkTaskContext = new WorkerSinkTaskContext(taskConfig, this, consumer);
            sinkTask.init(sinkTaskContext);
            sinkTask.start(taskConfig);
            // we assume executed here means we are safe
            log.info("Sink task start, config:{}", JSON.toJSONString(taskConfig));
            state.compareAndSet(WorkerTaskState.PENDING, WorkerTaskState.RUNNING);

            while (WorkerState.STARTED == workerState.get() && WorkerTaskState.RUNNING == state.get()) {
                // this method can block up to 3 minutes long
                try {
                    preCommit(false);
                    setQueueOffset();
                    pullMessageFromQueues();
                } catch (RetriableException e) {
                    connectStatsManager.incSinkRecordPutTotalFailNums();
                    connectStatsManager.incSinkRecordPutFailNums(taskConfig.getString(RuntimeConfigDefine.TASK_ID));
                    log.error("Sink task RetriableException exception", e);
                } catch (InterruptedException e) {
                    connectStatsManager.incSinkRecordPutTotalFailNums();
                    connectStatsManager.incSinkRecordPutFailNums(taskConfig.getString(RuntimeConfigDefine.TASK_ID));
                    log.error("Sink task InterruptedException exception", e);
                    throw e;
                } catch (Throwable e) {
                    state.set(WorkerTaskState.ERROR);
                    log.error(" sink task {},pull message MQClientException, Error {} ", this, e.getMessage(), e);
                    connectStatsManager.incSinkRecordPutTotalFailNums();
                    connectStatsManager.incSinkRecordPutFailNums(taskConfig.getString(RuntimeConfigDefine.TASK_ID));
                } finally {
                    // record sink read times
                    connectStatsManager.incSinkRecordReadTotalTimes();
                }
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
                log.info("Sink task consumer shutdown. config:{}", JSON.toJSONString(taskConfig));
            }
        }
    }

    private void setQueueOffset() {
        Map<MessageQueue, Long> messageQueueOffsetMap = this.sinkTaskContext.queuesOffsets();
        if (org.apache.commons.collections4.MapUtils.isEmpty(messageQueueOffsetMap)) {
            return;
        }
        for (Map.Entry<MessageQueue, Long> entry : messageQueueOffsetMap.entrySet()) {
            if (messageQueuesOffsetMap.containsKey(entry.getKey())) {
                this.messageQueuesOffsetMap.put(entry.getKey(), entry.getValue());
                try {
                    consumer.updateConsumeOffset(entry.getKey(), entry.getValue());
                } catch (MQClientException e) {
                    log.warn("updateConsumeOffset MQClientException, messageQueue {}, offset {}", JSON.toJSONString(entry.getKey()), entry.getValue(), e);
                }
            }
        }
        this.sinkTaskContext.cleanQueuesOffsets();
    }

    private void registTopics() {
        Set<String> topics = SinkConnectorConfig.parseTopicList(taskConfig);
        if (org.apache.commons.collections4.CollectionUtils.isEmpty(topics)) {
            throw new ConnectException("sink connector topics config can be null, please check sink connector config info");
        }
        for (String topic : topics) {
            consumer.registerMessageQueueListener(topic, new MessageQueueListener() {
                @Override
                public void messageQueueChanged(String topic, Set<MessageQueue> mqAll, Set<MessageQueue> mqDivided) {
                    log.info("messageQueueChanged, old messageQueuesOffsetMap {}", JSON.toJSONString(messageQueuesOffsetMap));
                    WorkerSinkTask.this.preCommit(true);
                    messageQueuesOffsetMap.forEach((key, value) -> {
                        if (key.getTopic().equals(topic)) {
                            messageQueuesOffsetMap.remove(key, value);
                        }
                    });

                    Set<RecordPartition> waitRemoveQueueMetaDatas = new HashSet<>();
                    recordPartitions.forEach(key -> {
                        if (key.getPartition().get("topic").equals(topic)) {
                            waitRemoveQueueMetaDatas.add(key);
                        }
                    });
                    recordPartitions.removeAll(waitRemoveQueueMetaDatas);
                    for (MessageQueue messageQueue : mqDivided) {
                        messageQueuesOffsetMap.put(messageQueue, consumeFromOffset(messageQueue, taskConfig));
                        RecordPartition recordPartition = ConnectUtil.convertToRecordPartition(messageQueue);
                        recordPartitions.add(recordPartition);
                    }
                    log.info("messageQueueChanged, new messageQueuesOffsetMap {}", JSON.toJSONString(messageQueuesOffsetMap));
                }
            });
        }
    }

    public long consumeFromOffset(MessageQueue messageQueue, ConnectKeyValue taskConfig) {
        //-1 when started
        long offset = consumer.getOffsetStore().readOffset(messageQueue, ReadOffsetType.READ_FROM_MEMORY);
        if (offset < 0) {
            //query from broker
            offset = consumer.getOffsetStore().readOffset(messageQueue, ReadOffsetType.READ_FROM_STORE);
        }

        String consumeFromWhere = taskConfig.getString("consume-from-where");
        if (StringUtils.isBlank(consumeFromWhere)) {
            consumeFromWhere = "CONSUME_FROM_LAST_OFFSET";
        }

        if (offset < 0) {
            for (int i = 0; i < 3; i++) {
                try {
                    if (consumeFromWhere.equals("CONSUME_FROM_FIRST_OFFSET")) {
                        offset = consumer.minOffset(messageQueue);
                    } else {
                        offset = consumer.maxOffset(messageQueue);
                    }
                    break;
                } catch (MQClientException e) {
                    log.error("get max offset MQClientException", e);
                    if (i == 3) {
                        throw new ConnectException("get max offset MQClientException", e);
                    }
                    continue;
                }
            }
        }
        //make sure
        if (offset < 0) {
            offset = 0;
        }
        return offset;
    }

    public void incPullTPS(String topic, int pullSize) {
        consumer.getDefaultMQPullConsumerImpl().getRebalanceImpl().getmQClientFactory()
            .getConsumerStatsManager().incPullTPS(consumer.getConsumerGroup(), topic, pullSize);
    }

    private void pullMessageFromQueues() throws InterruptedException {
        long startTimeStamp = System.currentTimeMillis();
        log.info("START pullMessageFromQueues, time started : {}", startTimeStamp);
        if (org.apache.commons.collections4.MapUtils.isEmpty(messageQueuesOffsetMap)) {
            log.info("messageQueuesOffsetMap is null, : {}", startTimeStamp);
            stopPullMsgLatch.await(PULL_MSG_ERROR_BACKOFF_MS, TimeUnit.MILLISECONDS);
        }
        for (Map.Entry<MessageQueue, Long> entry : messageQueuesOffsetMap.entrySet()) {
            if (messageQueuesStateMap.containsKey(entry.getKey())) {
                log.warn("sink task message queue state is not running, sink task id {}, queue info {}, queue state {}", taskConfig.getString(RuntimeConfigDefine.TASK_ID), JSON.toJSONString(entry.getKey()), JSON.toJSONString(messageQueuesStateMap.get(entry.getKey())));
                continue;
            }
            log.info("START pullBlockIfNotFound, time started : {}", System.currentTimeMillis());

            if (WorkerTaskState.RUNNING != state.get()) {
                log.warn("sink task state is not running, sink task id {}, state {}", taskConfig.getString(RuntimeConfigDefine.TASK_ID), state.get().name());
                break;
            }
            PullResult pullResult = null;
            final long beginPullMsgTimestamp = System.currentTimeMillis();
            try {
                shouldStopPullMsg();
                pullResult = consumer.pullBlockIfNotFound(entry.getKey(), "*", entry.getValue(), MAX_MESSAGE_NUM);
                pullMsgErrorCount = 0;
            } catch (MQClientException e) {
                pullMsgErrorCount++;
                log.error(" sink task message queue {}, offset {}, taskconfig {},pull message MQClientException, Error {}, taskState {}", JSON.toJSONString(entry.getKey()), JSON.toJSONString(entry.getValue()), JSON.toJSONString(taskConfig), e.getMessage(), this.state.get(), e);
                connectStatsManager.incSinkRecordReadTotalFailNums();
                connectStatsManager.incSinkRecordReadFailNums(taskConfig.getString(RuntimeConfigDefine.TASK_ID));
                long errorPullRT = System.currentTimeMillis() - beginPullMsgTimestamp;
                connectStatsManager.incSinkRecordReadTotalFailRT(errorPullRT);
                connectStatsManager.incSinkRecordReadFailRT(taskConfig.getString(RuntimeConfigDefine.TASK_ID), errorPullRT);
            } catch (RemotingException e) {
                pullMsgErrorCount++;
                log.error(" sink task message queue {}, offset {}, taskconfig {},pull message RemotingException, Error {}, taskState {}", JSON.toJSONString(entry.getKey()), JSON.toJSONString(entry.getValue()), JSON.toJSONString(taskConfig), e.getMessage(), this.state.get(), e);
                connectStatsManager.incSinkRecordReadTotalFailNums();
                connectStatsManager.incSinkRecordReadFailNums(taskConfig.getString(RuntimeConfigDefine.TASK_ID));
                long errorPullRT = System.currentTimeMillis() - beginPullMsgTimestamp;
                connectStatsManager.incSinkRecordReadTotalFailRT(errorPullRT);
                connectStatsManager.incSinkRecordReadFailRT(taskConfig.getString(RuntimeConfigDefine.TASK_ID), errorPullRT);
            } catch (MQBrokerException e) {
                pullMsgErrorCount++;
                log.error(" sink task message queue {}, offset {}, taskconfig {},pull message MQBrokerException, Error {}, taskState {}", JSON.toJSONString(entry.getKey()), JSON.toJSONString(entry.getValue()), JSON.toJSONString(taskConfig), e.getMessage(), this.state.get(), e);
                connectStatsManager.incSinkRecordReadTotalFailNums();
                connectStatsManager.incSinkRecordReadFailNums(taskConfig.getString(RuntimeConfigDefine.TASK_ID));
                long errorPullRT = System.currentTimeMillis() - beginPullMsgTimestamp;
                connectStatsManager.incSinkRecordReadTotalFailRT(errorPullRT);
                connectStatsManager.incSinkRecordReadFailRT(taskConfig.getString(RuntimeConfigDefine.TASK_ID), errorPullRT);
            } catch (InterruptedException e) {
                pullMsgErrorCount++;
                log.error(" sink task message queue {}, offset {}, taskconfig {},pull message InterruptedException, Error {}, taskState {}", JSON.toJSONString(entry.getKey()), JSON.toJSONString(entry.getValue()), JSON.toJSONString(taskConfig), e.getMessage(), this.state.get(), e);
                connectStatsManager.incSinkRecordReadTotalFailNums();
                connectStatsManager.incSinkRecordReadFailNums(taskConfig.getString(RuntimeConfigDefine.TASK_ID));
                long errorPullRT = System.currentTimeMillis() - beginPullMsgTimestamp;
                connectStatsManager.incSinkRecordReadTotalFailRT(errorPullRT);
                connectStatsManager.incSinkRecordReadFailRT(taskConfig.getString(RuntimeConfigDefine.TASK_ID), errorPullRT);
                throw e;
            } catch (Throwable e) {
                pullMsgErrorCount++;
                log.error(" sink task message queue {}, offset {}, taskconfig {},pull message Throwable, Error {}, taskState {}", JSON.toJSONString(entry.getKey()), JSON.toJSONString(entry.getValue()), JSON.toJSONString(taskConfig), e.getMessage(), e);
                connectStatsManager.incSinkRecordReadTotalFailNums();
                connectStatsManager.incSinkRecordReadFailNums(taskConfig.getString(RuntimeConfigDefine.TASK_ID));
                long errorPullRT = System.currentTimeMillis() - beginPullMsgTimestamp;
                connectStatsManager.incSinkRecordReadTotalFailRT(errorPullRT);
                connectStatsManager.incSinkRecordReadFailRT(taskConfig.getString(RuntimeConfigDefine.TASK_ID), errorPullRT);
                throw e;
            }
            long currentTime = System.currentTimeMillis();

            List<MessageExt> messages = null;
            log.info("INSIDE pullMessageFromQueues, time elapsed : {}", currentTime - startTimeStamp);
            if (null != pullResult && pullResult.getPullStatus().equals(PullStatus.FOUND)) {
                this.incPullTPS(entry.getKey().getTopic(), pullResult.getMsgFoundList().size());
                messages = pullResult.getMsgFoundList();
                connectStatsManager.incSinkRecordReadTotalNums(messages.size());
                connectStatsManager.incSinkRecordReadNums(taskConfig.getString(RuntimeConfigDefine.TASK_ID), messages.size());
                long pullRT = System.currentTimeMillis() - beginPullMsgTimestamp;
                connectStatsManager.incSinkRecordReadTotalRT(pullRT);
                connectStatsManager.incSinkRecordReadRT(taskConfig.getString(RuntimeConfigDefine.TASK_ID), pullRT);
                receiveMessages(messages);
                if (messageQueuesOffsetMap.containsKey(entry.getKey())) {
                    messageQueuesOffsetMap.put(entry.getKey(), pullResult.getNextBeginOffset());
                } else {
                    log.warn("The consumer may have load balancing, and the current task does not process the message queue,messageQueuesOffsetMap {}, messageQueue {}", JSON.toJSONString(messageQueuesOffsetMap), JSON.toJSONString(entry.getKey()));
                }
                try {
                    consumer.updateConsumeOffset(entry.getKey(), pullResult.getNextBeginOffset());
                } catch (MQClientException e) {
                    log.warn("updateConsumeOffset MQClientException, pullResult {}", pullResult, e);
                }
            } else if (null != pullResult && pullResult.getPullStatus().equals(PullStatus.OFFSET_ILLEGAL)) {
                log.warn("offset illegal, reset offset, message queue {}, pull offset {}, nextBeginOffset {}", JSON.toJSONString(entry.getKey()), entry.getValue(), pullResult.getNextBeginOffset());
                this.sinkTaskContext.resetOffset(ConnectUtil.convertToRecordPartition(entry.getKey()), ConnectUtil.convertToRecordOffset(pullResult.getNextBeginOffset()));
            } else if (null != pullResult && pullResult.getPullStatus().equals(PullStatus.NO_NEW_MSG)) {
                log.info("no new message, pullResult {}, message queue {}, pull offset {}", JSON.toJSONString(pullResult), JSON.toJSONString(entry.getKey()), entry.getValue());
            } else if (null != pullResult && pullResult.getPullStatus().equals(PullStatus.NO_MATCHED_MSG)) {
                log.info("no matched msg, pullResult {}, message queue {}, pull offset {}", JSON.toJSONString(pullResult), JSON.toJSONString(entry.getKey()), entry.getValue());
                this.sinkTaskContext.resetOffset(ConnectUtil.convertToRecordPartition(entry.getKey()), ConnectUtil.convertToRecordOffset(pullResult.getNextBeginOffset()));
            } else {
                log.info("no new message, pullResult {}, message queue {}, pull offset {}", JSON.toJSONString(pullResult), JSON.toJSONString(entry.getKey()), entry.getValue());
            }

            AtomicLong atomicLong = connectStatsService.singleSinkTaskTimesTotal(taskConfig.getString(RuntimeConfigDefine.TASK_ID));
            if (null != atomicLong) {
                atomicLong.addAndGet(org.apache.commons.collections4.CollectionUtils.isEmpty(messages) ? 0 : messages.size());
            }
        }
    }

    private void shouldStopPullMsg() throws InterruptedException {
        if (pullMsgErrorCount == PULL_MSG_ERROR_THRESHOLD) {
            log.error("Accumulative error {} times, stop pull msg for {} ms", pullMsgErrorCount, PULL_MSG_ERROR_BACKOFF_MS);
            stopPullMsgLatch.await(PULL_MSG_ERROR_BACKOFF_MS, TimeUnit.MILLISECONDS);
            pullMsgErrorCount = 0;
        }
    }

    private void preCommit(boolean isForce) {
        long commitInterval = taskConfig.getLong(OFFSET_COMMIT_TIMEOUT_MS_CONFIG, 1000);
        if (nextCommitTime <= 0) {
            long now = System.currentTimeMillis();
            nextCommitTime = now + commitInterval;
        }
        if (isForce || nextCommitTime < System.currentTimeMillis()) {
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
        try {
            transformChain.close();
        } catch (Exception exception) {
            log.error("Transform close failed, {}", exception);
        }
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

        } else {
            final byte[] messageBody = message.getBody();
            String s = new String(messageBody);
            sinkDataEntry = JSON.parseObject(s, ConnectRecord.class);
        }

        KeyValue keyValue = new DefaultKeyValue();
        if (MapUtils.isNotEmpty(properties)) {
            for (Map.Entry<String, String> entry : properties.entrySet()) {
                if (MQ_SYS_KEYS.contains(entry.getKey())) {
                    keyValue.put("MQ-SYS-" + entry.getKey(), entry.getValue());
                } else if (entry.getKey().startsWith("connect-ext-")) {
                    keyValue.put(entry.getKey().replaceAll("connect-ext-", ""), entry.getValue());
                } else {
                    keyValue.put(entry.getKey(), entry.getValue());
                }
            }
        }
        sinkDataEntry.addExtension(keyValue);

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

    public Set<RecordPartition> getRecordPartitions() {
        return recordPartitions;
    }

    /**
     * Reset the consumer offset for the given queue.
     *
     * @param recordPartition the queue to reset offset.
     * @param recordOffset    the offset to reset to.
     */
    public void resetOffset(RecordPartition recordPartition, RecordOffset recordOffset) {
        this.sinkTaskContext.resetOffset(recordPartition, recordOffset);
    }

    /**
     * Reset the consumer offsets for the given queue.
     *
     * @param offsets the map of offsets for queuename.
     */
    public void resetOffset(Map<RecordPartition, RecordOffset> offsets) {
        this.sinkTaskContext.resetOffset(offsets);
    }

}


