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
import io.openmessaging.connector.api.data.*;
import io.openmessaging.connector.api.errors.ConnectException;
import io.openmessaging.connector.api.errors.RetriableException;
import io.openmessaging.internal.DefaultKeyValue;
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
import org.apache.rocketmq.connect.runtime.config.ConnectConfig;
import org.apache.rocketmq.connect.runtime.config.RuntimeConfigDefine;
import org.apache.rocketmq.connect.runtime.config.SinkConnectorConfig;
import org.apache.rocketmq.connect.runtime.errors.ErrorReporter;
import org.apache.rocketmq.connect.runtime.errors.RetryWithToleranceOperator;
import org.apache.rocketmq.connect.runtime.errors.WorkerErrorRecordReporter;
import org.apache.rocketmq.connect.runtime.stats.ConnectStatsManager;
import org.apache.rocketmq.connect.runtime.stats.ConnectStatsService;
import org.apache.rocketmq.connect.runtime.utils.Base64Util;
import org.apache.rocketmq.connect.runtime.utils.ConnectUtil;
import org.apache.rocketmq.connect.runtime.utils.ConnectorTaskId;
import org.apache.rocketmq.connect.runtime.utils.Utils;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;


/**
 * A wrapper of {@link SinkTask} for runtime.
 */
public class WorkerSinkTask extends WorkerTask {

    private static final Logger log = LoggerFactory.getLogger(LoggerName.ROCKETMQ_RUNTIME);
    /**
     * The implements of the sink task.
     */
    private SinkTask sinkTask;

    /**
     * A RocketMQ consumer to pull message from MQ.
     */
    private final DefaultMQPullConsumer consumer;

    /**
     * A converter to parse sink data entry to object.
     */
    private RecordConverter keyConverter;
    private RecordConverter valueConverter;

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

    private long pullNotFountMsgCount = 0;

    private static final long PULL_MSG_ERROR_BACKOFF_MS = 1000 * 10;
    private static final long PULL_NO_MSG_BACKOFF_MS = 1000 * 3;
    private static final long PULL_MSG_ERROR_THRESHOLD = 16;

    /**
     * stat
     */
    private final ConnectStatsManager connectStatsManager;
    private final ConnectStatsService connectStatsService;

    private final CountDownLatch stopPullMsgLatch;
    private WorkerSinkTaskContext sinkTaskContext;
    private WorkerErrorRecordReporter errorRecordReporter;


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

    public WorkerSinkTask(ConnectConfig workerConfig,
                          ConnectorTaskId id,
                          SinkTask sinkTask,
                          ClassLoader classLoader,
                          ConnectKeyValue taskConfig,
                          RecordConverter keyConverter,
                          RecordConverter valueConverter,
                          DefaultMQPullConsumer consumer,
                          AtomicReference<WorkerState> workerState,
                          ConnectStatsManager connectStatsManager,
                          ConnectStatsService connectStatsService,
                          TransformChain<ConnectRecord> transformChain,
                          RetryWithToleranceOperator retryWithToleranceOperator,
                          WorkerErrorRecordReporter errorRecordReporter) {
        super(workerConfig, id, classLoader, taskConfig, retryWithToleranceOperator, transformChain, workerState);
        this.sinkTask = sinkTask;
        this.consumer = consumer;
        this.keyConverter = keyConverter;
        this.valueConverter = valueConverter;
        this.messageQueuesOffsetMap = new ConcurrentHashMap<>(256);
        this.messageQueuesStateMap = new ConcurrentHashMap<>(256);
        this.connectStatsManager = connectStatsManager;
        this.connectStatsService = connectStatsService;
        this.stopPullMsgLatch = new CountDownLatch(1);
        this.sinkTaskContext = new WorkerSinkTaskContext(taskConfig, this, consumer);
        this.errorRecordReporter = errorRecordReporter;

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

    /**
     * sub topics
     */
    private void registryTopics() {
        Set<String> topics = SinkConnectorConfig.parseTopicList(taskConfig);
        if (org.apache.commons.collections4.CollectionUtils.isEmpty(topics)) {
            throw new ConnectException("Sink connector topics config can be null, please check sink connector config info");
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
        consumer.getDefaultMQPullConsumerImpl()
                .getRebalanceImpl()
                .getmQClientFactory()
                .getConsumerStatsManager()
                .incPullTPS(consumer.getConsumerGroup(), topic, pullSize);
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
                log.warn("sink task message queue state is not running, sink task id {}, queue info {}, queue state {}", id().toString(), JSON.toJSONString(entry.getKey()), JSON.toJSONString(messageQueuesStateMap.get(entry.getKey())));
                continue;
            }
            log.info("START pullBlockIfNotFound, time started : {}", System.currentTimeMillis());
            if (isStopping()) {
                log.warn("sink task state is not running, sink task id {}, state {}", id().toString(), state.get().name());
                break;
            }
            PullResult pullResult = null;
            final long beginPullMsgTimestamp = System.currentTimeMillis();
            try {
                shouldStopPullMsg();
                pullResult = consumer.pull(entry.getKey(), "*", entry.getValue(), MAX_MESSAGE_NUM);
                if (pullResult == null) {
                    continue;
                }
                pullMsgErrorCount = 0;
            } catch (MQClientException | RemotingException | MQBrokerException e) {
                pullMsgErrorCount++;
                log.error(" sink task message queue {}, offset {}, taskconfig {},pull message {}, Error {}, taskState {}", JSON.toJSONString(entry.getKey()), JSON.toJSONString(entry.getValue()), JSON.toJSONString(taskConfig), e.getClass().getName(), e.getMessage(), this.state.get(), e);
                readRecordFail(beginPullMsgTimestamp);
            } catch (InterruptedException e) {
                pullMsgErrorCount++;
                log.error(" sink task message queue {}, offset {}, taskconfig {},pull message InterruptedException, Error {}, taskState {}", JSON.toJSONString(entry.getKey()), JSON.toJSONString(entry.getValue()), JSON.toJSONString(taskConfig), e.getMessage(), this.state.get(), e);
                readRecordFail(beginPullMsgTimestamp);
                throw e;
            } catch (Throwable e) {
                pullMsgErrorCount++;
                log.error(" sink task message queue {}, offset {}, taskconfig {},pull message Throwable, Error {}, taskState {}", JSON.toJSONString(entry.getKey()), JSON.toJSONString(entry.getValue()), JSON.toJSONString(taskConfig), e.getMessage(), e);
                readRecordFail(beginPullMsgTimestamp);
                throw e;
            }

            List<MessageExt> messages = null;
            log.info("INSIDE pullMessageFromQueues, time elapsed : {}", System.currentTimeMillis() - startTimeStamp);
            PullStatus status = pullResult.getPullStatus();
            switch (status) {
                case FOUND:
                    pullNotFountMsgCount = 0;
                    this.incPullTPS(entry.getKey().getTopic(), pullResult.getMsgFoundList().size());
                    messages = pullResult.getMsgFoundList();
                    recordReadSuccess(messages.size(), beginPullMsgTimestamp);
                    receiveMessages(messages);
                    if (messageQueuesOffsetMap.containsKey(entry.getKey())) {
                        // put offset
                        messageQueuesOffsetMap.put(entry.getKey(), pullResult.getNextBeginOffset());
                    } else {
                        // load balancing
                        log.warn("The consumer may have load balancing, and the current task does not process the message queue,messageQueuesOffsetMap {}, messageQueue {}", JSON.toJSONString(messageQueuesOffsetMap), JSON.toJSONString(entry.getKey()));
                    }
                    try {
                        consumer.updateConsumeOffset(entry.getKey(), pullResult.getNextBeginOffset());
                    } catch (MQClientException e) {
                        log.warn("updateConsumeOffset MQClientException, pullResult {}", pullResult, e);
                    }
                    break;
                case OFFSET_ILLEGAL:
                    log.warn("Offset illegal, reset offset, message queue {}, pull offset {}, nextBeginOffset {}", JSON.toJSONString(entry.getKey()), entry.getValue(), pullResult.getNextBeginOffset());
                    this.sinkTaskContext.resetOffset(ConnectUtil.convertToRecordPartition(entry.getKey()), ConnectUtil.convertToRecordOffset(pullResult.getNextBeginOffset()));
                    break;
                case NO_NEW_MSG:
                    pullNotFountMsgCount++;
                    log.info("No new message, pullResult {}, message queue {}, pull offset {}", JSON.toJSONString(pullResult), JSON.toJSONString(entry.getKey()), entry.getValue());
                    break;
                case NO_MATCHED_MSG:
                    log.info("no matched msg, pullResult {}, message queue {}, pull offset {}", JSON.toJSONString(pullResult), JSON.toJSONString(entry.getKey()), entry.getValue());
                    this.sinkTaskContext.resetOffset(ConnectUtil.convertToRecordPartition(entry.getKey()), ConnectUtil.convertToRecordOffset(pullResult.getNextBeginOffset()));
                    break;
                default:
                    pullNotFountMsgCount++;
                    log.info("unknow pull msg state, pullResult {}, message queue {}, pull offset {}", JSON.toJSONString(pullResult), JSON.toJSONString(entry.getKey()), entry.getValue());
                    break;
            }

            AtomicLong atomicLong = connectStatsService.singleSinkTaskTimesTotal(id().toString());
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
        if (pullNotFountMsgCount >= PULL_MSG_ERROR_THRESHOLD) {
            log.error("pull not found msg {} times, stop pull msg for {} ms", pullNotFountMsgCount, PULL_NO_MSG_BACKOFF_MS);
            stopPullMsgLatch.await(PULL_NO_MSG_BACKOFF_MS, TimeUnit.MILLISECONDS);
            pullNotFountMsgCount = 0;
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

    @Override
    public void close() {
        sinkTask.stop();
        consumer.shutdown();
        stopPullMsgLatch.countDown();
        Utils.closeQuietly(transformChain, "transform chain");
        Utils.closeQuietly(retryWithToleranceOperator, "retry operator");
    }


    /**
     * receive message from MQ.
     *
     * @param messages
     */
    private void receiveMessages(List<MessageExt> messages) {
        List<ConnectRecord> records = new ArrayList<>(32);
        for (MessageExt message : messages) {
            this.retryWithToleranceOperator.consumerRecord(message);
            ConnectRecord connectRecord = convertMessages(message);
            if (connectRecord != null && !this.retryWithToleranceOperator.failed()) {
                records.add(connectRecord);
            }
            log.info("Received one message success : msgId {}", message.getMsgId());
        }
        try {
            sinkTask.put(records);
            return;
        } catch (RetriableException e) {
            log.error("task {} put sink recode RetriableException", this, e.getMessage(), e);
            throw e;
        } catch (Throwable t) {
            log.error("task {} put sink recode Throwable", this, t.getMessage(), t);
            throw t;
        }

    }

    private ConnectRecord convertMessages(MessageExt message) {
        Map<String, String> properties = message.getProperties();
        // timestamp
        String connectTimestamp = properties.get(RuntimeConfigDefine.CONNECT_TIMESTAMP);
        Long timestamp = StringUtils.isNotEmpty(connectTimestamp) ? Long.valueOf(connectTimestamp) : null;

        // partition and offset
        RecordPartition recordPartition = ConnectUtil.convertToRecordPartition(message.getTopic(), message.getBrokerName(), message.getQueueId());
        RecordOffset recordOffset = ConnectUtil.convertToRecordOffset(message.getQueueOffset());


        SchemaAndValue schemaAndKey = retryWithToleranceOperator.execute(() -> keyConverter.toConnectData(message.getTopic(), Base64Util.base64Decode(message.getKeys())),
                ErrorReporter.Stage.CONVERTER, keyConverter.getClass());

        // convert value
        SchemaAndValue schemaAndValue = retryWithToleranceOperator.execute(() -> valueConverter.toConnectData(message.getTopic(), message.getBody()),
                ErrorReporter.Stage.CONVERTER, valueConverter.getClass());


        Schema schema = SchemaBuilder.struct()
                .name(message.getTopic())
                .field("id",SchemaBuilder.int32().build())
                .field("name", SchemaBuilder.string().build())
                .build();
        schemaAndKey.schema().setKeySchema(schema);
        schemaAndValue.schema().setValueSchema(schema);
        schemaAndKey.schema().setName(message.getTopic());
        schemaAndValue.schema().setName(message.getTopic());
        schemaAndKey.schema().setFieldType(FieldType.STRUCT);
        schemaAndValue.schema().setFieldType(FieldType.STRUCT);
        int id = 1;
        String name = "rocketmq";
        io.openmessaging.connector.api.data.Struct struct= new Struct(schema);
        struct.put("id",id);
        struct.put("name",name);
        ConnectRecord record = new ConnectRecord(recordPartition, recordOffset, timestamp, schemaAndKey.schema(), schemaAndKey.value(), schemaAndValue.schema(), schemaAndValue.value());
        record.setSchema(schema);
        record.setKey(schema);
        record.setData(struct);

        if (retryWithToleranceOperator.failed()) {
            return null;
        }


        // Apply the transformations
        ConnectRecord transformedRecord = transformChain.doTransforms(record);
        if (transformedRecord == null) {
            return null;
        }

        // add extension
        addExtension(properties, record);
        return record;
    }

    private void addExtension(Map<String, String> properties, ConnectRecord sinkDataEntry) {
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
    }

    /**
     * initinalize and start
     */
    @Override
    protected void initializeAndStart() {
        registryTopics();
        try {
            consumer.start();
        } catch (MQClientException e) {
        }
        log.info("Sink task consumer start. taskConfig {}", JSON.toJSONString(taskConfig));
        sinkTask.init(sinkTaskContext);
        sinkTask.start(taskConfig);
    }

    /**
     * execute poll and send record
     */
    @Override
    protected void execute() {
        while (isRunning()) {
            // this method can block up to 3 minutes long
            try {
                preCommit(false);
                setQueueOffset();
                pullMessageFromQueues();
            } catch (RetriableException e) {
                readRecordFailNum();
                log.error("Sink task RetriableException exception", e);
            } catch (InterruptedException e) {
                readRecordFailNum();
                log.error("Sink task InterruptedException exception", e);
            } catch (Throwable e) {
                log.error(" sink task {},pull message MQClientException, Error {} ", this, e.getMessage(), e);
                readRecordFailNum();
                throw e;
            } finally {
                // record sink read times
                connectStatsManager.incSinkRecordReadTotalTimes();
            }
        }

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

    /**
     * error record reporter
     *
     * @return
     */
    public WorkerErrorRecordReporter errorRecordReporter() {
        return errorRecordReporter;
    }

    private void recordReadSuccess(int recordSize, long beginPullMsgTimestamp) {
        long pullRT = System.currentTimeMillis() - beginPullMsgTimestamp;
        recordReadNums(recordSize);
        recordReadRT(pullRT);
    }

    private void recordReadNums(int size) {
        connectStatsManager.incSinkRecordReadTotalNums(size);
        connectStatsManager.incSinkRecordReadNums(id().toString(), size);
    }

    private void recordReadRT(long pullRT) {
        connectStatsManager.incSinkRecordReadTotalRT(pullRT);
        connectStatsManager.incSinkRecordReadRT(id().toString(), pullRT);
    }


    private void readRecordFail(long beginPullMsgTimestamp) {
        readRecordFailNum();
        readRecordFailRT(System.currentTimeMillis() - beginPullMsgTimestamp);
    }

    private void readRecordFailRT(long errorPullRT) {
        connectStatsManager.incSinkRecordReadTotalFailRT(errorPullRT);
        connectStatsManager.incSinkRecordReadFailRT(id().toString(), errorPullRT);
    }

    private void readRecordFailNum() {
        connectStatsManager.incSinkRecordReadTotalFailNums();
        connectStatsManager.incSinkRecordReadFailNums(id().toString());
    }

}


