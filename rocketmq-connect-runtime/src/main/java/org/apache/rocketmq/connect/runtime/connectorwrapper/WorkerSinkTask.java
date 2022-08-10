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
import io.openmessaging.connector.api.data.RecordConverter;
import io.openmessaging.connector.api.data.RecordOffset;
import io.openmessaging.connector.api.data.RecordPartition;
import io.openmessaging.connector.api.data.SchemaAndValue;
import io.openmessaging.connector.api.errors.ConnectException;
import io.openmessaging.connector.api.errors.RetriableException;
import io.openmessaging.internal.DefaultKeyValue;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.client.consumer.DefaultLitePullConsumer;
import org.apache.rocketmq.client.consumer.MessageQueueListener;
import org.apache.rocketmq.client.consumer.store.ReadOffsetType;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.connect.runtime.common.ConnectKeyValue;
import org.apache.rocketmq.connect.runtime.common.LoggerName;
import org.apache.rocketmq.connect.runtime.config.ConnectConfig;
import org.apache.rocketmq.connect.runtime.config.RuntimeConfigDefine;
import org.apache.rocketmq.connect.runtime.config.SinkConnectorConfig;
import org.apache.rocketmq.connect.runtime.connectorwrapper.status.WrapperStatusListener;
import org.apache.rocketmq.connect.runtime.errors.ErrorReporter;
import org.apache.rocketmq.connect.runtime.errors.RetryWithToleranceOperator;
import org.apache.rocketmq.connect.runtime.errors.WorkerErrorRecordReporter;
import org.apache.rocketmq.connect.runtime.stats.ConnectStatsManager;
import org.apache.rocketmq.connect.runtime.stats.ConnectStatsService;
import org.apache.rocketmq.connect.runtime.utils.Base64Util;
import org.apache.rocketmq.connect.runtime.utils.ConnectUtil;
import org.apache.rocketmq.connect.runtime.utils.ConnectorTaskId;
import org.apache.rocketmq.connect.runtime.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static java.util.Collections.singleton;


/**
 * A wrapper of {@link SinkTask} for runtime.
 */
public class WorkerSinkTask extends WorkerTask {

    private static final Logger log = LoggerFactory.getLogger(LoggerName.ROCKETMQ_RUNTIME);

    private static final Integer MAX_MESSAGE_NUM = 32;
    private static final long PULL_MSG_ERROR_BACKOFF_MS = 1000 * 5;
    /**
     * The implements of the sink task.
     */
    private SinkTask sinkTask;

    /**
     * A RocketMQ consumer to pull message from MQ.
     */
    private final DefaultLitePullConsumer consumer;

    /**
     * A converter to parse sink data entry to object.
     */
    private RecordConverter keyConverter;
    private RecordConverter valueConverter;

    /**
     * cache offset
     */
    private final Map<MessageQueue, Long> lastCommittedOffsets;
    private final Map<MessageQueue, Long> currentOffsets;
    private final Map<MessageQueue, Long> originalOffsets;
    private final Set<MessageQueue> messageQueues;

    private final List<ConnectRecord> messageBatch;


    private Set<RecordPartition> recordPartitions = new CopyOnWriteArraySet<>();

    private MessageQueueListener messageQueueListener = null;
    /**
     * stat
     */
    private final ConnectStatsManager connectStatsManager;
    private final ConnectStatsService connectStatsService;

    private final CountDownLatch stopPullMsgLatch;
    private WorkerSinkTaskContext sinkTaskContext;
    private WorkerErrorRecordReporter errorRecordReporter;

    /**
     * for commit
     */
    private long nextCommit;
    private int commitSeqno;
    private long commitStarted;
    private boolean committing;
    private boolean pausedForRetry;



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
                          DefaultLitePullConsumer consumer,
                          AtomicReference<WorkerState> workerState,
                          ConnectStatsManager connectStatsManager,
                          ConnectStatsService connectStatsService,
                          TransformChain<ConnectRecord> transformChain,
                          RetryWithToleranceOperator retryWithToleranceOperator,
                          WorkerErrorRecordReporter errorRecordReporter,
                          WrapperStatusListener statusListener) {
        super(workerConfig, id, classLoader, taskConfig, retryWithToleranceOperator, transformChain, workerState, statusListener);
        this.sinkTask = sinkTask;
        this.consumer = consumer;
        this.keyConverter = keyConverter;
        this.valueConverter = valueConverter;
        this.messageQueues = new HashSet<>();
        this.connectStatsManager = connectStatsManager;
        this.connectStatsService = connectStatsService;
        this.stopPullMsgLatch = new CountDownLatch(1);
        this.sinkTaskContext = new WorkerSinkTaskContext(taskConfig, this, consumer);
        this.errorRecordReporter = errorRecordReporter;
        // cache commit offset
        this.lastCommittedOffsets = new ConcurrentHashMap<>();
        this.currentOffsets = new ConcurrentHashMap<>();
        this.originalOffsets = new ConcurrentHashMap<>();
        this.messageBatch =  new ArrayList<>();

        // commit
        this.nextCommit = System.currentTimeMillis() + workerConfig.getOffsetCommitIntervalMs();
        this.committing = false;
        this.commitSeqno = 0;
        this.commitStarted = -1;
        // pause for retry
        this.pausedForRetry = false;
    }



    protected void iteration() {
        final long offsetCommitIntervalMs = workerConfig.getOffsetCommitIntervalMs();
        long now = System.currentTimeMillis();
        // check committing
        if (!committing && now >= nextCommit) {
            commitOffsets(now, false);
            nextCommit = now + offsetCommitIntervalMs;
        }

        final long commitTimeoutMs = commitStarted + workerConfig.getOffsetCommitTimeoutMsConfig();

        // Check for timed out commits
        if (committing && now >= commitTimeoutMs) {
            log.warn("{} Commit of offsets timed out", this);
            committing = false;
        }

        // And process messages
        long timeoutMs = Math.max(nextCommit - now, 0);
        poll(timeoutMs);
    }


    @Override
    public void transitionTo(TargetState state) {
        super.transitionTo(state);
    }


    // resume all consumer topic queue
    private void resumeAll() {
        for (MessageQueue queue : messageQueues) {
            if (!sinkTaskContext.getPausedQueues().contains(queue)) {
                consumer.resume(singleton(queue));
            }
        }
    }

    //pause all consumer topic queue
    private void pauseAll() {
        consumer.pause(messageQueues);
    }

    /**
     * commit offset
     * @param now
     * @param closing
     */
    private void commitOffsets(long now, boolean closing) {
        commitOffsets(now, closing, messageQueues);
    }

    private void commitOffsets(long now, boolean closing, Set<MessageQueue> messageQueues) {
        log.trace("Committing offsets for queues {}", messageQueues);

        Map<MessageQueue, Long> offsetsToCommit = currentOffsets.entrySet()
                .stream()
                .filter(e -> messageQueues.contains(e.getKey()))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        if (offsetsToCommit.isEmpty()) {
            return;
        }

        committing = true;
        commitSeqno += 1;
        commitStarted = now;

        Map<MessageQueue, Long> lastCommittedOffsetsForPartitions = this.lastCommittedOffsets.entrySet()
                .stream()
                .filter(e -> offsetsToCommit.containsKey(e.getKey()))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        final Map<MessageQueue, Long> taskResetOffsets  = new ConcurrentHashMap<>();
        Map<RecordPartition, RecordOffset>  taskProvidedRecordOffset = new ConcurrentHashMap<>();
        try {
            log.trace("{} Calling task.preCommit with current offsets: {}", this, offsetsToCommit);

            Map<RecordPartition, RecordOffset>  queueCommitOffset =  new ConcurrentHashMap<>();
            for (Map.Entry<MessageQueue, Long> messageQueueOffset : offsetsToCommit.entrySet()) {
                RecordPartition recordPartition = ConnectUtil.convertToRecordPartition(messageQueueOffset.getKey());
                RecordOffset recordOffset = ConnectUtil.convertToRecordOffset(messageQueueOffset.getValue());
                queueCommitOffset.put(recordPartition, recordOffset);
            }

            // pre commit
            taskProvidedRecordOffset = sinkTask.preCommit(queueCommitOffset);

            for (Map.Entry<RecordPartition, RecordOffset> entry : taskProvidedRecordOffset.entrySet()){
                taskResetOffsets.put(ConnectUtil.convertToMessageQueue(entry.getKey()),ConnectUtil.convertToOffset(entry.getValue()));
            }

        } catch (Throwable t) {
            if (closing) {
                log.warn("{} Offset commit failed", this);
            } else {
                log.error("{} Offset commit failed, reset to last committed offsets", this, t);
                for (Map.Entry<MessageQueue, Long> entry : lastCommittedOffsetsForPartitions.entrySet()) {
                    log.debug("{} Rewinding topic queue {} to offset {}", this, entry.getKey(), entry.getValue());
                    try {
                        consumer.seek(entry.getKey(), entry.getValue());
                    } catch (MQClientException e) {}
                }
                //
                currentOffsets.putAll(lastCommittedOffsetsForPartitions);
            }
            onCommitCompleted(t, commitSeqno, null);
            return;
        } finally {
            if (closing) {
                log.trace("{} Closing the task before committing the offsets: {}", this, offsetsToCommit);
                sinkTask.flush(taskProvidedRecordOffset);
            }
        }

        if (taskResetOffsets.isEmpty()) {
            log.debug("{} Skipping offset commit, task opted-out by returning no offsets from preCommit", this);
            onCommitCompleted(null, commitSeqno, null);
            return;
        }

        //get all assign topic queue
        Collection<MessageQueue> allAssignedTopicQueues = this.messageQueues;
        // committable offsets
        final Map<MessageQueue, Long> committableOffsets = new HashMap<>(lastCommittedOffsetsForPartitions);
        for (Map.Entry<MessageQueue, Long> taskResetOffsetEntry : taskResetOffsets.entrySet()) {

            // task reset offset
            final MessageQueue queue = taskResetOffsetEntry.getKey();
            final Long taskResetOffset = taskResetOffsetEntry.getValue();

            if (committableOffsets.containsKey(queue)) {
                long currentOffset = offsetsToCommit.get(queue);
                if (currentOffset >= taskResetOffset ) {
                    committableOffsets.put(queue, taskResetOffset);
                }
            } else if (!allAssignedTopicQueues.contains(queue)) {
                log.warn("{} Ignoring invalid task provided offset {}/{} -- partition not assigned, assignment={}",
                        this, queue, taskResetOffset, allAssignedTopicQueues);
            } else {
                log.debug("{} Ignoring task provided offset {}/{} -- partition not requested, requested={}",
                        this, queue, taskResetOffset, committableOffsets.keySet());
            }

        }

        if (committableOffsets.equals(lastCommittedOffsetsForPartitions)) {
            log.debug("{} Skipping offset commit, no change since last commit", this);
            onCommitCompleted(null, commitSeqno, null);
            return;
        }
        doCommitSync(committableOffsets, commitSeqno);
    }

    /**
     * do commi
     * @param offsets
     * @param seqno
     */
    private void doCommitSync(Map<MessageQueue, Long> offsets, int seqno) {
        log.debug("{} Committing offsets synchronously using sequence number {}: {}", this, seqno, offsets);
        try {
            offsets.forEach((queue, offset)->{
                consumer.getOffsetStore().updateOffset(queue, offset, false);
            });
            onCommitCompleted(null, seqno, offsets);
        } catch (Exception e) {
            onCommitCompleted(e, seqno, offsets);
        }
    }

    /**
     * commit
     * @param error
     * @param seqno
     * @param committedOffsets
     */
    private void onCommitCompleted(Throwable error, long seqno, Map<MessageQueue, Long> committedOffsets) {
        if (commitSeqno != seqno) {
            log.debug("{} Received out of order commit callback for sequence number {}, but most recent sequence number is {}",
                    this, seqno, commitSeqno);
        } else {
            long durationMillis = System.currentTimeMillis() - commitStarted;
            if (error != null) {
                log.error("{} Commit of offsets threw an unexpected exception for sequence number {}: {}",
                        this, seqno, committedOffsets, error);

            } else {
                log.debug("{} Finished offset commit successfully in {} ms for sequence number {}: {}",
                        this, durationMillis, seqno, committedOffsets);
                if (committedOffsets != null) {
                    log.trace("{} Adding to last committed offsets: {}", this, committedOffsets);
                    lastCommittedOffsets.putAll(committedOffsets);
                    log.debug("{} Last committed offsets are now {}", this, committedOffsets);
                }
            }
            committing = false;
        }
    }



    /**
     * Poll for new messages with the given timeout. Should only be invoked by the worker thread.
     */
    protected void poll(long timeoutMs) {
        rewind();
        log.trace("{} Polling consumer with timeout {} ms", this, timeoutMs);
        List<MessageExt> msgs = pollConsumer(timeoutMs);
        assert messageBatch.isEmpty() || msgs.isEmpty();
        log.trace("{} Polling returned {} messages", this, msgs.size());
        receiveMessages(msgs);
    }

    /**
     * reset offset by custom
     */
    private void rewind() {
        Map<MessageQueue, Long> offsets = sinkTaskContext.queuesOffsets();
        if (offsets.isEmpty()) {
            return;
        }
        for (Map.Entry<MessageQueue, Long> entry: offsets.entrySet()) {
            MessageQueue queue = entry.getKey();
            Long offset = entry.getValue();
            if (offset != null) {
                log.trace("{} Rewind {} to offset {}", this, queue, offset);
                try {
                    consumer.seek(queue, offset);
                    lastCommittedOffsets.put(queue, offset);
                    currentOffsets.put(queue, offset);
                } catch (MQClientException e) {
                    // NO-op
                }
            } else {
                log.warn("{} Cannot rewind {} to null offset", this, queue);
            }
        }
        sinkTaskContext.cleanQueuesOffsets();
    }

    /**
     * poll consumer
     * @param timeoutMs
     * @return
     */
    private List<MessageExt> pollConsumer(long timeoutMs) {
        final long beginPullMsgTimestamp = System.currentTimeMillis();
        List<MessageExt> msgs = consumer.poll(timeoutMs);
        // metrics
        recordReadSuccess(msgs.size(), beginPullMsgTimestamp);
        return msgs;
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
        if(messageBatch.isEmpty()){
            originalOffsets.clear();
        }
        for (MessageExt message : messages) {
            this.retryWithToleranceOperator.consumerRecord(message);
            ConnectRecord connectRecord = convertMessages(message);
            originalOffsets.put(
                    new MessageQueue(message.getTopic(), message.getBrokerName(), message.getQueueId()),
                    message.getQueueOffset() + 1
            );
            if (connectRecord != null && !this.retryWithToleranceOperator.failed()) {
                messageBatch.add(connectRecord);
            }
            log.info("Received one message success : msgId {}", message.getMsgId());
        }
        try {
            sinkTask.put(new ArrayList<>(messageBatch));
            currentOffsets.putAll(originalOffsets);
            messageBatch.clear();

            if (!shouldPause()) {
                if (pausedForRetry){
                    resumeAll();
                    pausedForRetry = false;
                }

            }
        } catch (RetriableException e) {
            log.error("task {} put sink recode RetriableException", this, e.getMessage(), e);
            // pause all consumer wait for put data
            pausedForRetry = true;
            pauseAll();
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
        ConnectRecord record = new ConnectRecord(
                recordPartition,
                recordOffset,
                timestamp,
                schemaAndKey == null? null: schemaAndKey.schema(),
                schemaAndKey == null? null: schemaAndKey.value(),
                schemaAndValue.schema(),
                schemaAndValue.value()
        );
        if (retryWithToleranceOperator.failed()) {
            return null;
        }

        // apply the transformations
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
        Set<String> topics = SinkConnectorConfig.parseTopicList(taskConfig);
        if (org.apache.commons.collections4.CollectionUtils.isEmpty(topics)) {
            throw new ConnectException("Sink connector topics config can be null, please check sink connector config info");
        }
        // sub topics
        try {
            for (String topic : topics) {
                consumer.setPullBatchSize(MAX_MESSAGE_NUM);
                consumer.subscribe(topic, "*");
                if (messageQueueListener == null){
                    messageQueueListener = consumer.getMessageQueueListener();
                }
                consumer.setMessageQueueListener(new MessageQueueListener() {
                    @Override
                    public void messageQueueChanged(String subTopic, Set<MessageQueue> mqAll, Set<MessageQueue> mqDivided) {
                        // update assign message queue
                        messageQueueListener.messageQueueChanged(subTopic, mqAll, mqDivided);
                        // listener message queue changed
                        log.info("Message queue changed start, old message queues offset {}", JSON.toJSONString(messageQueues));

                        // remove message queue
                        Set<MessageQueue> removeQueues = new HashSet<>();
                        messageQueues.forEach(messageQueue -> {
                            if (messageQueue.getTopic().equals(subTopic)) {
                                removeQueues.add(messageQueue);
                            }
                        });
                        messageQueues.removeAll(removeQueues);

                        // remove record partitions
                        Set<RecordPartition> waitRemoveQueueMetaDatas = new HashSet<>();
                        recordPartitions.forEach(key -> {
                            if (key.getPartition().get(TOPIC).equals(subTopic)) {
                                waitRemoveQueueMetaDatas.add(key);
                            }
                        });
                        recordPartitions.removeAll(waitRemoveQueueMetaDatas);

                        // add record partitions
                        for (MessageQueue messageQueue : mqDivided) {
                            messageQueues.add(messageQueue);
                            // init queue offset
                            long offset = consumeFromOffset(messageQueue, taskConfig);
                            lastCommittedOffsets.put(messageQueue, offset);
                            currentOffsets.put(messageQueue, offset);
                            RecordPartition recordPartition = ConnectUtil.convertToRecordPartition(messageQueue);
                            recordPartitions.add(recordPartition);
                        }
                        log.info("Message queue changed start, new message queues offset {}", JSON.toJSONString(messageQueues));

                    }
                });
            }
            consumer.start();
        } catch(MQClientException e){
            throw new ConnectException(e);
        }
        log.info("Sink task consumer start. taskConfig {}", JSON.toJSONString(taskConfig));
        sinkTask.init(sinkTaskContext);
        sinkTask.start(taskConfig);
        log.info("{} Sink task finished initialization and start", this);
    }
    /**
     * consume fro offset
     * @param messageQueue
     * @param taskConfig
     */
    public long consumeFromOffset(MessageQueue messageQueue, ConnectKeyValue taskConfig) {

        //-1 when started
        long offset = consumer.getOffsetStore().readOffset(messageQueue, ReadOffsetType.READ_FROM_MEMORY);
        if (0 >= offset) {
            //query from broker
            offset = consumer.getOffsetStore().readOffset(messageQueue, ReadOffsetType.READ_FROM_STORE);
        }

        if (offset < 0 ){
            String consumeFromWhere = taskConfig.getString("consume-from-where");
            if (StringUtils.isBlank(consumeFromWhere)) {
                consumeFromWhere = ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET.name();
            }
            try {
                switch (ConsumeFromWhere.valueOf(consumeFromWhere)) {
                    case CONSUME_FROM_LAST_OFFSET:
                        consumer.seekToEnd(messageQueue);
                        break;
                    case CONSUME_FROM_FIRST_OFFSET:
                        consumer.seekToBegin(messageQueue);
                        break;
                    default:
                        break;
                }
            } catch (MQClientException e) {
                throw new ConnectException(e);
            }
        }

        log.info("Consume {} from {}",messageQueue, offset);
        return offset < 0 ? 0 : offset;
    }



    /**
     * execute poll and send record
     */
    @Override
    protected void execute() {
        while (isRunning()) {
            try {
                long startTimeStamp = System.currentTimeMillis();
                log.info("START pullMessageFromQueues, time started : {}", startTimeStamp);
                if (messageQueues.size() == 0) {
                    log.info("messageQueuesOffsetMap is null, : {}", startTimeStamp);
                    stopPullMsgLatch.await(PULL_MSG_ERROR_BACKOFF_MS, TimeUnit.MILLISECONDS);
                    continue;
                }
                if (shouldPause()) {
                    // pause
                    pauseAll();
                    onPause();
                    try {
                        // wait unpause
                        if (awaitUnpause()) {
                            // check paused for retry
                            if (!pausedForRetry){
                                resumeAll();
                                onResume();
                            }
                        }
                        continue;
                    } catch (InterruptedException e) {
                        // do exception
                    }
                }
                iteration();
            }catch (RetriableException e) {
                log.error(" Sink task {}, pull message RetriableException, Error {} ", this, e.getMessage(), e);
                readRecordFailNum();
            }catch (InterruptedException interruptedException){
                //NO-op
            } catch (Throwable e) {
                log.error(" Sink task {}, pull message Throwable, Error {} ", this, e.getMessage(), e);
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
     * error record reporter
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


