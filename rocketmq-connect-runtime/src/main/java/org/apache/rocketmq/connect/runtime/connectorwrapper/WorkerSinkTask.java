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
import com.codahale.metrics.MetricRegistry;
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
import java.nio.charset.StandardCharsets;
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
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.client.consumer.DefaultLitePullConsumer;
import org.apache.rocketmq.client.consumer.MessageQueueListener;
import org.apache.rocketmq.client.consumer.store.ReadOffsetType;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.connect.metrics.stats.Avg;
import org.apache.rocketmq.connect.metrics.stats.CumulativeCount;
import org.apache.rocketmq.connect.metrics.stats.Max;
import org.apache.rocketmq.connect.metrics.stats.Rate;
import org.apache.rocketmq.connect.runtime.common.ConnectKeyValue;
import org.apache.rocketmq.connect.runtime.common.LoggerName;
import org.apache.rocketmq.connect.runtime.config.ConnectorConfig;
import org.apache.rocketmq.connect.runtime.config.SinkConnectorConfig;
import org.apache.rocketmq.connect.runtime.config.WorkerConfig;
import org.apache.rocketmq.connect.runtime.connectorwrapper.status.WrapperStatusListener;
import org.apache.rocketmq.connect.runtime.errors.ErrorReporter;
import org.apache.rocketmq.connect.runtime.errors.RetryWithToleranceOperator;
import org.apache.rocketmq.connect.runtime.errors.WorkerErrorRecordReporter;
import org.apache.rocketmq.connect.runtime.metrics.ConnectMetrics;
import org.apache.rocketmq.connect.runtime.metrics.ConnectMetricsTemplates;
import org.apache.rocketmq.connect.runtime.metrics.MetricGroup;
import org.apache.rocketmq.connect.runtime.metrics.Sensor;
import org.apache.rocketmq.connect.runtime.stats.ConnectStatsManager;
import org.apache.rocketmq.connect.runtime.stats.ConnectStatsService;
import org.apache.rocketmq.connect.runtime.utils.ConnectUtil;
import org.apache.rocketmq.connect.runtime.utils.ConnectorTaskId;
import org.apache.rocketmq.connect.runtime.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.Collections.singleton;


/**
 * A wrapper of {@link SinkTask} for runtime.
 */
public class WorkerSinkTask extends WorkerTask {

    public static final String BROKER_NAME = "brokerName";
    public static final String QUEUE_ID = "queueId";
    public static final String TOPIC = "topic";
    public static final String QUEUE_OFFSET = "queueOffset";
    private static final Logger log = LoggerFactory.getLogger(LoggerName.ROCKETMQ_RUNTIME);
    private static final Integer MAX_MESSAGE_NUM = 32;
    private static final long PULL_MSG_ERROR_BACKOFF_MS = 1000 * 5;
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
    /**
     * A RocketMQ consumer to pull message from MQ.
     */
    private final DefaultLitePullConsumer consumer;
    /**
     * cache offset
     */
    private final Map<MessageQueue, Long> lastCommittedOffsets;
    private final Map<MessageQueue, Long> currentOffsets;
    private final Map<MessageQueue, Long> originalOffsets;
    private final Set<MessageQueue> messageQueues;
    private final List<ConnectRecord> messageBatch;
    /**
     * stat
     */
    private final ConnectStatsManager connectStatsManager;
    private final ConnectStatsService connectStatsService;

    private final CountDownLatch stopPullMsgLatch;
    private final SinkTaskMetricsGroup sinkTaskMetricsGroup;
    /**
     * The implements of the sink task.
     */
    private SinkTask sinkTask;
    /**
     * A converter to parse sink data entry to object.
     */
    private RecordConverter keyConverter;
    private RecordConverter valueConverter;
    private Set<RecordPartition> recordPartitions = new CopyOnWriteArraySet<>();
    private MessageQueueListener messageQueueListener = null;
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

    public WorkerSinkTask(WorkerConfig workerConfig,
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
                          WrapperStatusListener statusListener,
                          ConnectMetrics connectMetrics) {
        super(workerConfig, id, classLoader, taskConfig, retryWithToleranceOperator, transformChain, workerState, statusListener, connectMetrics);
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
        this.messageBatch = new ArrayList<>();

        // commit
        this.nextCommit = System.currentTimeMillis() + workerConfig.getOffsetCommitIntervalMsConfig();
        this.committing = false;
        this.commitSeqno = 0;
        this.commitStarted = -1;
        // pause for retry
        this.pausedForRetry = false;
        this.sinkTaskMetricsGroup = new SinkTaskMetricsGroup(id, connectMetrics);
    }


    protected void iteration() {
        final long offsetCommitIntervalMs = workerConfig.getOffsetCommitIntervalMsConfig();
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
        //  pre reset commit
        preCommit();
        List<MessageExt> msgs = pollConsumer(timeoutMs);
        assert messageBatch.isEmpty() || msgs.isEmpty();
        log.info("{} Polling returned {} messages", this, msgs.size());
        receiveMessages(msgs);
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
     *
     * @param now
     * @param closing
     */
    private void commitOffsets(long now, boolean closing) {
        commitOffsets(now, closing, messageQueues);
    }

    private void commitOffsets(long now, boolean closing, Set<MessageQueue> messageQueues) {
        log.trace("Start commit offsets {}", messageQueues);

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

        Map<MessageQueue, Long> lastCommittedQueuesOffsets = this.lastCommittedOffsets.entrySet()
                .stream()
                .filter(e -> offsetsToCommit.containsKey(e.getKey()))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        Map<MessageQueue, Long> taskProvidedOffsets = new ConcurrentHashMap<>();
        Map<RecordPartition, RecordOffset> taskProvidedRecordOffsets = new ConcurrentHashMap<>();
        try {
            log.info(" Call task.preCommit reset offset : {}", offsetsToCommit);
            Map<RecordPartition, RecordOffset> recordOffsetsToCommit = new ConcurrentHashMap<>();
            for (Map.Entry<MessageQueue, Long> messageQueueOffset : offsetsToCommit.entrySet()) {
                RecordPartition recordPartition = ConnectUtil.convertToRecordPartition(messageQueueOffset.getKey());
                RecordOffset recordOffset = ConnectUtil.convertToRecordOffset(messageQueueOffset.getValue());
                recordOffsetsToCommit.put(recordPartition, recordOffset);
            }

            // pre commit
            taskProvidedRecordOffsets = sinkTask.preCommit(recordOffsetsToCommit);
            // task provided commit offset
            for (Map.Entry<RecordPartition, RecordOffset> entry : taskProvidedRecordOffsets.entrySet()) {
                taskProvidedOffsets.put(ConnectUtil.convertToMessageQueue(entry.getKey()), ConnectUtil.convertToOffset(entry.getValue()));
            }

        } catch (Throwable t) {
            if (closing) {
                log.warn(" {} Offset commit failed {}", this);
            } else {
                log.error("{} Offset commit failed, reset to last committed offsets", this, t);
                for (Map.Entry<MessageQueue, Long> entry : lastCommittedQueuesOffsets.entrySet()) {
                    try {
                        consumer.seek(entry.getKey(), entry.getValue());
                    } catch (MQClientException e) {
                    }
                }
                currentOffsets.putAll(lastCommittedQueuesOffsets);
            }
            onCommitCompleted(t, commitSeqno, null);
            return;
        } finally {
            if (closing) {
                log.trace("{} Closing the task before committing the offsets: {}", this, offsetsToCommit);
            }
        }
        if (taskProvidedOffsets.isEmpty()) {
            log.debug("{} Skipping offset commit, task opted-out by returning no offsets from preCommit", this);
            onCommitCompleted(null, commitSeqno, null);
            return;
        }
        compareAndCommit(offsetsToCommit, lastCommittedQueuesOffsets, taskProvidedOffsets);
    }

    /**
     * compare and commit
     *
     * @param offsetsToCommit
     * @param lastCommittedQueuesOffsets
     * @param taskProvidedOffsets
     */
    private void compareAndCommit(Map<MessageQueue, Long> offsetsToCommit, Map<MessageQueue, Long> lastCommittedQueuesOffsets, Map<MessageQueue, Long> taskProvidedOffsets) {

        //Get all assign topic message queue
        Collection<MessageQueue> assignedTopicQueues = this.messageQueues;
        // committable offsets
        final Map<MessageQueue, Long> committableOffsets = new HashMap<>(lastCommittedQueuesOffsets);
        for (Map.Entry<MessageQueue, Long> taskProvidedOffsetsEntry : taskProvidedOffsets.entrySet()) {

            // task provided offset
            final MessageQueue queue = taskProvidedOffsetsEntry.getKey();
            final Long taskProvidedOffset = taskProvidedOffsetsEntry.getValue();

            //check reblance remove
            if (!assignedTopicQueues.contains(queue)) {
                log.warn("{} After rebalancing, the MessageQueue is removed from the current consumer {}/{} , assignment={}",
                        this, queue, taskProvidedOffset, assignedTopicQueues);
                continue;
            }

            if (!committableOffsets.containsKey(queue)) {
                log.debug("{} The MessageQueue provided by the task is not subscribed {}/{} , requested={}",
                        this, queue, taskProvidedOffset, committableOffsets.keySet());
                continue;
            }

            if (committableOffsets.containsKey(queue)) {
                // current offset
                long currentOffset = offsetsToCommit.get(queue);
                // compare and set
                if (currentOffset >= taskProvidedOffset) {
                    committableOffsets.put(queue, taskProvidedOffset);
                }
            }
        }

        if (committableOffsets.equals(lastCommittedQueuesOffsets)) {
            log.debug("{} Skipping offset commit, no change since last commit", this);
            onCommitCompleted(null, commitSeqno, null);
            return;
        }
        doCommitSync(committableOffsets, commitSeqno);
    }

    /**
     * do commit
     *
     * @param offsets
     * @param seqno
     */
    private void doCommitSync(Map<MessageQueue, Long> offsets, int seqno) {
        log.debug("{} Committing offsets synchronously using sequence number {}: {}", this, seqno, offsets);
        try {
            for (Map.Entry<MessageQueue, Long> offsetEntry : offsets.entrySet()) {
                consumer.getOffsetStore().updateOffset(offsetEntry.getKey(), offsetEntry.getValue(), true);
                // consumer.getOffsetStore().updateConsumeOffsetToBroker(offsetEntry.getKey(), offsetEntry.getValue(), false);
            }
            onCommitCompleted(null, seqno, offsets);
        } catch (Exception e) {
            onCommitCompleted(e, seqno, offsets);
        }
    }

    /**
     * commit
     *
     * @param error
     * @param seqno
     * @param committedOffsets
     */
    private void onCommitCompleted(Throwable error, long seqno, Map<MessageQueue, Long> committedOffsets) {
        if (commitSeqno != seqno) {
            // skip this commit
            sinkTaskMetricsGroup.recordOffsetCommitSkip();
            return;
        }
        if (error != null) {
            log.error("{} An exception was thrown when committing commit offset, sequence number {}: {}",
                    this, seqno, committedOffsets, error);
            recordCommitFailure(System.currentTimeMillis() - commitStarted);
        } else {
            log.debug("{} Finished offset commit successfully in {} ms for sequence number {}: {}",
                    this, System.currentTimeMillis() - commitStarted, seqno, committedOffsets);
            if (committedOffsets != null) {
                lastCommittedOffsets.putAll(committedOffsets);
                log.debug("{} Last committed offsets are now {}", this, committedOffsets);
            }
            sinkTaskMetricsGroup.recordOffsetCommitSuccess();
        }
        committing = false;
    }

    /**
     * reset offset by custom
     */
    private void preCommit() {
        Map<MessageQueue, Long> offsets = sinkTaskContext.queuesOffsets();
        if (offsets.isEmpty()) {
            return;
        }
        for (Map.Entry<MessageQueue, Long> entry : offsets.entrySet()) {
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
            }
        }
        sinkTaskContext.cleanQueuesOffsets();
    }

    /**
     * poll consumer
     *
     * @param timeoutMs
     * @return
     */
    private List<MessageExt> pollConsumer(long timeoutMs) {
        List<MessageExt> msgs = consumer.poll(timeoutMs);
        // metrics
        recordReadSuccess(msgs.size());
        return msgs;
    }

    public void removeMetrics() {
        super.removeMetrics();
        Utils.closeQuietly(this.sinkTaskMetricsGroup, "Remove sink " + id.toString() + " metrics");
    }

    @Override
    public void close() {
        sinkTask.stop();
        consumer.shutdown();
        stopPullMsgLatch.countDown();
        removeMetrics();
        Utils.closeQuietly(transformChain, "transform chain");
        Utils.closeQuietly(retryWithToleranceOperator, "retry operator");
    }


    /**
     * receive message from MQ.
     *
     * @param messages
     */
    private void receiveMessages(List<MessageExt> messages) {
        if (messageBatch.isEmpty()) {
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
            long start = System.currentTimeMillis();
            sinkTask.put(new ArrayList<>(messageBatch));
            //metrics
            recordMultiple(messageBatch.size());
            sinkTaskMetricsGroup.recordPut(System.currentTimeMillis() - start);

            currentOffsets.putAll(originalOffsets);
            messageBatch.clear();

            if (!shouldPause()) {
                if (pausedForRetry) {
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
        String connectTimestamp = properties.get(ConnectorConfig.CONNECT_TIMESTAMP);
        Long timestamp = StringUtils.isNotEmpty(connectTimestamp) ? Long.valueOf(connectTimestamp) : message.getBornTimestamp();

        // partition and offset
        RecordPartition recordPartition = ConnectUtil.convertToRecordPartition(message.getTopic(), message.getBrokerName(), message.getQueueId());
        RecordOffset recordOffset = ConnectUtil.convertToRecordOffset(message.getQueueOffset());


        byte[] keysBytes = null;
        String keys = message.getKeys();
        if (StringUtils.isNotEmpty(keys) && StringUtils.isNotBlank(keys)) {
            keysBytes = keys.getBytes(StandardCharsets.UTF_8);
        }
        byte[] finalKeysBytes = keysBytes;
        SchemaAndValue schemaAndKey = retryWithToleranceOperator.execute(() -> keyConverter.toConnectData(message.getTopic(), finalKeysBytes),
            ErrorReporter.Stage.CONVERTER, keyConverter.getClass());

        // convert value
        SchemaAndValue schemaAndValue = retryWithToleranceOperator.execute(() -> valueConverter.toConnectData(message.getTopic(), message.getBody()),
                ErrorReporter.Stage.CONVERTER, valueConverter.getClass());
        ConnectRecord record = new ConnectRecord(
                recordPartition,
                recordOffset,
                timestamp,
                schemaAndKey == null ? null : schemaAndKey.schema(),
                schemaAndKey == null ? null : schemaAndKey.value(),
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
        Set<String> topics = new SinkConnectorConfig(taskConfig).parseTopicList();
        if (org.apache.commons.collections4.CollectionUtils.isEmpty(topics)) {
            throw new ConnectException("Sink connector topics config can be null, please check sink connector config info");
        }
        // sub topics
        try {
            for (String topic : topics) {
                consumer.setPullBatchSize(MAX_MESSAGE_NUM);
                consumer.subscribe(topic, "*");
            }
            if (messageQueueListener == null) {
                messageQueueListener = consumer.getMessageQueueListener();
            }
            consumer.setMessageQueueListener(new MessageQueueListener() {
                @Override
                public void messageQueueChanged(String subTopic, Set<MessageQueue> mqAll, Set<MessageQueue> mqDivided) {
                    // update assign message queue
                    messageQueueListener.messageQueueChanged(subTopic, mqAll, mqDivided);
                    // listener message queue changed
                    log.info("Message queue changed start, old message queues offset {}", JSON.toJSONString(messageQueues));

                    if (isStopping()) {
                        log.trace("Skipping partition revocation callback as task has already been stopped");
                        return;
                    }
                    // remove and close message queue
                    log.info("Task {},MessageQueueChanged, old messageQueuesOffsetMap {}", id.toString(), JSON.toJSONString(messageQueues));
                    removeAndCloseMessageQueue(subTopic, mqDivided);

                    // add new message queue
                    assignMessageQueue(mqDivided);
                    log.info("Task {}, Message queue changed end, new message queues offset {}", id, JSON.toJSONString(messageQueues));
                    preCommit();
                    log.info("Message queue changed start, new message queues offset {}", JSON.toJSONString(messageQueues));

                }
            });
            consumer.start();
        } catch (MQClientException e) {
            log.error("Task {},InitializeAndStart MQClientException", id.toString(), e);
            throw new ConnectException(e);
        }
        log.info("Sink task consumer start. taskConfig {}", JSON.toJSONString(taskConfig));
        sinkTask.init(sinkTaskContext);
        sinkTask.start(taskConfig);
        log.info("{} Sink task finished initialization and start", this);
    }

    /**
     * remove and close message queue
     *
     * @param queues
     */
    public void removeAndCloseMessageQueue(String topic, Set<MessageQueue> queues) {
        Set<MessageQueue> removeMessageQueues;
        if (queues == null) {
            removeMessageQueues = new HashSet<>();
            for (MessageQueue messageQueue : messageQueues) {
                if (messageQueue.getTopic().equals(topic)) {
                    removeMessageQueues.add(messageQueue);
                }
            }
        } else {
            // filter not contains in messageQueues
            removeMessageQueues = messageQueues.stream().filter(messageQueue -> topic.equals(messageQueue.getTopic()) && !queues.contains(messageQueue)).collect(Collectors.toSet());
        }
        if (removeMessageQueues == null || removeMessageQueues.isEmpty()) {
            return;
        }

        // clean message queues offset
        closeMessageQueues(removeMessageQueues, false);

        // remove record partitions
        Set<RecordPartition> waitRemoveQueueMetaDatas = new HashSet<>();
        for (MessageQueue messageQueue : removeMessageQueues) {
            recordPartitions.forEach(key -> {
                if (key.getPartition().get(TOPIC).equals(messageQueue.getTopic()) && key.getPartition().get(BROKER_NAME).equals(messageQueue.getBrokerName())
                    && Integer.valueOf(String.valueOf(key.getPartition().get(QUEUE_ID))).equals(messageQueue.getQueueId())) {
                    waitRemoveQueueMetaDatas.add(key);
                }
            });
        }

        recordPartitions.removeAll(waitRemoveQueueMetaDatas);
        // start remove
        messageQueues.removeAll(removeMessageQueues);
    }

    /**
     * remove offset from currentOffsets/lastCommittedOffsets
     * remove message from messageBatch
     *
     * @param queues
     * @param lost
     */
    private void closeMessageQueues(Set<MessageQueue> queues, boolean lost) {

        if (!lost) {
            commitOffsets(System.currentTimeMillis(), true, queues);
        } else {
            log.trace("{} Closing the task as partitions have been lost: {}", this, queues);
            currentOffsets.keySet().removeAll(queues);
        }
        lastCommittedOffsets.keySet().removeAll(queues);

        messageBatch.removeIf(record -> {
            MessageQueue messageQueue = ConnectUtil.convertToMessageQueue(record.getPosition().getPartition());
            return queues.contains(messageQueue);
        });
    }

    public void assignMessageQueue(Set<MessageQueue> queues) {
        if (queues == null) {
            return;
        }
        Set<MessageQueue> newMessageQueues = queues.stream().filter(messageQueue -> !messageQueues.contains(messageQueue)).collect(Collectors.toSet());

        // add record queues
        messageQueues.addAll(newMessageQueues);
        for (MessageQueue messageQueue : newMessageQueues) {
            // init queue offset
            long offset = consumeFromOffset(messageQueue, taskConfig);
            lastCommittedOffsets.put(messageQueue, offset);
            currentOffsets.put(messageQueue, offset);
            RecordPartition recordPartition = ConnectUtil.convertToRecordPartition(messageQueue);
            recordPartitions.add(recordPartition);
        }
        boolean wasPausedForRedelivery = pausedForRetry;
        pausedForRetry = wasPausedForRedelivery && !messageBatch.isEmpty();
        //Paused for retry. When the subscribed MessageQueue changes, it needs to pause again
        if (pausedForRetry) {
            pauseAll();
        } else {
            // Paused for retry. If the data has been written through the plug-in, all queues can be resumed
            if (pausedForRetry) {
                resumeAll();
            }
            // reset
            sinkTaskContext.getPausedQueues().retainAll(messageQueues);
            if (shouldPause()) {
                pauseAll();
                return;
            }
            if (!sinkTaskContext.getPausedQueues().isEmpty()) {
                consumer.pause(sinkTaskContext.getPausedQueues());
            }
        }
        log.info("Message queue changed start, new message queues offset {}", JSON.toJSONString(messageQueues));

    }


    /**
     * consume fro offset
     *
     * @param messageQueue
     * @param taskConfig
     */
    public long consumeFromOffset(MessageQueue messageQueue, ConnectKeyValue taskConfig) {

        //-1 when started
        long offset = consumer.getOffsetStore().readOffset(messageQueue, ReadOffsetType.READ_FROM_MEMORY);
        if (0 > offset) {
            //query from broker
            offset = consumer.getOffsetStore().readOffset(messageQueue, ReadOffsetType.READ_FROM_STORE);
        }

        if (offset < 0) {
            String consumeFromWhere = taskConfig.getString(ConnectorConfig.CONSUME_FROM_WHERE);
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

        log.info("Consume {} from {}", messageQueue, offset);
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
                            if (!pausedForRetry) {
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
            } catch (RetriableException e) {
                log.error(" Sink task {}, pull message RetriableException, Error {} ", this, e.getMessage(), e);
            } catch (InterruptedException interruptedException) {
                //NO-op
            } catch (Throwable e) {
                log.error(" Sink task {}, pull message Throwable, Error {} ", this, e.getMessage(), e);
                throw e;
            }
        }

    }

    public Set<RecordPartition> getRecordPartitions() {
        return recordPartitions;
    }

    @Override
    protected void recordMultiple(int size) {
        super.recordMultiple(size);
        sinkTaskMetricsGroup.recordSend(size);
    }

    @Override
    protected void recordCommitFailure(long duration) {
        super.recordCommitFailure(duration);
    }

    @Override
    protected void recordCommitSuccess(long duration) {
        super.recordCommitSuccess(duration);
        sinkTaskMetricsGroup.recordOffsetCommitSuccess();
    }


    /**
     * error record reporter
     *
     * @return
     */
    public WorkerErrorRecordReporter errorRecordReporter() {
        return errorRecordReporter;
    }

    private void recordReadSuccess(int recordSize) {
        sinkTaskMetricsGroup.recordRead(recordSize);
    }


    static class SinkTaskMetricsGroup implements AutoCloseable {
        private final MetricGroup metricGroup;

        private final Sensor sinkRecordRead;
        private final Sensor sinkRecordSend;

        private final Sensor offsetCompletion;
        private final Sensor offsetCompletionSkip;
        private final Sensor putBatchTime;


        public SinkTaskMetricsGroup(ConnectorTaskId id, ConnectMetrics connectMetrics) {
            ConnectMetricsTemplates templates = connectMetrics.templates();
            metricGroup = connectMetrics
                    .group(templates.connectorTagName(), id.connector(), templates.taskTagName(),
                            Integer.toString(id.task()));

            MetricRegistry registry = connectMetrics.registry();

            sinkRecordRead = metricGroup.sensor();
            sinkRecordRead.addStat(new Rate(registry, metricGroup.name(templates.sinkRecordReadRate)));
            sinkRecordRead.addStat(new CumulativeCount(registry, metricGroup.name(templates.sinkRecordReadTotal)));

            sinkRecordSend = metricGroup.sensor();
            sinkRecordSend.addStat(new Rate(registry, metricGroup.name(templates.sinkRecordSendRate)));
            sinkRecordSend.addStat(new Rate(registry, metricGroup.name(templates.sinkRecordSendTotal)));

            putBatchTime = metricGroup.sensor();
            putBatchTime.addStat(new Max(registry, metricGroup.name(templates.sinkRecordPutBatchTimeMax)));
            putBatchTime.addStat(new Avg(registry, metricGroup.name(templates.sinkRecordPutBatchTimeAvg)));

            offsetCompletion = metricGroup.sensor();
            offsetCompletion.addStat(new Rate(registry, metricGroup.name(templates.sinkRecordOffsetCommitCompletionRate)));
            offsetCompletion.addStat(new CumulativeCount(registry, metricGroup.name(templates.sinkRecordOffsetCommitCompletionTotal)));

            offsetCompletionSkip = metricGroup.sensor();
            offsetCompletionSkip.addStat(new Rate(registry, metricGroup.name(templates.sinkRecordOffsetCommitSkipRate)));
            offsetCompletionSkip.addStat(new CumulativeCount(registry, metricGroup.name(templates.sinkRecordOffsetCommitSkipTotal)));


        }

        public void close() {
            metricGroup.close();
        }

        void recordRead(int batchSize) {
            sinkRecordRead.record(batchSize);
        }

        void recordSend(int batchSize) {
            sinkRecordSend.record(batchSize);
        }

        void recordPut(long duration) {
            putBatchTime.record(duration);
        }

        void recordOffsetCommitSuccess() {
            offsetCompletion.record();
        }

        void recordOffsetCommitSkip() {
            offsetCompletionSkip.record();
        }

    }

}


