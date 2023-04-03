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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.apache.rocketmq.connect.runtime.connectorwrapper;

import com.alibaba.fastjson.JSON;
import io.openmessaging.KeyValue;
import io.openmessaging.connector.api.component.task.source.SourceTask;
import io.openmessaging.connector.api.data.ConnectRecord;
import io.openmessaging.connector.api.data.RecordConverter;
import io.openmessaging.connector.api.errors.ConnectException;
import io.openmessaging.connector.api.errors.RetriableException;
import io.openmessaging.connector.api.storage.OffsetStorageReader;

import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.selector.SelectMessageQueueByHash;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageAccessor;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.connect.metrics.stats.Avg;
import org.apache.rocketmq.connect.metrics.stats.CumulativeCount;
import org.apache.rocketmq.connect.metrics.stats.Max;
import org.apache.rocketmq.connect.metrics.stats.Rate;
import org.apache.rocketmq.connect.runtime.common.ConnectKeyValue;
import org.apache.rocketmq.connect.runtime.common.LoggerName;
import org.apache.rocketmq.connect.runtime.config.ConnectorConfig;
import org.apache.rocketmq.connect.runtime.config.SourceConnectorConfig;
import org.apache.rocketmq.connect.runtime.config.WorkerConfig;
import org.apache.rocketmq.connect.runtime.connectorwrapper.status.WrapperStatusListener;
import org.apache.rocketmq.connect.runtime.errors.ErrorReporter;
import org.apache.rocketmq.connect.runtime.errors.RetryWithToleranceOperator;
import org.apache.rocketmq.connect.runtime.errors.ToleranceType;
import org.apache.rocketmq.connect.runtime.metrics.ConnectMetrics;
import org.apache.rocketmq.connect.runtime.metrics.ConnectMetricsTemplates;
import org.apache.rocketmq.connect.runtime.metrics.MetricGroup;
import org.apache.rocketmq.connect.runtime.metrics.Sensor;
import org.apache.rocketmq.connect.runtime.service.PositionManagementService;
import org.apache.rocketmq.connect.runtime.stats.ConnectStatsManager;
import org.apache.rocketmq.connect.runtime.stats.ConnectStatsService;
import org.apache.rocketmq.connect.runtime.store.PositionStorageReaderImpl;
import org.apache.rocketmq.connect.runtime.store.PositionStorageWriter;
import org.apache.rocketmq.connect.runtime.utils.ConnectUtil;
import org.apache.rocketmq.connect.runtime.utils.ConnectorTaskId;
import org.apache.rocketmq.connect.runtime.utils.Utils;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.rocketmq.connect.runtime.connectorwrapper.WorkerSinkTask.TOPIC;

/**
 * A wrapper of {@link SourceTask} for runtime.
 */
public class WorkerSourceTask extends WorkerTask {

    private static final Logger log = LoggerFactory.getLogger(LoggerName.ROCKETMQ_RUNTIME);
    private static final long SEND_FAILED_BACKOFF_MS = 100;
    /**
     * The property of message in WHITE_KEY_SET don't need add a connect prefix
     */
    private static final Set<String> WHITE_KEY_SET = new HashSet<>();

    static {
        WHITE_KEY_SET.add(MessageConst.PROPERTY_KEYS);
        WHITE_KEY_SET.add(MessageConst.PROPERTY_TAGS);
    }

    protected final WorkerSourceTaskContext sourceTaskContext;
    /**
     * The implements of the source task.
     */
    private final SourceTask sourceTask;
    private final SourceTaskMetricsGroup sourceTaskMetricsGroup;
    /**
     * Used to read the position of source data source.
     */
    private final OffsetStorageReader offsetStorageReader;
    /**
     * Used to write the position of source data source.
     */
    private final PositionStorageWriter positionStorageWriter;
    /**
     * A converter to parse source data entry to byte[].
     */
    private final RecordConverter keyConverter;
    private final RecordConverter valueConverter;
    /**
     * stat connect
     */
    private final ConnectStatsManager connectStatsManager;
    private final ConnectStatsService connectStatsService;
    private final CountDownLatch stopRequestedLatch;
    private final AtomicReference<Throwable> producerSendException;
    private final RecordOffsetManagement offsetManagement;
    /**
     * A RocketMQ producer to send message to dest MQ.
     */
    private DefaultMQProducer producer;
    private List<ConnectRecord> toSendRecord;
    private volatile RecordOffsetManagement.CommittableOffsets committableOffsets;

    public WorkerSourceTask(WorkerConfig workerConfig,
                            ConnectorTaskId id,
                            SourceTask sourceTask,
                            ClassLoader classLoader,
                            ConnectKeyValue taskConfig,
                            PositionManagementService positionManagementService,
                            RecordConverter keyConverter,
                            RecordConverter valueConverter,
                            DefaultMQProducer producer,
                            AtomicReference<WorkerState> workerState,
                            ConnectStatsManager connectStatsManager,
                            ConnectStatsService connectStatsService,
                            TransformChain<ConnectRecord> transformChain,
                            RetryWithToleranceOperator retryWithToleranceOperator,
                            WrapperStatusListener statusListener,
                            ConnectMetrics connectMetrics) {
        super(workerConfig, id, classLoader, taskConfig, retryWithToleranceOperator, transformChain, workerState, statusListener, connectMetrics);

        this.sourceTask = sourceTask;
        this.offsetStorageReader = new PositionStorageReaderImpl(id.connector(), positionManagementService);
        this.positionStorageWriter = new PositionStorageWriter(id.connector(), positionManagementService);
        this.producer = producer;
        this.valueConverter = valueConverter;
        this.keyConverter = keyConverter;
        this.connectStatsManager = connectStatsManager;
        this.connectStatsService = connectStatsService;
        this.sourceTaskContext = new WorkerSourceTaskContext(offsetStorageReader, this, taskConfig);
        this.stopRequestedLatch = new CountDownLatch(1);
        this.producerSendException = new AtomicReference<>();
        this.offsetManagement = new RecordOffsetManagement();
        this.committableOffsets = RecordOffsetManagement.CommittableOffsets.EMPTY;
        this.sourceTaskMetricsGroup = new SourceTaskMetricsGroup(id, connectMetrics);
    }

    @Nullable
    private static String overwriteTopicFromRecord(ConnectRecord record) {
        KeyValue extensions = record.getExtensions();
        if (extensions == null) {
            log.error("record extensions null , lack of topic config");
            return null;
        }
        String o = extensions.getString(TOPIC, null);
        if (null == o) {
            log.error("Partition map element topic is null , lack of topic config");
            return null;
        }
        return o;
    }

    private List<ConnectRecord> poll() throws InterruptedException {
        try {
            List<ConnectRecord> connectRecords = sourceTask.poll();
            if (CollectionUtils.isEmpty(connectRecords)) {
                return null;
            }
            return connectRecords;
        } catch (RetriableException e) {
            log.error("Source task RetriableException exception, taskconfig {}", JSON.toJSONString(taskConfig), e);
            return null;
        }
    }

    public void removeMetrics() {
        super.removeMetrics();
        Utils.closeQuietly(sourceTaskMetricsGroup, "Remove source " + id.toString() + " metrics");
    }

    @Override
    public void close() {
        sourceTask.stop();
        producer.shutdown();
        stopRequestedLatch.countDown();
        removeMetrics();
        Utils.closeQuietly(transformChain, "transform chain");
        Utils.closeQuietly(retryWithToleranceOperator, "retry operator");
        Utils.closeQuietly(positionStorageWriter, "position storage writer");
    }

    protected void updateCommittableOffsets() {
        RecordOffsetManagement.CommittableOffsets newOffsets = offsetManagement.committableOffsets();
        synchronized (this) {
            this.committableOffsets = this.committableOffsets.updatedWith(newOffsets);
        }
    }

    protected Optional<RecordOffsetManagement.SubmittedPosition> prepareToSendRecord(
            ConnectRecord record
    ) {
        maybeThrowProducerSendException();
        return Optional.of(this.offsetManagement.submitRecord(record.getPosition()));
    }

    /**
     * Send list of sourceDataEntries to MQ.
     */
    private Boolean sendRecord() throws InterruptedException {
        int processed = 0;


        final CalcSourceRecordWrite counter = new CalcSourceRecordWrite(toSendRecord.size(), sourceTaskMetricsGroup);
        for (ConnectRecord preTransformRecord : toSendRecord) {
            retryWithToleranceOperator.sourceRecord(preTransformRecord);
            ConnectRecord record = transformChain.doTransforms(preTransformRecord);
            String topic = maybeCreateAndGetTopic(record);
            Message sourceMessage = convertTransformedRecord(topic, record);
            if (sourceMessage == null || retryWithToleranceOperator.failed()) {
                // commit record
                recordFailed(preTransformRecord);
                counter.skipRecord();
                continue;
            }
            log.trace("{} Appending record to the topic {} , value {}", this, topic, record.getData());
            /**prepare to send record*/
            Optional<RecordOffsetManagement.SubmittedPosition> submittedRecordPosition = prepareToSendRecord(preTransformRecord);
            try {

                SendCallback callback = new SendCallback() {
                    @Override
                    public void onSuccess(SendResult result) {
                        log.info("Successful send message to RocketMQ:{}, Topic {}", result.getMsgId(), result.getMessageQueue().getTopic());
                        // complete record
                        counter.completeRecord();
                        // commit record for custom
                        recordSent(preTransformRecord, sourceMessage, result);
                        // ack record position
                        submittedRecordPosition.ifPresent(RecordOffsetManagement.SubmittedPosition::ack);
                    }

                    @Override
                    public void onException(Throwable throwable) {

                        log.error("Source task send record failed ,error msg {}. message {}", throwable.getMessage(), JSON.toJSONString(sourceMessage), throwable);
                        // skip record
                        counter.skipRecord();
                        // record send failed
                        recordSendFailed(false, sourceMessage, preTransformRecord, throwable);
                    }
                };

                if (StringUtils.isEmpty(sourceMessage.getKeys())) {
                    // Round robin
                    producer.send(sourceMessage, callback);
                } else {
                    // Partition message ordering,
                    // At the same time, ensure that the data is pulled in an orderly manner, which needs to be guaranteed by sourceTask in the business
                    producer.send(sourceMessage, new SelectMessageQueueByHash(), sourceMessage.getKeys(), callback);
                }

            } catch (RetriableException e) {
                log.warn("{} Failed to send record to topic '{}'. Backing off before retrying: ",
                        this, sourceMessage.getTopic(), e);
                // Intercepted as successfully sent, used to continue sending next time
                toSendRecord = toSendRecord.subList(processed, toSendRecord.size());
                // remove pre submit position, for retry
                submittedRecordPosition.ifPresent(RecordOffsetManagement.SubmittedPosition::remove);
                // retry metrics
                counter.retryRemaining();
                return false;
            } catch (InterruptedException e) {
                log.error("Send message InterruptedException. message: {}, error info: {}.", sourceMessage, e);
                // throw e and stop task
                throw e;
            } catch (Exception e) {
                log.error("Send message MQClientException. message: {}, error info: {}.", sourceMessage, e);
                recordSendFailed(true, sourceMessage, preTransformRecord, e);
            }

            processed++;
        }
        toSendRecord = null;
        return true;
    }

    private void prepareToPollTask() {
        maybeThrowProducerSendException();
    }

    private void maybeThrowProducerSendException() {
        if (producerSendException.get() != null) {
            throw new ConnectException(
                    "Unrecoverable exception from producer send callback",
                    producerSendException.get()
            );
        }
    }

    private void recordSendFailed(
            boolean synchronous,
            Message sourceMessage,
            ConnectRecord preTransformRecord,
            Throwable e) {
        if (synchronous) {
            throw new ConnectException("Unrecoverable exception trying to send", e);
        }
        String topic = sourceMessage.getTopic();
        if (retryWithToleranceOperator.getErrorToleranceType() == ToleranceType.ALL) {
            // ignore all error
            log.trace(
                    "Ignoring failed record send: {} failed to send record to {}: ",
                    WorkerSourceTask.this,
                    topic,
                    e
            );
            retryWithToleranceOperator.executeFailed(
                    ErrorReporter.Stage.ROCKETMQ_PRODUCE,
                    WorkerSourceTask.class,
                    preTransformRecord,
                    e);
            commitTaskRecord(preTransformRecord, null);
        } else {
            log.error("{} failed to send record to {}: ", WorkerSourceTask.this, topic, e);
            log.trace("{} Failed record: {}", WorkerSourceTask.this, preTransformRecord);
            producerSendException.compareAndSet(null, e);
        }
    }

    /**
     * failed send
     *
     * @param record
     */
    private void recordFailed(ConnectRecord record) {
        commitTaskRecord(record, null);
    }

    /**
     * send success record
     *
     * @param preTransformRecord
     * @param sourceMessage
     * @param result
     */
    private void recordSent(
            ConnectRecord preTransformRecord,
            Message sourceMessage,
            SendResult result) {
        commitTaskRecord(preTransformRecord, result);
    }

    private void commitTaskRecord(ConnectRecord preTransformRecord, SendResult result) {
        ConnectKeyValue keyValue = null;
        if (result != null) {
            keyValue = new ConnectKeyValue();
            keyValue.put("send.status", result.getSendStatus().name());
            keyValue.put("msg.id", result.getMsgId());
            keyValue.put("topic", result.getMessageQueue().getTopic());
            keyValue.put("broker.name", result.getMessageQueue().getBrokerName());
            keyValue.put("queue.id", result.getMessageQueue().getQueueId());
            keyValue.put("queue.offset", result.getQueueOffset());
            keyValue.put("transaction.id", result.getTransactionId());
            keyValue.put("offset.msg.id", result.getOffsetMsgId());
            keyValue.put("region.id", result.getRegionId());
        }
        sourceTask.commit(preTransformRecord, keyValue == null ? null : keyValue.getProperties());
    }

    /**
     * Convert the source record into a producer record.
     */
    protected Message convertTransformedRecord(final String topic, ConnectRecord record) {
        if (record == null) {
            return null;
        }
        Message sourceMessage = new Message();
        sourceMessage.setTopic(topic);
        byte[] key = retryWithToleranceOperator.execute(() -> keyConverter.fromConnectData(topic, record.getKeySchema(), record.getKey()),
                ErrorReporter.Stage.CONVERTER, keyConverter.getClass());

        byte[] value = retryWithToleranceOperator.execute(() -> valueConverter.fromConnectData(topic, record.getSchema(), record.getData()),
                ErrorReporter.Stage.CONVERTER, valueConverter.getClass());
        if (value.length > ConnectorConfig.MAX_MESSAGE_SIZE) {
            log.error("Send record, message size is greater than {} bytes, record: {}", ConnectorConfig.MAX_MESSAGE_SIZE, JSON.toJSONString(record));
        }
        if (key != null) {
            sourceMessage.setKeys(new String(key));
        }
        sourceMessage.setBody(value);
        if (retryWithToleranceOperator.failed()) {
            return null;
        }
        // put extend msg property
        putExtendMsgProperty(record, sourceMessage, topic);
        return sourceMessage;
    }

    /**
     * maybe create and get topic
     *
     * @param record
     * @return
     */
    private String maybeCreateAndGetTopic(ConnectRecord record) {
        String topic = overwriteTopicFromRecord(record);
        if (StringUtils.isBlank(topic)) {
            // topic from config
            topic = taskConfig.getString(SourceConnectorConfig.CONNECT_TOPICNAME);
        }
        if (StringUtils.isBlank(topic)) {
            throw new ConnectException("source connect lack of topic config");
        }
        if (!ConnectUtil.isTopicExist(workerConfig, topic)) {
            ConnectUtil.createTopic(workerConfig, new TopicConfig(topic));
        }
        return topic;
    }

    private void putExtendMsgProperty(ConnectRecord sourceDataEntry, Message sourceMessage, String topic) {
        KeyValue extensionKeyValues = sourceDataEntry.getExtensions();
        if (null == extensionKeyValues) {
            log.info("extension key value is null.");
            return;
        }
        Set<String> keySet = extensionKeyValues.keySet();
        if (CollectionUtils.isEmpty(keySet)) {
            log.info("extension keySet null.");
            return;
        }
        if (sourceDataEntry.getTimestamp() != null) {
            MessageAccessor.putProperty(sourceMessage, ConnectorConfig.CONNECT_TIMESTAMP, sourceDataEntry.getTimestamp().toString());
        }
        for (String key : keySet) {
            if (WHITE_KEY_SET.contains(key)) {
                MessageAccessor.putProperty(sourceMessage, key, extensionKeyValues.getString(key));
            } else {
                MessageAccessor.putProperty(sourceMessage, "connect-ext-" + key, extensionKeyValues.getString(key));
            }
        }

    }

    /**
     * initinalize and start
     */
    @Override
    protected void initializeAndStart() {
        try {
            producer.start();
        } catch (MQClientException e) {
            log.error("{} Source task producer start failed!!", this);
            throw new ConnectException(e);
        }
        sourceTask.init(sourceTaskContext);
        sourceTask.start(taskConfig);
        log.info("{} Source task finished initialization and start", this);
    }


    /**
     * execute poll and send record
     */
    @Override
    protected void execute() {
        while (isRunning()) {
            updateCommittableOffsets();

            if (shouldPause()) {
                onPause();
                try {
                    // wait unpause
                    if (awaitUnpause()) {
                        onResume();
                    }
                    continue;
                } catch (InterruptedException e) {
                    // do exception
                }
            }


            if (CollectionUtils.isEmpty(toSendRecord)) {
                try {
                    prepareToPollTask();
                    long start = System.currentTimeMillis();
                    toSendRecord = poll();
                    if (null != toSendRecord && toSendRecord.size() > 0) {
                        recordPollReturned(toSendRecord.size(), System.currentTimeMillis() - start);
                    }
                    if (toSendRecord == null) {
                        continue;
                    }
                    log.trace("{} About to send {} records to RocketMQ", this, toSendRecord.size());
                    if (!sendRecord()) {
                        stopRequestedLatch.await(SEND_FAILED_BACKOFF_MS, TimeUnit.MILLISECONDS);
                    }
                } catch (InterruptedException e) {
                    // Ignore and allow to exit.
                } catch (Exception e) {
                    try {
                        finalOffsetCommit(true);
                    } catch (Exception offsetException) {
                        log.error("Failed to commit offsets for already-failing task", offsetException);
                    }
                    throw e;
                } finally {
                    finalOffsetCommit(false);
                    // record source poll times
                    connectStatsManager.incSourceRecordPollTotalTimes();
                }
            }
            AtomicLong atomicLong = connectStatsService.singleSourceTaskTimesTotal(id().toString());
            if (null != atomicLong) {
                atomicLong.addAndGet(toSendRecord == null ? 0 : toSendRecord.size());
            }
        }
    }

    protected void finalOffsetCommit(boolean b) {

        offsetManagement.awaitAllMessages(
                workerConfig.getOffsetCommitTimeoutMsConfig(),
                TimeUnit.MILLISECONDS
        );
        updateCommittableOffsets();
        commitOffsets();
    }

    public boolean commitOffsets() {
        long commitTimeoutMs = workerConfig.getOffsetCommitTimeoutMsConfig();
        log.debug("{} Committing offsets", this);

        long started = System.currentTimeMillis();
        long timeout = started + commitTimeoutMs;

        RecordOffsetManagement.CommittableOffsets offsetsToCommit;
        synchronized (this) {
            offsetsToCommit = this.committableOffsets;
            this.committableOffsets = RecordOffsetManagement.CommittableOffsets.EMPTY;
        }

        if (committableOffsets.isEmpty()) {
            log.debug("{} Either no records were produced by the task since the last offset commit, "
                            + "or every record has been filtered out by a transformation "
                            + "or dropped due to transformation or conversion errors.",
                    this
            );
            // We continue with the offset commit process here instead of simply returning immediately
            // in order to invoke SourceTask::commit and record metrics for a successful offset commit
        } else {
            log.info("{} Committing offsets for {} acknowledged messages", this, committableOffsets.numCommittableMessages());
            if (committableOffsets.hasPending()) {
                log.debug("{} There are currently {} pending messages spread across {} source partitions whose offsets will not be committed. "
                                + "The source partition with the most pending messages is {}, with {} pending messages",
                        this,
                        committableOffsets.numUncommittableMessages(),
                        committableOffsets.numDeques(),
                        committableOffsets.largestDequePartition(),
                        committableOffsets.largestDequeSize()
                );
            } else {
                log.debug("{} There are currently no pending messages for this offset commit; "
                                + "all messages dispatched to the task's producer since the last commit have been acknowledged",
                        this
                );
            }
        }

        // write offset
        offsetsToCommit.offsets().forEach(positionStorageWriter::writeOffset);

        // begin flush
        if (!positionStorageWriter.beginFlush()) {
            // There was nothing in the offsets to process, but we still mark a successful offset commit.
            long durationMillis = System.currentTimeMillis() - started;
            recordCommitSuccess(durationMillis);
            log.debug("{} Finished offset commitOffsets successfully in {} ms",
                    this, durationMillis);
            commitSourceTask();
            return true;
        }

        Future<Void> flushFuture = positionStorageWriter.doFlush((error, key, result) -> {
            if (error != null) {
                log.error("{} Failed to flush offsets to storage: ", WorkerSourceTask.this, error);
            } else {
                log.trace("{} Finished flushing offsets to storage", WorkerSourceTask.this);
            }
        });
        try {
            flushFuture.get(Math.max(timeout - System.currentTimeMillis(), 0), TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            log.warn("{} Flush of offsets interrupted, cancelling", this);
            positionStorageWriter.cancelFlush();
            recordCommitFailure(System.currentTimeMillis() - started);
            return false;
        } catch (ExecutionException e) {
            log.error("{} Flush of offsets threw an unexpected exception: ", this, e);
            positionStorageWriter.cancelFlush();
            recordCommitFailure(System.currentTimeMillis() - started);
            return false;
        } catch (TimeoutException e) {
            log.error("{} Timed out waiting to flush offsets to storage; will try again on next flush interval with latest offsets", this);
            positionStorageWriter.cancelFlush();
            recordCommitFailure(System.currentTimeMillis() - started);
            return false;
        }
        long durationMillis = System.currentTimeMillis() - started;
        recordCommitSuccess(durationMillis);
        log.debug("{} Finished commitOffsets successfully in {} ms",
                this, durationMillis);
        commitSourceTask();
        return true;
    }

    protected void commitSourceTask() {
        try {
            this.sourceTask.commit();
        } catch (Throwable t) {
            log.error("{} Exception thrown while calling task.commit()", this, t);
        }
    }


    protected void recordPollReturned(int numRecordsInBatch, long millTime) {
        sourceTaskMetricsGroup.recordPoll(numRecordsInBatch, millTime);
    }

    static class SourceTaskMetricsGroup implements AutoCloseable {
        private final Sensor sourceRecordPoll;
        private final Sensor sourceRecordWrite;
        private final Sensor sourceRecordActiveCount;
        private final Sensor pollTime;
        private int activeRecordCount;

        private MetricGroup metricGroup;

        public SourceTaskMetricsGroup(ConnectorTaskId id, ConnectMetrics connectMetrics) {
            ConnectMetricsTemplates templates = connectMetrics.templates();
            metricGroup = connectMetrics.group(
                    templates.connectorTagName(), id.connector(),
                    templates.taskTagName(), Integer.toString(id.task()));

            sourceRecordPoll = metricGroup.sensor();
            sourceRecordPoll.addStat(new Rate(connectMetrics.registry(), metricGroup.name(templates.sourceRecordPollRate)));
            sourceRecordPoll.addStat(new CumulativeCount(connectMetrics.registry(), metricGroup.name(templates.sourceRecordPollTotal)));

            sourceRecordWrite = metricGroup.sensor();
            sourceRecordWrite.addStat(new Rate(connectMetrics.registry(), metricGroup.name(templates.sourceRecordWriteRate)));
            sourceRecordWrite.addStat(new CumulativeCount(connectMetrics.registry(), metricGroup.name(templates.sourceRecordWriteTotal)));

            pollTime = metricGroup.sensor();
            pollTime.addStat(new Max(connectMetrics.registry(), metricGroup.name(templates.sourceRecordPollBatchTimeMax)));
            pollTime.addStat(new Avg(connectMetrics.registry(), metricGroup.name(templates.sourceRecordPollBatchTimeAvg)));

            sourceRecordActiveCount = metricGroup.sensor();
            sourceRecordActiveCount.addStat(new Max(connectMetrics.registry(), metricGroup.name(templates.sourceRecordActiveCountMax)));
            sourceRecordActiveCount.addStat(new Avg(connectMetrics.registry(), metricGroup.name(templates.sourceRecordActiveCountAvg)));
        }

        @Override
        public void close() {
            metricGroup.close();
        }

        void recordPoll(int batchSize, long duration) {
            sourceRecordPoll.record(batchSize);
            pollTime.record(duration);
            activeRecordCount += batchSize;
            sourceRecordActiveCount.record(activeRecordCount);
        }

        void recordWrite(int recordCount) {
            sourceRecordWrite.record(recordCount);
            activeRecordCount -= recordCount;
            activeRecordCount = Math.max(0, activeRecordCount);
            sourceRecordActiveCount.record(activeRecordCount);
        }
    }

    static class CalcSourceRecordWrite {
        private final SourceTaskMetricsGroup metricsGroup;
        private final int batchSize;
        private boolean completed = false;
        private int counter;

        public CalcSourceRecordWrite(int batchSize, SourceTaskMetricsGroup metricsGroup) {
            assert batchSize > 0;
            assert metricsGroup != null;
            this.batchSize = batchSize;
            counter = batchSize;
            this.metricsGroup = metricsGroup;
        }

        public void skipRecord() {
            if (counter > 0 && --counter == 0) {
                finishedAllWrites();
            }
        }

        public void completeRecord() {
            if (counter > 0 && --counter == 0) {
                finishedAllWrites();
            }
        }

        public void retryRemaining() {
            finishedAllWrites();
        }

        private void finishedAllWrites() {
            if (!completed) {
                metricsGroup.recordWrite(batchSize - counter);
                completed = true;
            }
        }
    }
}
