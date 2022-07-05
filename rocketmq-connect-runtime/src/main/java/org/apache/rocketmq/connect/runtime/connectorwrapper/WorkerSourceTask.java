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
import com.alibaba.fastjson.serializer.SerializerFeature;
import io.openmessaging.KeyValue;
import io.openmessaging.connector.api.component.task.source.SourceTask;
import io.openmessaging.connector.api.data.ConnectRecord;
import io.openmessaging.connector.api.data.RecordConverter;
import io.openmessaging.connector.api.data.RecordPartition;
import io.openmessaging.connector.api.data.RecordPosition;
import io.openmessaging.connector.api.errors.ConnectException;
import io.openmessaging.connector.api.errors.RetriableException;
import io.openmessaging.connector.api.storage.OffsetStorageReader;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageAccessor;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.connect.runtime.common.ConnectKeyValue;
import org.apache.rocketmq.connect.runtime.common.LoggerName;
import org.apache.rocketmq.connect.runtime.config.RuntimeConfigDefine;
import org.apache.rocketmq.connect.runtime.errors.ErrorReporter;
import org.apache.rocketmq.connect.runtime.errors.RetryWithToleranceOperator;
import org.apache.rocketmq.connect.runtime.errors.ToleranceType;
import org.apache.rocketmq.connect.runtime.service.PositionManagementService;
import org.apache.rocketmq.connect.runtime.stats.ConnectStatsManager;
import org.apache.rocketmq.connect.runtime.stats.ConnectStatsService;
import org.apache.rocketmq.connect.runtime.store.PositionStorageReaderImpl;
import org.apache.rocketmq.connect.runtime.store.PositionStorageWriter;
import org.apache.rocketmq.connect.runtime.utils.ConnectorTaskId;
import org.apache.rocketmq.connect.runtime.utils.Utils;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.rocketmq.connect.runtime.connectorwrapper.WorkerSinkTask.TOPIC;

/**
 * A wrapper of {@link SourceTask} for runtime.
 */
public class WorkerSourceTask extends WorkerTask {

    private static final Logger log = LoggerFactory.getLogger(LoggerName.ROCKETMQ_RUNTIME);
    private static final long SEND_FAILED_BACKOFF_MS = 100;
    /**
     * The implements of the source task.
     */
    private final SourceTask sourceTask;

    /**
     * The configs of current source task.
     */
    private final ConnectKeyValue taskConfig;
    protected final WorkerSourceTaskContext sourceTaskContext;

    /**
     * Used to read the position of source data source.
     */
    private final OffsetStorageReader offsetStorageReader;

    /**
     * Used to write the position of source data source.
     */
    private final PositionStorageWriter positionStorageWriter;

    /**
     * A RocketMQ producer to send message to dest MQ.
     */
    private DefaultMQProducer producer;

    /**
     * A converter to parse source data entry to byte[].
     */
    private final RecordConverter recordConverter;

    /**
     * stat connect
     */
    private final ConnectStatsManager connectStatsManager;
    private final ConnectStatsService connectStatsService;

    private final CountDownLatch stopRequestedLatch;
    private final AtomicReference<Throwable> producerSendException;
    private List<ConnectRecord> toSendRecord;
    /**
     * The property of message in WHITE_KEY_SET don't need add a connect prefix
     */
    private static final Set<String> WHITE_KEY_SET = new HashSet<>();
    static {
        WHITE_KEY_SET.add(MessageConst.PROPERTY_KEYS);
        WHITE_KEY_SET.add(MessageConst.PROPERTY_TAGS);
    }

    public WorkerSourceTask(ConnectorTaskId id,
                            SourceTask sourceTask,
                            ClassLoader classLoader,
                            ConnectKeyValue taskConfig,
                            PositionManagementService positionManagementService,
                            RecordConverter recordConverter,
                            DefaultMQProducer producer,
                            AtomicReference<WorkerState> workerState,
                            ConnectStatsManager connectStatsManager,
                            ConnectStatsService connectStatsService,
                            TransformChain<ConnectRecord> transformChain,
                            RetryWithToleranceOperator retryWithToleranceOperator) {
        super(id, classLoader, taskConfig, retryWithToleranceOperator, transformChain, workerState);

        this.sourceTask = sourceTask;
        this.taskConfig = taskConfig;
        this.offsetStorageReader = new PositionStorageReaderImpl(id.connector(), positionManagementService);
        this.positionStorageWriter = new PositionStorageWriter(id.connector(), positionManagementService);
        this.producer = producer;
        this.recordConverter = recordConverter;
        this.connectStatsManager = connectStatsManager;
        this.connectStatsService = connectStatsService;
        this.sourceTaskContext = new WorkerSourceTaskContext(offsetStorageReader, this, taskConfig);
        this.stopRequestedLatch = new CountDownLatch(1);
        this.producerSendException = new AtomicReference<>();
    }

    private List<ConnectRecord> poll() throws InterruptedException {
        try {
            List<ConnectRecord> connectRecordList = sourceTask.poll();
            if (CollectionUtils.isEmpty(connectRecordList)) {
                return null;
            }
            return connectRecordList;
        } catch (RetriableException e) {
            log.error("Source task RetriableException exception, taskconfig {}", JSON.toJSONString(taskConfig), e);
            return null;
        }
    }

    @Override
    public void close() {
        producer.shutdown();
        stopRequestedLatch.countDown();
        Utils.closeQuietly(transformChain, "transform chain");
        Utils.closeQuietly(retryWithToleranceOperator, "retry operator");
    }

    /**
     * Send list of sourceDataEntries to MQ.
     */
    private Boolean sendRecord() throws InterruptedException{
        int processed = 0;
        for (ConnectRecord preTransformRecord : toSendRecord) {
            retryWithToleranceOperator.sourceRecord(preTransformRecord);
            ConnectRecord record = transformChain.doTransforms(preTransformRecord);
            String topic = maybeCreateAndGetTopic(record);
            Message sourceMessage =convertTransformedRecord(topic, record);
            if (sourceMessage == null || retryWithToleranceOperator.failed()) {
                // commit record
                recordDropped(preTransformRecord);
                continue;
            }
            try {
                producer.send(sourceMessage, new SendCallback() {
                    @Override
                    public void onSuccess(SendResult result) {
                        log.info("Successful send message to RocketMQ:{}, Topic {}", result.getMsgId(), result.getMessageQueue().getTopic());
                        connectStatsManager.incSourceRecordWriteTotalNums();
                        connectStatsManager.incSourceRecordWriteNums(taskConfig.getString(RuntimeConfigDefine.TASK_ID));

                        // custom commit record
                        recordSent(preTransformRecord, sourceMessage, result);

                        RecordPosition position = record.getPosition();
                        RecordPartition partition = position.getPartition();
                        // commit offset
                        if (null != partition && null != position) {
                            Map<String, String> offsetMap = (Map<String, String>) position.getOffset();
                            offsetMap.put(RuntimeConfigDefine.UPDATE_TIMESTAMP, String.valueOf(record.getTimestamp()));
                            positionStorageWriter.putPosition(partition, position.getOffset());
                        }
                    }

                    @Override
                    public void onException(Throwable throwable) {
                        log.error("Source task send record failed ,error msg {}. message {}", throwable.getMessage(), JSON.toJSONString(sourceMessage), throwable);
                        connectStatsManager.incSourceRecordWriteTotalFailNums();
                        connectStatsManager.incSourceRecordWriteFailNums(taskConfig.getString(RuntimeConfigDefine.TASK_ID));
                        // send failed message
                        recordSendFailed(false, sourceMessage, preTransformRecord, throwable);
                    }
                });
            } catch (RetriableException  e) {
                log.warn("{} Failed to send record to topic '{}'. Backing off before retrying: ",
                        this, sourceMessage.getTopic(), e);
                toSendRecord = toSendRecord.subList(processed, toSendRecord.size());
                return false;
            } catch (MQClientException | RemotingException  e) {
                log.error("Send message MQClientException. message: {}, error info: {}.", sourceMessage, e);
                connectStatsManager.incSourceRecordWriteTotalFailNums();
                connectStatsManager.incSourceRecordWriteFailNums(taskConfig.getString(RuntimeConfigDefine.TASK_ID));
                recordSendFailed(true, sourceMessage, preTransformRecord, e);
            } catch (InterruptedException e) {
                log.error("Send message InterruptedException. message: {}, error info: {}.", sourceMessage, e);
                connectStatsManager.incSourceRecordWriteTotalFailNums();
                connectStatsManager.incSourceRecordWriteFailNums(taskConfig.getString(RuntimeConfigDefine.TASK_ID));
                throw e;
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
     * @param record
     */
    private void recordDropped(ConnectRecord record) {
        commitTaskRecord(record, null);
    }


    /**
     * send success record
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
        if (result != null){
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
        sourceTask.commit(preTransformRecord, keyValue == null? null : keyValue.getProperties());
    }


    /**
     * Convert the source record into a producer record.
     *
     * @param record the transformed record
     * @return the producer record which can sent over to Kafka. A null is returned if the input is null or
     * if an error was encountered during any of the converter stages.
     */
    protected Message convertTransformedRecord(final String topic,ConnectRecord record) {
        if (record == null) {
            return null;
        }
        Message sourceMessage = new Message();
        sourceMessage.setTopic(topic);
        // converter
        if (recordConverter == null) {
            final byte[] messageBody = JSON.toJSONString(record, SerializerFeature.DisableCircularReferenceDetect,  SerializerFeature.WriteMapNullValue).getBytes();
            if (messageBody.length > RuntimeConfigDefine.MAX_MESSAGE_SIZE) {
                log.error("Send record, message size is greater than {} bytes, record: {}", RuntimeConfigDefine.MAX_MESSAGE_SIZE, JSON.toJSONString(record));
            }
            sourceMessage.setBody(messageBody);
        } else {
            byte[] messageBody = retryWithToleranceOperator.execute(() -> recordConverter.fromConnectData(topic, record.getSchema(), record.getData()),
                    ErrorReporter.Stage.CONVERTER, recordConverter.getClass());
            if (messageBody.length > RuntimeConfigDefine.MAX_MESSAGE_SIZE) {
                log.error("Send record, message size is greater than {} bytes, record: {}", RuntimeConfigDefine.MAX_MESSAGE_SIZE, JSON.toJSONString(record));
            }
            sourceMessage.setBody(messageBody);
        }

        if (retryWithToleranceOperator.failed()) {
            return null;
        }
        // put extend msg property
        putExtendMsgProperty(record, sourceMessage, topic);
        return sourceMessage;
    }

    /**
     * maybe create and get topic
     * @param record
     * @return
     */
    private String maybeCreateAndGetTopic(ConnectRecord record) {
        String topic = taskConfig.getString(RuntimeConfigDefine.CONNECT_TOPICNAME);
        if (StringUtils.isBlank(topic)) {
            RecordPosition recordPosition = record.getPosition();
            if (null == recordPosition) {
                log.error("connect-topicname config is null and recordPosition is null , lack of topic config");
            }
            RecordPartition partition = recordPosition.getPartition();
            if (null == partition) {
                log.error("connect-topicname config is null and partition is null , lack of topic config");
            }
            Map<String, ?> partitionMap = partition.getPartition();
            if (null == partitionMap) {
                log.error("connect-topicname config is null and partitionMap is null , lack of topic config");
            }
            Object o = partitionMap.get(TOPIC);
            if (null == o) {
                log.error("connect-topicname config is null and partitionMap.get is null , lack of topic config");
            }
            topic = (String) o;
        }
        if (StringUtils.isBlank(topic)) {
            throw new ConnectException("source connect lack of topic config");
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


    protected void recordPollReturned(int numRecordsInBatch) {
        connectStatsManager.incSourceRecordPollTotalNums(numRecordsInBatch);
        connectStatsManager.incSourceRecordPollNums(taskConfig.getString(RuntimeConfigDefine.TASK_ID), numRecordsInBatch);
    }

    /**
     * execute poll and send record
     */
    @Override
    protected void execute() {
        while (isRunning()) {
            if (CollectionUtils.isEmpty(toSendRecord)) {
                try {
                    prepareToPollTask();
                    toSendRecord = poll();
                    if (null != toSendRecord && toSendRecord.size() > 0) {
                        recordPollReturned(toSendRecord.size());
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
                } finally {
                    // record source poll times
                    connectStatsManager.incSourceRecordPollTotalTimes();
                }
            }
            AtomicLong atomicLong = connectStatsService.singleSourceTaskTimesTotal(taskConfig.getString(RuntimeConfigDefine.TASK_ID));
            if (null != atomicLong) {
                atomicLong.addAndGet(toSendRecord == null ? 0 : toSendRecord.size());
            }
        }
    }

}
