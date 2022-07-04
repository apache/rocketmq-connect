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
import io.openmessaging.connector.api.data.RecordOffset;
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
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageAccessor;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.connect.runtime.common.ConnectKeyValue;
import org.apache.rocketmq.connect.runtime.common.LoggerName;
import org.apache.rocketmq.connect.runtime.config.RuntimeConfigDefine;
import org.apache.rocketmq.connect.runtime.errors.ErrorReporter;
import org.apache.rocketmq.connect.runtime.errors.RetryWithToleranceOperator;
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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.rocketmq.connect.runtime.connectorwrapper.WorkerSinkTask.TOPIC;

/**
 * A wrapper of {@link SourceTask} for runtime.
 */
public class WorkerSourceTask extends WorkerTask {

    private static final Logger log = LoggerFactory.getLogger(LoggerName.ROCKETMQ_RUNTIME);

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


    private List<ConnectRecord> toSendRecord;

    private final TransformChain<ConnectRecord> transformChain;

    private final RetryWithToleranceOperator retryWithToleranceOperator;
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
        super(id, classLoader, taskConfig, retryWithToleranceOperator, workerState);

        this.sourceTask = sourceTask;
        this.taskConfig = taskConfig;
        this.offsetStorageReader = new PositionStorageReaderImpl(id.connector(), positionManagementService);
        this.positionStorageWriter = new PositionStorageWriter(id.connector(), positionManagementService);
        this.producer = producer;
        this.recordConverter = recordConverter;
        this.connectStatsManager = connectStatsManager;
        this.connectStatsService = connectStatsService;
        this.transformChain = transformChain;
        this.retryWithToleranceOperator = retryWithToleranceOperator;
        this.transformChain.retryWithToleranceOperator(this.retryWithToleranceOperator);
        this.sourceTaskContext = new WorkerSourceTaskContext(offsetStorageReader, this, taskConfig);
    }

    private List<ConnectRecord> poll() throws InterruptedException {
        List<ConnectRecord> connectRecordList = null;
        try {
            connectRecordList = sourceTask.poll();
            if (CollectionUtils.isEmpty(connectRecordList)) {
                return null;
            }
            List<ConnectRecord> connectRecordList1 = new ArrayList<>(32);
            for (ConnectRecord connectRecord : connectRecordList) {
                retryWithToleranceOperator.sourceRecord(connectRecord);
                ConnectRecord connectRecord1 = this.transformChain.doTransforms(connectRecord);
                if (null != connectRecord1 && !retryWithToleranceOperator.failed()) {
                    connectRecordList1.add(connectRecord1);
                }
            }
            return connectRecordList1;
        } catch (RetriableException e) {
            log.error("Source task RetriableException exception, taskconfig {}", JSON.toJSONString(taskConfig), e);
            return null;
        }
    }

    @Override
    public void close() {
        Utils.closeQuietly(retryWithToleranceOperator, "retry operator");
        Utils.closeQuietly(transformChain, "transform chain");
    }

    /**
     * Send list of sourceDataEntries to MQ.
     */
    private void sendRecord() throws InterruptedException{
        for (ConnectRecord sourceDataEntry : toSendRecord) {
            RecordPosition position = sourceDataEntry.getPosition();
            RecordOffset offset = position.getOffset();

            Message sourceMessage = new Message();
            String topic = null;
            topic = taskConfig.getString(RuntimeConfigDefine.CONNECT_TOPICNAME);
            if (StringUtils.isBlank(topic)) {
                RecordPosition recordPosition = sourceDataEntry.getPosition();
                if (null == recordPosition) {
                    log.error("connect-topicname config is null and recordPosition is null , lack of topic config");
                    return;
                }
                RecordPartition partition = recordPosition.getPartition();
                if (null == partition) {
                    log.error("connect-topicname config is null and partition is null , lack of topic config");
                    return;
                }
                Map<String, ?> partitionMap = partition.getPartition();
                if (null == partitionMap) {
                    log.error("connect-topicname config is null and partitionMap is null , lack of topic config");
                    return;
                }
                Object o = partitionMap.get(TOPIC);
                if (null == o) {
                    log.error("connect-topicname config is null and partitionMap.get is null , lack of topic config");
                    return;
                }
                topic = (String) o;
            }
            if (StringUtils.isBlank(topic)) {
                throw new ConnectException("source connect lack of topic config");
            }
            sourceMessage.setTopic(topic);
            // converter
            if (recordConverter == null) {
                final byte[] messageBody = JSON.toJSONString(sourceDataEntry, SerializerFeature.DisableCircularReferenceDetect,  SerializerFeature.WriteMapNullValue).getBytes();
                if (messageBody.length > RuntimeConfigDefine.MAX_MESSAGE_SIZE) {
                    log.error("Send record, message size is greater than {} bytes, sourceDataEntry: {}", RuntimeConfigDefine.MAX_MESSAGE_SIZE, JSON.toJSONString(sourceDataEntry));
                    continue;
                }
                sourceMessage.setBody(messageBody);
            } else {
                String finalTopic = topic;
                byte[] messageBody = retryWithToleranceOperator.execute(() -> recordConverter.fromConnectData(finalTopic, sourceDataEntry.getSchema(), sourceDataEntry.getData()),
                        ErrorReporter.Stage.CONVERTER, recordConverter.getClass());
                if (messageBody.length > RuntimeConfigDefine.MAX_MESSAGE_SIZE) {
                    log.error("Send record, message size is greater than {} bytes, sourceDataEntry: {}", RuntimeConfigDefine.MAX_MESSAGE_SIZE, JSON.toJSONString(sourceDataEntry));
                    continue;
                }
                sourceMessage.setBody(messageBody);
            }
            // put extend msg property
            putExtendMsgProperty(sourceDataEntry, sourceMessage, topic);

            try {
                producer.send(sourceMessage, new SendCallback() {
                    @Override
                    public void onSuccess(org.apache.rocketmq.client.producer.SendResult result) {
                        log.info("Successful send message to RocketMQ:{}, Topic {}", result.getMsgId(), result.getMessageQueue().getTopic());
                        connectStatsManager.incSourceRecordWriteTotalNums();
                        connectStatsManager.incSourceRecordWriteNums(taskConfig.getString(RuntimeConfigDefine.TASK_ID));
                        RecordPartition partition = position.getPartition();
                        try {
                            if (null != partition && null != position) {
                                Map<String, String> offsetMap = (Map<String, String>) offset.getOffset();
                                offsetMap.put(RuntimeConfigDefine.UPDATE_TIMESTAMP, String.valueOf(sourceDataEntry.getTimestamp()));
                                positionStorageWriter.putPosition(partition, offset);
                            }

                        } catch (Exception e) {
                            log.error("Source task save position info failed. partition {}, offset {}", JSON.toJSONString(partition), JSON.toJSONString(offset), e);
                        }
                    }

                    @Override
                    public void onException(Throwable throwable) {
                        log.error("Source task send record failed ,error msg {}. message {}", throwable.getMessage(), JSON.toJSONString(sourceMessage), throwable);
                        connectStatsManager.incSourceRecordWriteTotalFailNums();
                        connectStatsManager.incSourceRecordWriteFailNums(taskConfig.getString(RuntimeConfigDefine.TASK_ID));
                    }
                });
            } catch (MQClientException e) {
                log.error("Send message MQClientException. message: {}, error info: {}.", sourceMessage, e);
                connectStatsManager.incSourceRecordWriteTotalFailNums();
                connectStatsManager.incSourceRecordWriteFailNums(taskConfig.getString(RuntimeConfigDefine.TASK_ID));
            } catch (RemotingException e) {
                log.error("Send message RemotingException. message: {}, error info: {}.", sourceMessage, e);
                connectStatsManager.incSourceRecordWriteTotalFailNums();
                connectStatsManager.incSourceRecordWriteFailNums(taskConfig.getString(RuntimeConfigDefine.TASK_ID));
            } catch (InterruptedException e) {
                log.error("Send message InterruptedException. message: {}, error info: {}.", sourceMessage, e);
                connectStatsManager.incSourceRecordWriteTotalFailNums();
                connectStatsManager.incSourceRecordWriteFailNums(taskConfig.getString(RuntimeConfigDefine.TASK_ID));
                throw e;
            }
        }
        toSendRecord = null;
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

    /**
     * execute poll and send record
     */
    @Override
    protected void execute() {
        while (isRunning()) {
            if (CollectionUtils.isEmpty(toSendRecord)) {
                try {
                    toSendRecord = poll();
                    if (null != toSendRecord && toSendRecord.size() > 0) {
                        connectStatsManager.incSourceRecordPollTotalNums(toSendRecord.size());
                        connectStatsManager.incSourceRecordPollNums(taskConfig.getString(RuntimeConfigDefine.TASK_ID), toSendRecord.size());
                        sendRecord();
                    }
                } catch (RetriableException e) {
                    connectStatsManager.incSourceRecordPollTotalFailNums();
                    connectStatsManager.incSourceRecordPollFailNums(taskConfig.getString(RuntimeConfigDefine.TASK_ID));
                    log.error("Source task RetriableException exception", e);
                    throw e;

                } catch (InterruptedException e) {
                        // Ignore and allow to exit.
                } catch (RuntimeException e) {
                    connectStatsManager.incSourceRecordPollTotalFailNums();
                    connectStatsManager.incSourceRecordPollFailNums(taskConfig.getString(RuntimeConfigDefine.TASK_ID));
                    log.error("Source task Exception exception", e);
                    throw e;
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
