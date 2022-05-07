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
import io.openmessaging.connector.api.component.task.source.SourceTaskContext;
import io.openmessaging.connector.api.data.ConnectRecord;
import io.openmessaging.connector.api.data.Converter;
import io.openmessaging.connector.api.data.RecordOffset;
import io.openmessaging.connector.api.data.RecordPartition;
import io.openmessaging.connector.api.data.RecordPosition;
import io.openmessaging.connector.api.errors.ConnectException;
import io.openmessaging.connector.api.errors.RetriableException;
import io.openmessaging.connector.api.storage.OffsetStorageReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
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
import org.apache.rocketmq.connect.runtime.converter.RocketMQConverter;
import org.apache.rocketmq.connect.runtime.service.PositionManagementService;
import org.apache.rocketmq.connect.runtime.stats.ConnectStatsManager;
import org.apache.rocketmq.connect.runtime.stats.ConnectStatsService;
import org.apache.rocketmq.connect.runtime.store.PositionStorageReaderImpl;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.rocketmq.connect.runtime.connectorwrapper.WorkerSinkTask.TOPIC;

/**
 * A wrapper of {@link SourceTask} for runtime.
 */
public class WorkerSourceTask implements WorkerTask {

    private static final Logger log = LoggerFactory.getLogger(LoggerName.ROCKETMQ_RUNTIME);

    /**
     * Connector name of current task.
     */
    private String connectorName;

    /**
     * The implements of the source task.
     */
    private SourceTask sourceTask;

    /**
     * The configs of current source task.
     */
    private ConnectKeyValue taskConfig;

    /**
     * Atomic state variable
     */
    private AtomicReference<WorkerTaskState> state;

    private final PositionManagementService positionManagementService;

    /**
     * Used to read the position of source data source.
     */
    private OffsetStorageReader offsetStorageReader;

    /**
     * A RocketMQ producer to send message to dest MQ.
     */
    private DefaultMQProducer producer;

    /**
     * A converter to parse source data entry to byte[].
     */
    private Converter recordConverter;

    private final AtomicReference<WorkerState> workerState;

    private ConnectStatsManager connectStatsManager;

    private ConnectStatsService connectStatsService;

    private List<ConnectRecord> toSendRecord;

    private TransformChain<ConnectRecord> transformChain;

    public WorkerSourceTask(String connectorName,
        SourceTask sourceTask,
        ConnectKeyValue taskConfig,
        PositionManagementService positionManagementService,
        Converter recordConverter,
        DefaultMQProducer producer,
        AtomicReference<WorkerState> workerState,
        ConnectStatsManager connectStatsManager,
        ConnectStatsService connectStatsService,
        TransformChain<ConnectRecord> transformChain) {
        this.connectorName = connectorName;
        this.sourceTask = sourceTask;
        this.taskConfig = taskConfig;
        this.positionManagementService = positionManagementService;
        this.offsetStorageReader = new PositionStorageReaderImpl(positionManagementService);
        this.producer = producer;
        this.recordConverter = recordConverter;
        this.state = new AtomicReference<>(WorkerTaskState.NEW);
        this.workerState = workerState;
        this.connectStatsManager = connectStatsManager;
        this.connectStatsService = connectStatsService;
        this.transformChain = transformChain;
    }

    /**
     * Start a source task, and send data entry to MQ cyclically.
     */
    @Override
    public void run() {
        try {
            producer.start();
            log.info("Source task producer start.");
            state.compareAndSet(WorkerTaskState.NEW, WorkerTaskState.PENDING);
            sourceTask.init(taskConfig);
            sourceTask.start(new SourceTaskContext() {

                @Override public OffsetStorageReader offsetStorageReader() {
                    return offsetStorageReader;
                }

                @Override public String getConnectorName() {
                    return taskConfig.getString(RuntimeConfigDefine.CONNECTOR_ID);
                }

                @Override public String getTaskName() {
                    return taskConfig.getString(RuntimeConfigDefine.TASK_ID);
                }
            });
            state.compareAndSet(WorkerTaskState.PENDING, WorkerTaskState.RUNNING);
            log.info("Source task start, config:{}", JSON.toJSONString(taskConfig));
            while (WorkerState.STARTED == workerState.get() && WorkerTaskState.RUNNING == state.get()) {
                if (CollectionUtils.isEmpty(toSendRecord)) {
                    try {
                        toSendRecord = poll();
                        if (null != toSendRecord && toSendRecord.size() > 0) {
                            connectStatsManager.incSourceRecordPollTotalNums();
                            connectStatsManager.incSourceRecordPollNums(taskConfig.getString(RuntimeConfigDefine.TASK_ID));
                            sendRecord();
                        }
                    } catch (RetriableException e) {
                        connectStatsManager.incSourceRecordPollTotalFailNums();
                        connectStatsManager.incSourceRecordPollFailNums(taskConfig.getString(RuntimeConfigDefine.TASK_ID));
                        log.error("Source task RetriableException exception", e);
                    } catch (Exception e) {
                        connectStatsManager.incSourceRecordPollTotalFailNums();
                        connectStatsManager.incSourceRecordPollFailNums(taskConfig.getString(RuntimeConfigDefine.TASK_ID));
                        log.error("Source task RetriableException exception", e);
                        state.set(WorkerTaskState.ERROR);
                    }
                }
                AtomicLong atomicLong = connectStatsService.singleSourceTaskTimesTotal(taskConfig.getString(RuntimeConfigDefine.TASK_ID));
                if (null != atomicLong) {
                    atomicLong.addAndGet(toSendRecord == null ? 0 : toSendRecord.size());
                }
            }
            sourceTask.stop();
            state.compareAndSet(WorkerTaskState.STOPPING, WorkerTaskState.STOPPED);
            log.info("Source task stop, config:{}", JSON.toJSONString(taskConfig));
        } catch (Exception e) {
            log.error("Run task failed., task config: " +  JSON.toJSONString(taskConfig), e);
            state.set(WorkerTaskState.ERROR);
        } finally {
            if (producer != null) {
                producer.shutdown();
                log.info("Source task producer shutdown. task config {}", JSON.toJSONString(taskConfig));
            }
        }
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
                ConnectRecord connectRecord1 = this.transformChain.doTransforms(connectRecord);
                if (null != connectRecord1) {
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
    public void stop() {
        state.compareAndSet(WorkerTaskState.RUNNING, WorkerTaskState.STOPPING);
        log.warn("Stop a task success.");
    }

    @Override
    public void cleanup() {
        log.info("Cleaning a task, current state {}, destination state {}", state.get().name(), WorkerTaskState.TERMINATED.name());
        if (state.compareAndSet(WorkerTaskState.STOPPED, WorkerTaskState.TERMINATED) ||
            state.compareAndSet(WorkerTaskState.ERROR, WorkerTaskState.TERMINATED)) {
            log.info("Cleaning a task success");
        } else {
            log.error("[BUG] cleaning a task but it's not in STOPPED or ERROR state");
        }
    }

    /**
     * Send list of sourceDataEntries to MQ.
     */
    private void sendRecord() throws InterruptedException, RemotingException, MQClientException {
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
            if (null == recordConverter || recordConverter instanceof RocketMQConverter) {
                putExtendMsgProperty(sourceDataEntry, sourceMessage, topic);
                Object payload = sourceDataEntry.getData();
                if (null != payload) {
                    final byte[] messageBody = (String.valueOf(payload)).getBytes();
                    if (messageBody.length > RuntimeConfigDefine.MAX_MESSAGE_SIZE) {
                        log.error("Send record, message size is greater than {} bytes, sourceDataEntry: {}", RuntimeConfigDefine.MAX_MESSAGE_SIZE, JSON.toJSONString(sourceDataEntry));
                        continue;
                    }
                    sourceMessage.setBody(messageBody);
                }
            } else {
                final byte[] messageBody = JSON.toJSONString(sourceDataEntry).getBytes();
                if (messageBody.length > RuntimeConfigDefine.MAX_MESSAGE_SIZE) {
                    log.error("Send record, message size is greater than {} bytes, sourceDataEntry: {}", RuntimeConfigDefine.MAX_MESSAGE_SIZE, JSON.toJSONString(sourceDataEntry));
                    continue;
                }
                sourceMessage.setBody(messageBody);
            }
            try {
                producer.send(sourceMessage, new SendCallback() {
                    @Override public void onSuccess(org.apache.rocketmq.client.producer.SendResult result) {
                        log.info("Successful send message to RocketMQ:{}, Topic {}", result.getMsgId(), result.getMessageQueue().getTopic());
                        connectStatsManager.incSourceRecordWriteTotalNums();
                        connectStatsManager.incSourceRecordWriteNums(taskConfig.getString(RuntimeConfigDefine.TASK_ID));
                        RecordPartition partition = position.getPartition();
                        try {
                            if (null != partition && null != position) {
                                Map<String, String> offsetMap = (Map<String, String>) offset.getOffset();
                                offsetMap.put(RuntimeConfigDefine.UPDATE_TIMESTAMP, String.valueOf(sourceDataEntry.getTimestamp()));
                                positionManagementService.putPosition(partition, offset);
                            }

                        } catch (Exception e) {
                            log.error("Source task save position info failed. partition {}, offset {}", JSON.toJSONString(partition), JSON.toJSONString(offset), e);
                        }
                    }

                    @Override public void onException(Throwable throwable) {
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
            if (key.equals(MessageConst.PROPERTY_KEYS) || key.equals(MessageConst.PROPERTY_TAGS)) {
                MessageAccessor.putProperty(sourceMessage, key, extensionKeyValues.getString(key));
            } else {
                MessageAccessor.putProperty(sourceMessage, "connect-ext-" + key, extensionKeyValues.getString(key));
            }
        }
    }

    @Override
    public WorkerTaskState getState() {
        return this.state.get();
    }

    @Override
    public String getConnectorName() {
        return connectorName;
    }

    @Override
    public ConnectKeyValue getTaskConfig() {
        return taskConfig;
    }

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
}
