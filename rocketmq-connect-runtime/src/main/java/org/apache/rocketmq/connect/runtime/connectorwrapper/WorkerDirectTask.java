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
import io.openmessaging.connector.api.component.task.sink.SinkTask;
import io.openmessaging.connector.api.component.task.sink.SinkTaskContext;
import io.openmessaging.connector.api.component.task.source.SourceTask;
import io.openmessaging.connector.api.component.task.source.SourceTaskContext;
import io.openmessaging.connector.api.data.ConnectRecord;
import io.openmessaging.connector.api.data.RecordOffset;
import io.openmessaging.connector.api.data.RecordPartition;
import io.openmessaging.connector.api.storage.OffsetStorageReader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.commons.collections.MapUtils;
import org.apache.rocketmq.connect.runtime.common.ConnectKeyValue;
import org.apache.rocketmq.connect.runtime.common.LoggerName;
import org.apache.rocketmq.connect.runtime.config.RuntimeConfigDefine;
import org.apache.rocketmq.connect.runtime.service.PositionManagementService;
import org.apache.rocketmq.connect.runtime.store.PositionStorageReaderImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A wrapper of {@link SinkTask} and {@link SourceTask} for runtime.
 */
public class WorkerDirectTask implements WorkerTask {

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

    private final PositionManagementService positionManagementService;

    private final OffsetStorageReader positionStorageReader;

    private final AtomicReference<WorkerState> workerState;

    public WorkerDirectTask(String connectorName,
        SourceTask sourceTask,
        SinkTask sinkTask,
        ConnectKeyValue taskConfig,
        PositionManagementService positionManagementService,
        AtomicReference<WorkerState> workerState) {
        this.connectorName = connectorName;
        this.sourceTask = sourceTask;
        this.sinkTask = sinkTask;
        this.taskConfig = taskConfig;
        this.positionManagementService = positionManagementService;
        this.positionStorageReader = new PositionStorageReaderImpl(positionManagementService);
        this.state = new AtomicReference<>(WorkerTaskState.NEW);
        this.workerState = workerState;
    }

    /**
     * Start a source task, and send data entry to MQ cyclically.
     */
    @Override
    public void run() {
        try {
            starkSinkTask();
            startSourceTask();
            log.info("Direct task start, config:{}", JSON.toJSONString(taskConfig));
            while (WorkerState.STARTED == workerState.get() && WorkerTaskState.RUNNING == state.get()) {
                try {
                    Collection<ConnectRecord> toSendEntries = sourceTask.poll();
                    if (null != toSendEntries && toSendEntries.size() > 0) {
                        sendRecord(toSendEntries);
                    }
                } catch (Exception e) {
                    log.error("Direct task runtime exception", e);
                    state.set(WorkerTaskState.ERROR);
                }
            }
            stopSourceTask();
            stopSinkTask();
            state.compareAndSet(WorkerTaskState.STOPPING, WorkerTaskState.STOPPED);
            log.info("Direct task stop, config:{}", JSON.toJSONString(taskConfig));
        } catch (Exception e) {
            log.error("Run task failed.", e);
            state.set(WorkerTaskState.ERROR);
        }
    }

    private void sendRecord(Collection<ConnectRecord> sourceDataEntries) {
        List<ConnectRecord> sinkDataEntries = new ArrayList<>(sourceDataEntries.size());
        Map<RecordPartition, RecordOffset> map = new HashMap<>();
        for (ConnectRecord sourceDataEntry : sourceDataEntries) {
            sinkDataEntries.add(sourceDataEntry);
            RecordPartition recordPartition = sourceDataEntry.getPosition().getPartition();
            RecordOffset recordOffset = sourceDataEntry.getPosition().getOffset();
            if (null != recordPartition && null != recordOffset) {
                map.put(recordPartition, recordOffset);
            }
        }
        try {
            sinkTask.put(sinkDataEntries);
            try {
                if (!MapUtils.isEmpty(map)) {
                    map.forEach(positionManagementService::putPosition);
                }
            } catch (Exception e) {
                log.error("Source task save position info failed.", e);
            }
        } catch (Exception e) {
            log.error("Send message error, error info: {}.", e);
        }
    }

    private void starkSinkTask() {
        sinkTask.init(taskConfig);
        sinkTask.start(new SinkTaskContext() {

            @Override public String getConnectorName() {
                return taskConfig.getString(RuntimeConfigDefine.CONNECTOR_ID);
            }

            @Override public String getTaskName() {
                return taskConfig.getString(RuntimeConfigDefine.TASK_ID);
            }

            @Override public void resetOffset(RecordPartition recordPartition, RecordOffset recordOffset) {

            }

            @Override public void resetOffset(Map<RecordPartition, RecordOffset> offsets) {

            }

            @Override public void pause(List<RecordPartition> partitions) {

            }

            @Override public void resume(List<RecordPartition> partitions) {

            }

            @Override public Set<RecordPartition> assignment() {
                return null;
            }
        });
        log.info("Sink task start, config:{}", JSON.toJSONString(taskConfig));
    }

    private void stopSinkTask() {
        sinkTask.stop();
        log.info("Sink task stop, config:{}", JSON.toJSONString(taskConfig));
    }

    private void startSourceTask() {
        state.compareAndSet(WorkerTaskState.NEW, WorkerTaskState.PENDING);
        sourceTask.init(taskConfig);
        sourceTask.start(new SourceTaskContext() {
            @Override public OffsetStorageReader offsetStorageReader() {
                return positionStorageReader;
            }

            @Override public String getConnectorName() {
                return null;
            }

            @Override public String getTaskName() {
                return null;
            }
        });
        state.compareAndSet(WorkerTaskState.PENDING, WorkerTaskState.RUNNING);
        log.info("Source task start, config:{}", JSON.toJSONString(taskConfig));
    }

    private void stopSourceTask() {
        sourceTask.stop();
        log.info("Source task stop, config:{}", JSON.toJSONString(taskConfig));
    }

    @Override
    public WorkerTaskState getState() {
        return this.state.get();
    }

    @Override
    public void stop() {
        state.compareAndSet(WorkerTaskState.RUNNING, WorkerTaskState.STOPPING);
    }

    @Override
    public void cleanup() {
        if (state.compareAndSet(WorkerTaskState.STOPPED, WorkerTaskState.TERMINATED) ||
            state.compareAndSet(WorkerTaskState.ERROR, WorkerTaskState.TERMINATED)) {
        } else {
            log.error("[BUG] cleaning a task but it's not in STOPPED or ERROR state");
        }
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
    public Object getJsonObject() {
        HashMap obj = new HashMap<String, Object>();
        obj.put("connectorName", connectorName);
        obj.put("configs", JSON.toJSONString(taskConfig));
        obj.put("state", state.get().toString());
        return obj;
    }

    @Override
    public void timeout() {
        this.state.set(WorkerTaskState.ERROR);
    }
}
