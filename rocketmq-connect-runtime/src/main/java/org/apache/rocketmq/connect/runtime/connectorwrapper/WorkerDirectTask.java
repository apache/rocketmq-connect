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
import io.openmessaging.connector.api.component.task.source.SourceTask;
import io.openmessaging.connector.api.component.task.source.SourceTaskContext;
import io.openmessaging.connector.api.data.ConnectRecord;
import io.openmessaging.connector.api.data.RecordOffset;
import io.openmessaging.connector.api.data.RecordPartition;
import io.openmessaging.connector.api.storage.OffsetStorageReader;
import org.apache.rocketmq.connect.runtime.common.ConnectKeyValue;
import org.apache.rocketmq.connect.runtime.common.LoggerName;
import org.apache.rocketmq.connect.runtime.config.WorkerConfig;
import org.apache.rocketmq.connect.runtime.connectorwrapper.status.WrapperStatusListener;
import org.apache.rocketmq.connect.runtime.errors.RetryWithToleranceOperator;
import org.apache.rocketmq.connect.runtime.metrics.ConnectMetrics;
import org.apache.rocketmq.connect.runtime.service.PositionManagementService;
import org.apache.rocketmq.connect.runtime.stats.ConnectStatsManager;
import org.apache.rocketmq.connect.runtime.stats.ConnectStatsService;
import org.apache.rocketmq.connect.runtime.store.PositionStorageReaderImpl;
import org.apache.rocketmq.connect.runtime.utils.ConnectorTaskId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

/**
 * A wrapper of {@link SinkTask} and {@link SourceTask} for runtime.
 */
public class WorkerDirectTask extends WorkerSourceTask {

    private static final Logger log = LoggerFactory.getLogger(LoggerName.ROCKETMQ_RUNTIME);
    private final OffsetStorageReader positionStorageReader;
    /**
     * The implements of the source task.
     */
    private SourceTask sourceTask;
    /**
     * The implements of the sink task.
     */
    private SinkTask sinkTask;

    public WorkerDirectTask(WorkerConfig workerConfig,
                            ConnectorTaskId id,
                            SourceTask sourceTask,
                            ClassLoader classLoader,
                            SinkTask sinkTask,
                            ConnectKeyValue taskConfig,
                            PositionManagementService positionManagementService,
                            AtomicReference<WorkerState> workerState,
                            ConnectStatsManager connectStatsManager,
                            ConnectStatsService connectStatsService,
                            TransformChain<ConnectRecord> transformChain,
                            RetryWithToleranceOperator retryWithToleranceOperator,
                            WrapperStatusListener statusListener,
                            ConnectMetrics connectMetrics) {
        super(workerConfig,
                id,
                sourceTask,
                classLoader,
                taskConfig,
                positionManagementService,
                null,
                null,
                null,
                workerState,
                connectStatsManager,
                connectStatsService,
                transformChain,
                retryWithToleranceOperator,
                statusListener,
                connectMetrics
        );
        this.sourceTask = sourceTask;
        this.sinkTask = sinkTask;
        this.positionStorageReader = new PositionStorageReaderImpl(id.connector(), positionManagementService);
    }


    private void sendRecord(Collection<ConnectRecord> sourceDataEntries) {
        List<ConnectRecord> records = new ArrayList<>(sourceDataEntries.size());
        List<RecordOffsetManagement.SubmittedPosition> positions = new ArrayList<>();
        for (ConnectRecord preTransformRecord : sourceDataEntries) {

            retryWithToleranceOperator.sourceRecord(preTransformRecord);
            ConnectRecord record = transformChain.doTransforms(preTransformRecord);
            if (record == null) {
                continue;
            }

            records.add(record);
            /**prepare to send record*/
            positions.add(prepareToSendRecord(preTransformRecord).get());

        }
        try {
            sinkTask.put(records);
            // ack
            positions.forEach(submittedPosition -> {
                submittedPosition.ack();
            });
        } catch (Exception e) {
            // drop commit
            positions.forEach(submittedPosition -> {
                submittedPosition.remove();
            });
            log.error("Send message error, error info: {}.", e);
        }
    }

    private void starkSinkTask() {
        sinkTask.init(new DirectSinkTaskContext());
        sinkTask.start(taskConfig);
        log.info("Sink task start, config:{}", JSON.toJSONString(taskConfig));
    }

    private void stopSinkTask() {
        sinkTask.stop();
        log.info("Sink task stop, config:{}", JSON.toJSONString(taskConfig));
    }

    private void startSourceTask() {
        sourceTask.init(new DirectSourceTaskContext());
        sourceTask.start(taskConfig);
        log.info("Source task start, config:{}", JSON.toJSONString(taskConfig));
    }

    private void stopSourceTask() {
        sourceTask.stop();
        log.info("Source task stop, config:{}", JSON.toJSONString(taskConfig));
    }

    /**
     * initinalize and start
     */
    @Override
    protected void initializeAndStart() {
        starkSinkTask();
        startSourceTask();
        log.info("Direct task start, config:{}", JSON.toJSONString(taskConfig));
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

            try {
                Collection<ConnectRecord> toSendEntries = sourceTask.poll();
                if (!toSendEntries.isEmpty()) {
                    sendRecord(toSendEntries);
                }
            } catch (Exception e) {
                log.error("Direct task runtime exception", e);
                finalOffsetCommit(true);
                onFailure(e);
            }
        }
    }

    /**
     * close resources
     */
    @Override
    public void close() {
        stopSourceTask();
        stopSinkTask();
    }


    /**
     * direct sink task context
     */
    private class DirectSinkTaskContext implements SinkTaskContext {

        /**
         * Get the Connector Name
         *
         * @return connector name
         */
        @Override
        public String getConnectorName() {
            return id().connector();
        }

        /**
         * Get the Task Name of connector.
         *
         * @return task name
         */
        @Override
        public String getTaskName() {
            return id().task() + "";
        }

        /**
         * Get the configurations of current task.
         *
         * @return the configuration of current task.
         */
        @Override
        public KeyValue configs() {
            return taskConfig;
        }

        /**
         * Reset the consumer offset for the given queue.
         *
         * @param recordPartition the partition to reset offset.
         * @param recordOffset    the offset to reset to.
         */
        @Override
        public void resetOffset(RecordPartition recordPartition, RecordOffset recordOffset) {
            // no-op
        }

        /**
         * Reset the offsets for the given partition.
         *
         * @param offsets the map of offsets for targetPartition.
         */
        @Override
        public void resetOffset(Map<RecordPartition, RecordOffset> offsets) {
            // no-op
        }

        /**
         * Pause consumption of messages from the specified partition.
         *
         * @param partitions the partition list to be reset offset.
         */
        @Override
        public void pause(List<RecordPartition> partitions) {
            // no-op
        }

        /**
         * Resume consumption of messages from previously paused Partition.
         *
         * @param partitions the partition list to be resume.
         */
        @Override
        public void resume(List<RecordPartition> partitions) {
            // no-op
        }

        /**
         * Current task assignment processing partition
         *
         * @return the partition list
         */
        @Override
        public Set<RecordPartition> assignment() {
            return null;
        }
    }


    private class DirectSourceTaskContext implements SourceTaskContext {

        /**
         * Get the OffsetStorageReader for this SourceTask.
         *
         * @return offset storage reader
         */
        @Override
        public OffsetStorageReader offsetStorageReader() {
            return positionStorageReader;
        }

        /**
         * Get the Connector Name
         *
         * @return connector name
         */
        @Override
        public String getConnectorName() {
            return id().connector();
        }

        /**
         * Get the Task Id of connector.
         *
         * @return task name
         */
        @Override
        public String getTaskName() {
            return id().task() + "";
        }

        /**
         * Get the configurations of current task.
         *
         * @return the configuration of current task.
         */
        @Override
        public KeyValue configs() {
            return taskConfig;
        }
    }

}
