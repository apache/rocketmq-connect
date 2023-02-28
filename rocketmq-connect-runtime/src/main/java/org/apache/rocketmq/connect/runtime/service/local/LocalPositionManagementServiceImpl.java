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

package org.apache.rocketmq.connect.runtime.service.local;

import io.openmessaging.connector.api.data.RecordConverter;
import io.openmessaging.connector.api.data.RecordOffset;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;

import org.apache.rocketmq.connect.runtime.config.WorkerConfig;
import org.apache.rocketmq.connect.runtime.serialization.Serdes;
import org.apache.rocketmq.connect.runtime.serialization.store.RecordOffsetSerde;
import org.apache.rocketmq.connect.runtime.serialization.store.RecordPartitionSerde;
import org.apache.rocketmq.connect.runtime.service.AbstractPositionManagementService;
import org.apache.rocketmq.connect.runtime.store.ExtendRecordPartition;
import org.apache.rocketmq.connect.runtime.store.FileBaseKeyValueStore;
import org.apache.rocketmq.connect.runtime.utils.ConnectUtil;
import org.apache.rocketmq.connect.runtime.utils.FilePathConfigUtil;
import org.apache.rocketmq.connect.runtime.utils.datasync.BrokerBasedLog;
import org.apache.rocketmq.connect.runtime.utils.datasync.DataSynchronizer;

import static java.lang.Thread.sleep;

/**
 * local position management service impl
 */
public class LocalPositionManagementServiceImpl extends AbstractPositionManagementService {

    public LocalPositionManagementServiceImpl() {
    }

    @Override
    public void initialize(WorkerConfig workerConfig, RecordConverter keyConverter, RecordConverter valueConverter) {

        super.initialize(workerConfig, keyConverter, valueConverter);
        this.positionStore = new FileBaseKeyValueStore<>(FilePathConfigUtil.getPositionPath(workerConfig.getStorePathRootDir()),
            new RecordPartitionSerde(),
            new RecordOffsetSerde());
    }

    @Override
    public DataSynchronizer initializationDataSynchronizer(WorkerConfig workerConfig) {
        return new BrokerBasedLog(
            workerConfig,
            super.topic,
            ConnectUtil.createGroupName(super.positionManagePrefix, workerConfig.getWorkerId()),
            new PositionChangeCallback(),
            Serdes.serdeFrom(ByteBuffer.class),
            Serdes.serdeFrom(ByteBuffer.class),
            enabledCompactTopic()
        );
    }

    @Override
    public void start() {
        positionStore.load();
        dataSynchronizer.start();
        restorePosition();
    }

    /**
     * restore position
     */
    protected void restorePosition() {
        set(PositionChange.ONLINE, new ExtendRecordPartition(null, new HashMap<>()), new RecordOffset(new HashMap<>()));
    }

    @Override
    public void stop() {
        replicaOffsets();
        positionStore.persist();
        dataSynchronizer.stop();
    }

    protected void process(ByteBuffer result, List<Object> deKey, PositionChange key) {
        switch (key) {
            case ONLINE:
                replicaOffsets();
                break;
            case POSITION_CHANG:
                processPositionChange(result, deKey);
                break;
            default:
                break;
        }
    }

    /**
     * send change position
     */
    private void replicaOffsets() {
        while (true) {
            // wait for the last send to complete
            if (committing.get()) {
                try {
                    sleep(1000);
                    continue;
                } catch (InterruptedException e) {
                }
            }
            synchronize(false);
            break;
        }
    }
}