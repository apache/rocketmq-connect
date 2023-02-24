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

package org.apache.rocketmq.connect.runtime.service.rocketmq;

import io.openmessaging.connector.api.data.RecordConverter;
import org.apache.rocketmq.connect.runtime.config.WorkerConfig;
import org.apache.rocketmq.connect.runtime.serialization.Serdes;
import org.apache.rocketmq.connect.runtime.service.AbstractPositionManagementService;
import org.apache.rocketmq.connect.runtime.store.MemoryBasedKeyValueStore;
import org.apache.rocketmq.connect.runtime.utils.ConnectUtil;
import org.apache.rocketmq.connect.runtime.utils.datasync.BrokerBasedLog;
import org.apache.rocketmq.connect.runtime.utils.datasync.DataSynchronizer;

import java.nio.ByteBuffer;

/**
 * Rocketmq position management service impl
 */
public class RocketMqPositionManagementServiceImpl extends AbstractPositionManagementService {

    public RocketMqPositionManagementServiceImpl() {
    }

    @Override
    public void initialize(WorkerConfig workerConfig, RecordConverter keyConverter, RecordConverter valueConverter) {
        super.initialize(workerConfig, keyConverter, valueConverter);
        this.positionStore = new MemoryBasedKeyValueStore<>();
    }

    @Override
    public DataSynchronizer initializationDataSynchronizer(WorkerConfig workerConfig) {
        return new BrokerBasedLog(
            workerConfig,
            this.topic,
            ConnectUtil.createGroupName(positionManagePrefix, workerConfig.getWorkerId()),
            new PositionChangeCallback(),
            Serdes.serdeFrom(ByteBuffer.class),
            Serdes.serdeFrom(ByteBuffer.class),
            enabledCompactTopic()
        );
    }

    @Override
    public boolean enabledCompactTopic() {
        return Boolean.TRUE;
    }

    @Override
    public void start() {
        dataSynchronizer.start();
    }

    @Override
    public void stop() {
        if (dataSynchronizer != null) {
            dataSynchronizer.stop();
        }
    }

    @Override
    public void persist() {
        // No-op
    }

    @Override
    public void load() {
        // No-op
    }
}