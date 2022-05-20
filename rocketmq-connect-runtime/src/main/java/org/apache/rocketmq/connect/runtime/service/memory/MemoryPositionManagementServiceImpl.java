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

package org.apache.rocketmq.connect.runtime.service.memory;

import io.openmessaging.connector.api.data.RecordOffset;
import io.openmessaging.connector.api.data.RecordPartition;
import org.apache.rocketmq.connect.runtime.common.LoggerName;
import org.apache.rocketmq.connect.runtime.config.ConnectConfig;
import org.apache.rocketmq.connect.runtime.converter.RecordOffsetConverter;
import org.apache.rocketmq.connect.runtime.converter.RecordPartitionConverter;
import org.apache.rocketmq.connect.runtime.service.PositionManagementService;
import org.apache.rocketmq.connect.runtime.store.FileBaseKeyValueStore;
import org.apache.rocketmq.connect.runtime.store.KeyValueStore;
import org.apache.rocketmq.connect.runtime.utils.FilePathConfigUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

/**
 * standalone
 */
public class MemoryPositionManagementServiceImpl implements PositionManagementService {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.ROCKETMQ_RUNTIME);

    /**
     * Current position info in store.
     */
    private KeyValueStore<RecordPartition, RecordOffset> positionStore;
    /**
     * Listeners.
     */
    private PositionUpdateListener positionUpdateListener;


    public MemoryPositionManagementServiceImpl(ConnectConfig connectConfig) {
        this.positionStore = new FileBaseKeyValueStore<>(FilePathConfigUtil.getPositionPath(connectConfig.getStorePathRootDir()),
                new RecordPartitionConverter(),
                new RecordOffsetConverter());
    }


    @Override
    public void start() {
        positionStore.load();
    }

    @Override
    public void stop() {
        positionStore.persist();
    }

    @Override
    public void persist() {
        positionStore.persist();
    }

    @Override public void load() {
        positionStore.load();
    }

    @Override
    public void synchronize() { }

    @Override
    public Map<RecordPartition, RecordOffset> getPositionTable() {
        return positionStore.getKVMap();
    }

    @Override
    public RecordOffset getPosition(RecordPartition partition) {
        return positionStore.get(partition);
    }

    @Override
    public void putPosition(Map<RecordPartition, RecordOffset> positions) {
        positionStore.putAll(positions);
    }

    @Override
    public void putPosition(RecordPartition partition, RecordOffset position) {
        positionStore.put(partition, position);
    }

    @Override
    public void removePosition(List<RecordPartition> partitions) {
        if (null == partitions) {
            return;
        }
        for (RecordPartition partition : partitions) {
            positionStore.remove(partition);
        }
    }

    @Override
    public void registerListener(PositionUpdateListener listener) {
        this.positionUpdateListener = listener;
    }

}

