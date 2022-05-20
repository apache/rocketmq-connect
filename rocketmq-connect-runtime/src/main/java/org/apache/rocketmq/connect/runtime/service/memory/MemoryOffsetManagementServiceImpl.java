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

import io.netty.util.internal.ConcurrentSet;
import io.openmessaging.connector.api.data.RecordOffset;
import io.openmessaging.connector.api.data.RecordPartition;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.connect.runtime.common.LoggerName;
import org.apache.rocketmq.connect.runtime.config.ConnectConfig;
import org.apache.rocketmq.connect.runtime.converter.JsonConverter;
import org.apache.rocketmq.connect.runtime.converter.RecordOffsetConverter;
import org.apache.rocketmq.connect.runtime.converter.RecordPartitionConverter;
import org.apache.rocketmq.connect.runtime.converter.RecordPositionMapConverter;
import org.apache.rocketmq.connect.runtime.service.OffsetManagementServiceImpl;
import org.apache.rocketmq.connect.runtime.service.PositionManagementService;
import org.apache.rocketmq.connect.runtime.store.FileBaseKeyValueStore;
import org.apache.rocketmq.connect.runtime.store.KeyValueStore;
import org.apache.rocketmq.connect.runtime.utils.ConnectUtil;
import org.apache.rocketmq.connect.runtime.utils.FilePathConfigUtil;
import org.apache.rocketmq.connect.runtime.utils.datasync.BrokerBasedLog;
import org.apache.rocketmq.connect.runtime.utils.datasync.DataSynchronizer;
import org.apache.rocketmq.connect.runtime.utils.datasync.DataSynchronizerCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * memory offset management service impl
 */
public class MemoryOffsetManagementServiceImpl implements PositionManagementService {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.ROCKETMQ_RUNTIME);

    /**
     * Current offset info in store.
     */
    private KeyValueStore<RecordPartition, RecordOffset> offsetStore;


    /**
     * The updated partition of the task in the current instance.
     */
    private Set<RecordPartition> needSyncPartition;

    /**
     * Listeners.
     */
    private PositionUpdateListener offsetUpdateListener;

    public MemoryOffsetManagementServiceImpl(ConnectConfig connectConfig) {
        this.offsetStore = new FileBaseKeyValueStore<>(
                FilePathConfigUtil.getOffsetPath(connectConfig.getStorePathRootDir()),
                new RecordPartitionConverter(),
                new RecordOffsetConverter());
        this.needSyncPartition = new ConcurrentSet<>();
    }

    @Override
    public void start() {
        offsetStore.load();
    }

    @Override
    public void stop() {
        offsetStore.persist();
    }

    @Override
    public void persist() {
        offsetStore.persist();
    }

    @Override public void load() {
        offsetStore.load();
    }

    @Override
    public void synchronize() {
    }

    @Override
    public Map<RecordPartition, RecordOffset> getPositionTable() {
        return offsetStore.getKVMap();
    }

    @Override
    public RecordOffset getPosition(RecordPartition partition) {
        return offsetStore.get(partition);
    }

    @Override
    public void putPosition(Map<RecordPartition, RecordOffset> offsets) {
        offsetStore.putAll(offsets);
        needSyncPartition.addAll(offsets.keySet());
    }

    @Override
    public void putPosition(RecordPartition partition, RecordOffset position) {
        offsetStore.put(partition, position);
        needSyncPartition.add(partition);
    }

    @Override
    public void removePosition(List<RecordPartition> offsets) {
        if (null == offsets) {
            return;
        }
        for (RecordPartition offset : offsets) {
            needSyncPartition.remove(offset);
            offsetStore.remove(offset);
        }
    }

    @Override
    public void registerListener(PositionUpdateListener listener) {
        this.offsetUpdateListener = listener;
    }


    private void triggerListener() {
        offsetUpdateListener.onPositionUpdate();
    }

}

