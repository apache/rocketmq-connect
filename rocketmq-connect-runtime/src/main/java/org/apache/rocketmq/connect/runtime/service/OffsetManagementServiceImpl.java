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

package org.apache.rocketmq.connect.runtime.service;

import io.netty.util.internal.ConcurrentSet;
import io.openmessaging.connector.api.data.RecordOffset;
import io.openmessaging.connector.api.data.RecordPartition;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.connect.runtime.common.LoggerName;
import org.apache.rocketmq.connect.runtime.config.ConnectConfig;
import org.apache.rocketmq.connect.runtime.converter.JsonConverter;
import org.apache.rocketmq.connect.runtime.converter.RecordOffsetConverter;
import org.apache.rocketmq.connect.runtime.converter.RecordPartitionConverter;
import org.apache.rocketmq.connect.runtime.converter.RecordPositionMapConverter;
import org.apache.rocketmq.connect.runtime.store.FileBaseKeyValueStore;
import org.apache.rocketmq.connect.runtime.store.KeyValueStore;
import org.apache.rocketmq.connect.runtime.utils.ConnectUtil;
import org.apache.rocketmq.connect.runtime.utils.FilePathConfigUtil;
import org.apache.rocketmq.connect.runtime.utils.datasync.BrokerBasedLog;
import org.apache.rocketmq.connect.runtime.utils.datasync.DataSynchronizer;
import org.apache.rocketmq.connect.runtime.utils.datasync.DataSynchronizerCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OffsetManagementServiceImpl implements PositionManagementService {
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
     * Synchronize data with other workers.
     */
    private DataSynchronizer<String, Map<RecordPartition, RecordOffset>> dataSynchronizer;

    private final String offsetManagePrefix = "OffsetManage";

    /**
     * Listeners.
     */
    private Set<PositionUpdateListener> offsetUpdateListener;

    public OffsetManagementServiceImpl(ConnectConfig connectConfig) {

        this.offsetStore = new FileBaseKeyValueStore<>(FilePathConfigUtil.getOffsetPath(connectConfig.getStorePathRootDir()),
            new RecordPartitionConverter(),
            new RecordOffsetConverter());
        this.dataSynchronizer = new BrokerBasedLog(connectConfig,
            connectConfig.getOffsetStoreTopic(),
            ConnectUtil.createGroupName(offsetManagePrefix, connectConfig.getWorkerId()),
            new OffsetChangeCallback(),
            new JsonConverter(),
            new RecordPositionMapConverter());
        this.offsetUpdateListener = new HashSet<>();
        this.needSyncPartition = new ConcurrentSet<>();
        this.prepare(connectConfig);
    }

    /**
     * Preparation before startup
     *
     * @param connectConfig
     */
    private void prepare(ConnectConfig connectConfig) {
        String offsetStoreTopic = connectConfig.getOffsetStoreTopic();
        if (!ConnectUtil.isTopicExist(connectConfig, offsetStoreTopic)) {
            log.info("try to create offset store topic: {}!", offsetStoreTopic);
            TopicConfig topicConfig = new TopicConfig(offsetStoreTopic, 1, 1, 6);
            ConnectUtil.createTopic(connectConfig, topicConfig);
        }
    }

    @Override
    public void start() {

        offsetStore.load();
        dataSynchronizer.start();
        sendOnlineOffsetInfo();
    }

    @Override
    public void stop() {

        sendNeedSynchronizeOffset();
        offsetStore.persist();
        dataSynchronizer.stop();
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

        sendNeedSynchronizeOffset();
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

        this.offsetUpdateListener.add(listener);
    }

    private void sendOnlineOffsetInfo() {

        dataSynchronizer.send(OffsetChangeEnum.ONLINE_KEY.name(), offsetStore.getKVMap());
    }


    private void sendNeedSynchronizeOffset() {

        Set<RecordPartition> needSyncPartitionTmp = needSyncPartition;
        needSyncPartition = new ConcurrentSet<>();
        Map<RecordPartition, RecordOffset> needSyncOffset = offsetStore.getKVMap().entrySet().stream()
            .filter(entry -> needSyncPartitionTmp.contains(entry.getKey()))
            .collect(Collectors.toMap(entry -> entry.getKey(), entry -> entry.getValue()));

        dataSynchronizer.send(OffsetChangeEnum.OFFSET_CHANG_KEY.name(), needSyncOffset);
    }

    private void sendSynchronizeOffset() {

        dataSynchronizer.send(OffsetChangeEnum.OFFSET_CHANG_KEY.name(), offsetStore.getKVMap());
    }

    private class OffsetChangeCallback implements DataSynchronizerCallback<String, Map<RecordPartition, RecordOffset>> {

        @Override
        public void onCompletion(Throwable error, String key, Map<RecordPartition, RecordOffset> result) {

            boolean changed = false;
            switch (OffsetChangeEnum.valueOf(key)) {
                case ONLINE_KEY:
                    changed = true;
                    sendSynchronizeOffset();
                    break;
                case OFFSET_CHANG_KEY:
                    changed = mergeOffsetInfo(result);
                    break;
                default:
                    break;
            }
            if (changed) {
                triggerListener();
            }

        }
    }

    private void triggerListener() {
        for (PositionUpdateListener offsetUpdateListener : offsetUpdateListener) {
            offsetUpdateListener.onPositionUpdate();
        }
    }

    /**
     * Merge new received offset info with local store.
     *
     * @param result
     * @return
     */
    private boolean mergeOffsetInfo(Map<RecordPartition, RecordOffset> result) {

        boolean changed = false;
        if (null == result || 0 == result.size()) {
            return changed;
        }

        for (Map.Entry<RecordPartition, RecordOffset> newEntry : result.entrySet()) {
            boolean find = false;
            for (Map.Entry<RecordPartition, RecordOffset> existedEntry : offsetStore.getKVMap().entrySet()) {
                if (newEntry.getKey().equals(existedEntry.getKey())) {
                    find = true;
                    if (!newEntry.getValue().equals(existedEntry.getValue())) {
                        changed = true;
                        existedEntry.setValue(newEntry.getValue());
                    }
                    break;
                }
            }
            if (!find) {
                offsetStore.put(newEntry.getKey(), newEntry.getValue());
            }
        }
        return changed;
    }

    private enum OffsetChangeEnum {

        /**
         * Insert or update offset info.
         */
        OFFSET_CHANG_KEY,

        /**
         * A worker online.
         */
        ONLINE_KEY
    }
}

