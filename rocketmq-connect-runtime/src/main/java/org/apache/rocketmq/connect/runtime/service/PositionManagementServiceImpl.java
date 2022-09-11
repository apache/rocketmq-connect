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
import io.openmessaging.connector.api.data.RecordConverter;
import io.openmessaging.connector.api.data.RecordOffset;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import io.openmessaging.connector.api.data.SchemaAndValue;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.connect.runtime.common.LoggerName;
import org.apache.rocketmq.connect.runtime.config.WorkerConfig;
import org.apache.rocketmq.connect.runtime.serialization.Serdes;
import org.apache.rocketmq.connect.runtime.serialization.store.RecordOffsetSerde;
import org.apache.rocketmq.connect.runtime.serialization.store.RecordPartitionSerde;
import org.apache.rocketmq.connect.runtime.store.ExtendRecordPartition;
import org.apache.rocketmq.connect.runtime.store.FileBaseKeyValueStore;
import org.apache.rocketmq.connect.runtime.store.KeyValueStore;
import org.apache.rocketmq.connect.runtime.utils.ConnectUtil;
import org.apache.rocketmq.connect.runtime.utils.FilePathConfigUtil;
import org.apache.rocketmq.connect.runtime.utils.datasync.BrokerBasedLog;
import org.apache.rocketmq.connect.runtime.utils.datasync.DataSynchronizer;
import org.apache.rocketmq.connect.runtime.utils.datasync.DataSynchronizerCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.lang.Thread.sleep;

public class PositionManagementServiceImpl implements PositionManagementService {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.ROCKETMQ_RUNTIME);

    /**
     * Current position info in store.
     */
    private KeyValueStore<ExtendRecordPartition, RecordOffset> positionStore;

    private WorkerConfig config;

    private AtomicBoolean committing = new AtomicBoolean(false);
    private long commitStarted;

    /**
     * need sync position
     */
    private Set<ExtendRecordPartition> needSyncPartition;

    /**
     * Synchronize data with other workers.
     */
    private DataSynchronizer<ByteBuffer, ByteBuffer> dataSynchronizer;

    /**
     * Listeners.
     */
    private Set<PositionUpdateListener> positionUpdateListener;

    private final String positionManagePrefix = "PositionManage";

    private RecordConverter keyConverter;
    private RecordConverter valueConverter;

    private String topic;

    public PositionManagementServiceImpl() {
    }

    @Override
    public void initialize(WorkerConfig workerConfig, RecordConverter keyConverter, RecordConverter valueConverter) {
        this.keyConverter = keyConverter;
        this.valueConverter = valueConverter;
        this.topic = workerConfig.getPositionStoreTopic();
        this.keyConverter.configure(new HashMap<>());
        this.valueConverter.configure(new HashMap<>());
        this.dataSynchronizer = new BrokerBasedLog(
                workerConfig,
                this.topic,
                ConnectUtil.createGroupName(positionManagePrefix, workerConfig.getWorkerId()),
                new PositionChangeCallback(),
                Serdes.serdeFrom(ByteBuffer.class),
                Serdes.serdeFrom(ByteBuffer.class)
        );

        this.positionStore = new FileBaseKeyValueStore<>(FilePathConfigUtil.getPositionPath(workerConfig.getStorePathRootDir()),
                new RecordPartitionSerde(),
                new RecordOffsetSerde());

        this.positionUpdateListener = new HashSet<>();
        this.needSyncPartition = new ConcurrentSet<>();
        this.commitStarted = -1;
        this.config = workerConfig;
        this.prepare(workerConfig);
    }

    /**
     * Preparation before startup
     *
     * @param connectConfig
     */
    private void prepare(WorkerConfig connectConfig) {
        String positionStoreTopic = connectConfig.getPositionStoreTopic();
        if (!ConnectUtil.isTopicExist(connectConfig, positionStoreTopic)) {
            log.info("try to create position store topic: {}!", positionStoreTopic);
            TopicConfig topicConfig = new TopicConfig(positionStoreTopic, 1, 1, 6);
            ConnectUtil.createTopic(connectConfig, topicConfig);
        }
    }

    @Override
    public void start() {
        positionStore.load();
        dataSynchronizer.start();
        restorePosition();
    }

    @Override
    public void stop() {
        replicaOffsets();
        positionStore.persist();
        dataSynchronizer.stop();
    }

    @Override
    public void persist() {
        positionStore.persist();
    }

    @Override
    public void load() {
        positionStore.load();
    }

    @Override
    public Map<ExtendRecordPartition, RecordOffset> getPositionTable() {
        return positionStore.getKVMap();
    }

    @Override
    public RecordOffset getPosition(ExtendRecordPartition partition) {
        return positionStore.get(partition);
    }

    @Override
    public void putPosition(Map<ExtendRecordPartition, RecordOffset> positions) {
        positionStore.putAll(positions);
        this.needSyncPartition.addAll(positions.keySet());
    }

    @Override
    public void putPosition(ExtendRecordPartition partition, RecordOffset position) {
        positionStore.put(partition, position);
        // add need sync partition
        this.needSyncPartition.add(partition);
    }

    @Override
    public void synchronize(boolean increment) {
        // Check for timed out commits
        long now = System.currentTimeMillis();
        final long commitTimeoutMs = commitStarted + config.getOffsetCommitTimeoutMsConfig();
        if (committing.get() && now >= commitTimeoutMs) {
            log.warn("{} Commit of offsets timed out", this);
            committing.set(false);
        }

        if (!committing.compareAndSet(false, true)) {
            log.warn("Offset is being committed, ignoring this commit !!");
            return;
        }
        this.commitStarted = System.currentTimeMillis();
        // Full send
        if (!increment) {
            Set<ExtendRecordPartition> allPartitions = new HashSet<>();
            allPartitions.addAll(positionStore.getKVMap().keySet());
            allPartitions.forEach(partition -> set(PositionChange.POSITION_CHANG_KEY, partition, positionStore.get(partition)));
        }
        //Incremental send
        if (increment) {
            if (needSyncPartition.isEmpty()) {
                log.warn("There is no offset to commit");
                return;
            }
            Set<ExtendRecordPartition> partitionsTmp = new HashSet<>(needSyncPartition);
            partitionsTmp.forEach(partition -> set(PositionChange.POSITION_CHANG_KEY, partition, positionStore.get(partition)));
        }
        // end send offset
        if (increment) {
            needSyncPartition.clear();
        }
        committing.compareAndSet(true, false);
    }

    @Override
    public void removePosition(List<ExtendRecordPartition> partitions) {
        if (null == partitions) {
            return;
        }
        for (ExtendRecordPartition partition : partitions) {
            positionStore.remove(partition);
        }
    }

    @Override
    public void registerListener(PositionUpdateListener listener) {
        this.positionUpdateListener.add(listener);
    }

    @Override
    public StagingMode getStagingMode() {
        return StagingMode.DISTRIBUTED;
    }

    /**
     * restore position
     */
    private void restorePosition() {
        set(PositionChange.ONLINE_KEY, new ExtendRecordPartition(null, new HashMap<>()), new RecordOffset(new HashMap<>()));
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

    /**
     * send position
     *
     * @param partition
     * @param position
     */
    private synchronized void set(PositionChange change, ExtendRecordPartition partition, RecordOffset position) {
        String namespace = partition.getNamespace();
        // When serializing the key, we add in the namespace information so the key is [namespace, real key]
        byte[] key = keyConverter.fromConnectData(namespace, null, Arrays.asList(change.name(), namespace, partition != null ? partition.getPartition() : new HashMap<>()));
        ByteBuffer keyBuffer = (key != null) ? ByteBuffer.wrap(key) : null;
        byte[] value = valueConverter.fromConnectData(namespace, null, position != null ? position.getOffset() : new HashMap<>());
        ByteBuffer valueBuffer = (value != null) ? ByteBuffer.wrap(value) : null;
        dataSynchronizer.send(keyBuffer, valueBuffer);
    }

    private class PositionChangeCallback implements DataSynchronizerCallback<ByteBuffer, ByteBuffer> {

        @Override
        public void onCompletion(Throwable error, ByteBuffer key, ByteBuffer result) {
            if (key == null) {
                log.warn("The received position information key is empty and cannot be parsed. the message will be skipped");
                return;
            }
            SchemaAndValue schemaAndValueKey = keyConverter.toConnectData(topic, key.array());
            if (schemaAndValueKey.value() == null || schemaAndValueKey.value() == null) {
                log.error("The format of the monitored offset change data is wrong and will be discarded , schema and value {}", schemaAndValueKey.toString());
                return;
            }
            List<Object> deKey = (List<Object>) schemaAndValueKey.value();
            if (deKey.isEmpty() || deKey.size() != 3) {
                log.error("The format of the monitored offset change data is wrong and will be discarded , message {}", deKey);
                return;
            }
            String changeKey = (String) deKey.get(0);
            boolean changed = false;
            switch (PositionChange.valueOf(changeKey)) {
                case ONLINE_KEY:
                    changed = true;
                    replicaOffsets();
                    break;
                case POSITION_CHANG_KEY:
                    // partition
                    String namespace = (String) deKey.get(1);
                    Map<String, Object> partitions = (Map<String, Object>) deKey.get(2);
                    ExtendRecordPartition partition = new ExtendRecordPartition(namespace, partitions);
                    // offset
                    SchemaAndValue schemaAndValueValue = valueConverter.toConnectData(topic, result.array());
                    Map<String, Object> offset = (Map<String, Object>) schemaAndValueValue.value();
                    changed = mergeOffset(partition, new RecordOffset(offset));
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
        for (PositionUpdateListener positionUpdateListener : positionUpdateListener) {
            positionUpdateListener.onPositionUpdate();
        }
    }

    /**
     * Merge new received position info with local store.
     *
     * @param partition
     * @param offset
     * @return
     */
    private boolean mergeOffset(ExtendRecordPartition partition, RecordOffset offset) {
        if (null == partition || partition.getPartition().isEmpty()) {
            return false;
        }
        if (positionStore.getKVMap().containsKey(partition)) {
            RecordOffset existedOffset = positionStore.getKVMap().get(partition);
            // update
            if (!offset.equals(existedOffset)) {
                positionStore.put(partition, offset);
                return true;
            }
        } else {
            // add new position
            positionStore.put(partition, offset);
            return true;
        }
        return false;
    }

    private enum PositionChange {

        /**
         * Insert or update position info.
         */
        POSITION_CHANG_KEY,

        /**
         * A worker online.
         */
        ONLINE_KEY
    }
}