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
import io.openmessaging.connector.api.data.SchemaAndValue;
import org.apache.rocketmq.connect.runtime.common.LoggerName;
import org.apache.rocketmq.connect.runtime.config.WorkerConfig;
import org.apache.rocketmq.connect.runtime.store.ExtendRecordPartition;
import org.apache.rocketmq.connect.runtime.store.KeyValueStore;
import org.apache.rocketmq.connect.runtime.utils.datasync.DataSynchronizer;
import org.apache.rocketmq.connect.runtime.utils.datasync.DataSynchronizerCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Abstract position management service
 */
public abstract class AbstractPositionManagementService implements PositionManagementService, IChangeNotifier<ByteBuffer, ByteBuffer>, ICommonConfiguration {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.ROCKETMQ_RUNTIME);
    protected final String positionManagePrefix = "PositionManage";
    /**
     * Current position info in store.
     */
    protected KeyValueStore<ExtendRecordPartition, RecordOffset> positionStore;
    /**
     * Synchronize data with other workers.
     */
    protected DataSynchronizer<ByteBuffer, ByteBuffer> dataSynchronizer;
    protected AtomicBoolean committing = new AtomicBoolean(false);
    protected WorkerConfig config;
    protected boolean enabledCompactTopic;
    private long commitStarted;
    /**
     * need sync position
     */
    private Set<ExtendRecordPartition> needSyncPartition;
    private RecordConverter keyConverter;
    private RecordConverter valueConverter;
    protected String topic;

    public AbstractPositionManagementService() {
    }

    @Override
    public void initialize(WorkerConfig workerConfig, RecordConverter keyConverter, RecordConverter valueConverter) {
        this.keyConverter = keyConverter;
        this.valueConverter = valueConverter;
        this.topic = workerConfig.getPositionStoreTopic();
        this.keyConverter.configure(new HashMap<>());
        this.valueConverter.configure(new HashMap<>());
        this.needSyncPartition = new ConcurrentSet<>();
        this.commitStarted = -1;
        this.config = workerConfig;
        this.dataSynchronizer = initializationDataSynchronizer(workerConfig);
    }

    @Override
    public boolean enabledCompactTopic() {
        return false;
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
    public void removePosition(List<ExtendRecordPartition> partitions) {
        if (null == partitions) {
            return;
        }
        for (ExtendRecordPartition partition : partitions) {
            positionStore.remove(partition);
        }
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
            allPartitions.forEach(partition -> set(PositionChange.POSITION_CHANG, partition, positionStore.get(partition)));
        }
        //Incremental send
        if (increment) {
            if (needSyncPartition.isEmpty()) {
                log.warn("There is no offset to commit");
                return;
            }
            Set<ExtendRecordPartition> partitionsTmp = new HashSet<>(needSyncPartition);
            partitionsTmp.forEach(partition -> set(PositionChange.POSITION_CHANG, partition, positionStore.get(partition)));
        }
        // end send offset
        if (increment) {
            needSyncPartition.clear();
        }
        committing.compareAndSet(true, false);
    }

    /**
     * send position
     *
     * @param partition
     * @param position
     */
    protected synchronized void set(PositionChange change, ExtendRecordPartition partition, RecordOffset position) {
        String namespace = partition.getNamespace();
        // When serializing the key, we add in the namespace information so the key is [namespace, real key]
        byte[] key = keyConverter.fromConnectData(namespace, null, Arrays.asList(change.name(), namespace, partition != null ? partition.getPartition() : new HashMap<>()));
        ByteBuffer keyBuffer = (key != null) ? ByteBuffer.wrap(key) : null;
        byte[] value = valueConverter.fromConnectData(namespace, null, position != null ? position.getOffset() : new HashMap<>());
        ByteBuffer valueBuffer = (value != null) ? ByteBuffer.wrap(value) : null;
        notify(keyBuffer, valueBuffer);
    }

    @Override
    public void notify(ByteBuffer key, ByteBuffer values) {
        dataSynchronizer.send(key, values);
    }

    protected void process(ByteBuffer result, List<Object> deKey, PositionChange key) {
        switch (key) {
            case POSITION_CHANG:
                processPositionChange(result, deKey);
                break;
            default:
                log.warn("Discarding position update record with invalid key: {}", key);
                break;
        }
    }

    protected void processPositionChange(ByteBuffer result, List<Object> deKey) {
        // partition
        String namespace = (String) deKey.get(1);
        Map<String, Object> partitions = (Map<String, Object>) deKey.get(2);
        ExtendRecordPartition partition = new ExtendRecordPartition(namespace, partitions);
        // offset
        SchemaAndValue schemaAndValueValue = valueConverter.toConnectData(topic, result.array());
        Map<String, Object> offset = (Map<String, Object>) schemaAndValueValue.value();
        mergeOffset(partition, new RecordOffset(offset));
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

    public enum PositionChange {

        /**
         * Insert or update position info.
         */
        POSITION_CHANG,

        /**
         * A worker online.
         */
        ONLINE
    }

    public class PositionChangeCallback implements DataSynchronizerCallback<ByteBuffer, ByteBuffer> {

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
            process(result, deKey, PositionChange.valueOf(changeKey));
        }
    }
}