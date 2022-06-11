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

package org.apache.rocketmq.connect.kafka.connect.adaptor.context;

import io.openmessaging.connector.api.component.task.source.SourceTaskContext;
import io.openmessaging.connector.api.data.RecordOffset;
import io.openmessaging.connector.api.data.RecordPartition;
import org.apache.kafka.connect.storage.CloseableOffsetStorageReader;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * kafka offset storage reader
 */
public class KafkaOffsetStorageReader implements CloseableOffsetStorageReader {
    SourceTaskContext context;

    public KafkaOffsetStorageReader(SourceTaskContext context) {
        this.context = context;
    }

    @Override
    public void close() {
        this.context = null;
    }

    @Override
    public <T> Map<String, Object> offset(Map<String, T> map) {
        RecordPartition partition = new RecordPartition(map);
        RecordOffset offset = context.offsetStorageReader().readOffset(partition);
        return (Map<String, Object>) offset.getOffset();
    }

    @Override
    public <T> Map<Map<String, T>, Map<String, Object>> offsets(Collection<Map<String, T>> collection) {
        List<RecordPartition> partitions = new ArrayList<>();
        collection.forEach(partitionMap -> {
            RecordPartition partition = new RecordPartition(partitionMap);
            partitions.add(partition);
        });
        Map<Map<String, T>, Map<String, Object>> offsetMap = new ConcurrentHashMap<>();
        Map<RecordPartition, RecordOffset> offsets = context.offsetStorageReader().readOffsets(partitions);
        offsets.forEach((partition, offset) -> {
            Map<String, T> mapPartition = (Map<String, T>) partition.getPartition();
            Map<String, Object> mapOffset = (Map<String, Object>) offset.getOffset();
            offsetMap.put(mapPartition, mapOffset);
        });
        return offsetMap;
    }
}
