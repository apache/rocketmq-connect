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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.apache.rocketmq.connect.runtime.store;

import io.openmessaging.connector.api.data.RecordOffset;
import io.openmessaging.connector.api.data.RecordPartition;
import io.openmessaging.connector.api.storage.OffsetStorageReader;
import org.apache.rocketmq.connect.runtime.service.PositionManagementService;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class PositionStorageReaderImpl implements OffsetStorageReader {

    private final String namespace;
    private PositionManagementService positionManagementService;

    public PositionStorageReaderImpl(String namespace, PositionManagementService positionManagementService) {
        this.namespace = namespace;
        this.positionManagementService = positionManagementService;
    }

    @Override
    public RecordOffset readOffset(RecordPartition partition) {
        ExtendRecordPartition extendRecordPartition = new ExtendRecordPartition(namespace, partition.getPartition());
        return positionManagementService.getPositionTable().get(extendRecordPartition);
    }

    @Override
    public Map<RecordPartition, RecordOffset> readOffsets(Collection<RecordPartition> partitions) {
        Map<RecordPartition, RecordOffset> result = new HashMap<>();
        Map<ExtendRecordPartition, RecordOffset> allData = positionManagementService.getPositionTable();
        for (RecordPartition key : partitions) {
            ExtendRecordPartition extendRecordPartition = new ExtendRecordPartition(namespace, key.getPartition());
            if (allData.containsKey(extendRecordPartition)) {
                result.put(key, allData.get(extendRecordPartition));
            }
        }
        return result;
    }
}
