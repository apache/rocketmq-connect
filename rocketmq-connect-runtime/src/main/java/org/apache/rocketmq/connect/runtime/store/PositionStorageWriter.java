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
import org.apache.rocketmq.connect.runtime.service.PositionManagementService;

/**
 * position storage writer
 */
public class PositionStorageWriter {

    private PositionManagementService positionManagementService;
    private final String namespace;

    public PositionStorageWriter(String namespace, PositionManagementService positionManagementService) {
        this.namespace = namespace;
        this.positionManagementService = positionManagementService;
    }

    public void putPosition(RecordPartition partition, RecordOffset position) {
        ExtendRecordPartition extendRecordPartition = new ExtendRecordPartition(namespace, partition.getPartition());
        positionManagementService.putPosition(extendRecordPartition, position);
    }
}
