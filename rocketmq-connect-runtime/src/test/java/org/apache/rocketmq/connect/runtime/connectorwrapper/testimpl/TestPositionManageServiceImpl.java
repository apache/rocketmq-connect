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

package org.apache.rocketmq.connect.runtime.connectorwrapper.testimpl;

import io.openmessaging.connector.api.data.RecordConverter;
import io.openmessaging.connector.api.data.RecordOffset;
import org.apache.rocketmq.connect.runtime.config.WorkerConfig;
import org.apache.rocketmq.connect.runtime.service.PositionManagementService;
import org.apache.rocketmq.connect.runtime.store.ExtendRecordPartition;

import java.util.List;
import java.util.Map;

public class TestPositionManageServiceImpl implements PositionManagementService {

    @Override
    public void start() {

    }

    @Override
    public void stop() {

    }

    @Override
    public void persist() {

    }

    @Override
    public void load() {

    }

    @Override
    public void synchronize(boolean increment) {

    }

    @Override
    public Map<ExtendRecordPartition, RecordOffset> getPositionTable() {
        return null;
    }

    @Override
    public RecordOffset getPosition(ExtendRecordPartition partition) {
        return null;
    }

    @Override
    public void putPosition(Map<ExtendRecordPartition, RecordOffset> positions) {

    }

    @Override
    public void putPosition(ExtendRecordPartition partition, RecordOffset position) {

    }

    @Override
    public void removePosition(List<ExtendRecordPartition> partitions) {

    }

    @Override
    public void registerListener(PositionUpdateListener listener) {

    }


    @Override public void initialize(WorkerConfig connectConfig, RecordConverter keyConverter, RecordConverter valueConverter) {

    }
}
