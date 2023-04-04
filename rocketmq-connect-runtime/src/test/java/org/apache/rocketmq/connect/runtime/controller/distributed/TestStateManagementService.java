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
 */

package org.apache.rocketmq.connect.runtime.controller.distributed;

import io.openmessaging.connector.api.data.RecordConverter;
import org.apache.rocketmq.connect.runtime.common.ConnectKeyValue;
import org.apache.rocketmq.connect.runtime.config.WorkerConfig;
import org.apache.rocketmq.connect.runtime.connectorwrapper.status.ConnectorStatus;
import org.apache.rocketmq.connect.runtime.connectorwrapper.status.TaskStatus;
import org.apache.rocketmq.connect.runtime.service.StateManagementService;
import org.apache.rocketmq.connect.runtime.utils.ConnectorTaskId;

import java.util.Collection;
import java.util.Set;

public class TestStateManagementService implements StateManagementService {
    @Override
    public void initialize(WorkerConfig workerConfig, RecordConverter converter) {

    }

    @Override
    public void start() {

    }

    @Override
    public void stop() {

    }

    @Override
    public void put(ConnectorStatus status) {

    }

    @Override
    public void putSafe(ConnectorStatus status) {

    }

    @Override
    public void put(TaskStatus status) {

    }

    @Override
    public void putSafe(TaskStatus status) {

    }

    @Override
    public TaskStatus get(ConnectorTaskId id) {
        return null;
    }

    @Override
    public ConnectorStatus get(String connector) {
        return null;
    }

    @Override
    public Collection<TaskStatus> getAll(String connector) {
        return null;
    }

    @Override
    public Set<String> connectors() {
        return null;
    }

}
