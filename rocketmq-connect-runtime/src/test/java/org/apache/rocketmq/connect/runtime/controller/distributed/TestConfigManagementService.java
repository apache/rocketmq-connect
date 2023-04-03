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

import java.util.List;
import java.util.Map;

import io.openmessaging.connector.api.data.RecordConverter;
import org.apache.rocketmq.connect.runtime.common.ConnectKeyValue;
import org.apache.rocketmq.connect.runtime.config.WorkerConfig;
import org.apache.rocketmq.connect.runtime.controller.isolation.Plugin;
import org.apache.rocketmq.connect.runtime.service.ConfigManagementService;
import org.apache.rocketmq.connect.runtime.store.ClusterConfigState;

public class TestConfigManagementService implements ConfigManagementService {
    @Override
    public void start() {

    }

    @Override
    public void stop() {

    }

    @Override
    public Map<String, ConnectKeyValue> getConnectorConfigs() {
        return null;
    }


    @Override
    public String putConnectorConfig(String connectorName, ConnectKeyValue configs) {
        return null;
    }

    @Override
    public void deleteConnectorConfig(String connectorName) {

    }

    @Override
    public void pauseConnector(String connectorName) {
    }

    @Override
    public void resumeConnector(String connectorName) {

    }


    @Override
    public void recomputeTaskConfigs(String connectorName,
                                     ConnectKeyValue configs) {

    }

    @Override
    public Map<String, List<ConnectKeyValue>> getTaskConfigs() {
        return null;
    }

    @Override
    public void persist() {

    }

    @Override
    public void registerListener(ConnectorConfigUpdateListener listener) {

    }


    @Override public void initialize(WorkerConfig connectConfig, RecordConverter converter, Plugin plugin) {

    }

    @Override
    public ClusterConfigState snapshot() {
        return null;
    }

    @Override
    public Plugin getPlugin() {
        return null;
    }

}
