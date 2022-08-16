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

import io.openmessaging.KeyValue;
import io.openmessaging.connector.api.component.connector.Connector;
import org.apache.rocketmq.connect.runtime.common.ConnectKeyValue;
import org.apache.rocketmq.connect.runtime.config.RuntimeConfigDefine;
import org.apache.rocketmq.connect.runtime.config.SinkConnectorConfig;
import org.apache.rocketmq.connect.runtime.config.SourceConnectorConfig;
import org.apache.rocketmq.connect.runtime.connectorwrapper.TargetState;
import org.apache.rocketmq.connect.runtime.connectorwrapper.Worker;
import org.apache.rocketmq.connect.runtime.controller.isolation.Plugin;
import org.apache.rocketmq.connect.runtime.store.ClusterConfigState;
import org.apache.rocketmq.connect.runtime.store.KeyValueStore;
import org.apache.rocketmq.connect.runtime.utils.ConnectorTaskId;
import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Interface for config manager. Contains connector configs and task configs. All worker in a cluster should keep the
 * same configs.
 */
public abstract class AbstractConfigManagementService implements ConfigManagementService {

    protected Plugin plugin;

    /**
     * Current task configs in the store.
     */
    protected KeyValueStore<String, List<ConnectKeyValue>> taskKeyValueStore;

    /**
     * Current connector configs in the store.
     */
    protected KeyValueStore<String, ConnectKeyValue> connectorKeyValueStore;


    @Override
    public void recomputeTaskConfigs(String connectorName, Connector connector, Long currentTimestamp, ConnectKeyValue configs) {
        int maxTask = configs.getInt(RuntimeConfigDefine.MAX_TASK, RuntimeConfigDefine.TASKS_MAX_DEFAULT);
        ConnectKeyValue connectConfig = connectorKeyValueStore.get(connectorName);
        boolean directEnable = Boolean.parseBoolean(connectConfig.getString(RuntimeConfigDefine.CONNECTOR_DIRECT_ENABLE));
        List<KeyValue> taskConfigs = connector.taskConfigs(maxTask);
        List<ConnectKeyValue> converterdConfigs = new ArrayList<>();
        int taskId = 0;
        for (KeyValue keyValue : taskConfigs) {
            ConnectKeyValue newKeyValue = new ConnectKeyValue();
            for (String key : keyValue.keySet()) {
                newKeyValue.put(key, keyValue.getString(key));
            }
            if (directEnable) {
                newKeyValue.put(RuntimeConfigDefine.TASK_TYPE, Worker.TaskType.DIRECT.name());
                newKeyValue.put(RuntimeConfigDefine.SOURCE_TASK_CLASS, connectConfig.getString(RuntimeConfigDefine.SOURCE_TASK_CLASS));
                newKeyValue.put(RuntimeConfigDefine.SINK_TASK_CLASS, connectConfig.getString(RuntimeConfigDefine.SINK_TASK_CLASS));
            }
            // put task id
            newKeyValue.put(RuntimeConfigDefine.TASK_ID, taskId);
            newKeyValue.put(RuntimeConfigDefine.TASK_CLASS, connector.taskClass().getName());
            newKeyValue.put(RuntimeConfigDefine.UPDATE_TIMESTAMP, currentTimestamp);

            newKeyValue.put(SourceConnectorConfig.CONNECT_TOPICNAME, configs.getString(SourceConnectorConfig.CONNECT_TOPICNAME));
            newKeyValue.put(SinkConnectorConfig.CONNECT_TOPICNAMES, configs.getString(SinkConnectorConfig.CONNECT_TOPICNAMES));
            Set<String> connectConfigKeySet = configs.keySet();
            for (String connectConfigKey : connectConfigKeySet) {
                if (connectConfigKey.startsWith(RuntimeConfigDefine.TRANSFORMS)) {
                    newKeyValue.put(connectConfigKey, configs.getString(connectConfigKey));
                }
            }
            converterdConfigs.add(newKeyValue);
            taskId++;
        }
        putTaskConfigs(connectorName, converterdConfigs);
    }

    protected abstract void putTaskConfigs(String connectorName, List<ConnectKeyValue> configs);


    @NotNull
    protected Connector loadConnector(ConnectKeyValue configs) {
        String connectorClass = configs.getString(RuntimeConfigDefine.CONNECTOR_CLASS);
        Connector connector = plugin.newConnector(connectorClass);
        connector.validate(configs);
        connector.start(configs);
        return connector;
    }


    @Override
    public ClusterConfigState snapshot() {
        if (taskKeyValueStore == null && connectorKeyValueStore == null) {
            return ClusterConfigState.EMPTY;
        }
        Map<String, Integer> connectorTaskCounts = new HashMap<>();
        Map<ConnectorTaskId, Map<String, String>> connectorTaskConfigs = new ConcurrentHashMap<>();
        taskKeyValueStore.getKVMap().forEach((connectorName, taskConfigs) -> {
            connectorTaskCounts.put(connectorName, taskConfigs.size());
            taskConfigs.forEach(taskConfig -> {
                ConnectorTaskId id = new ConnectorTaskId(connectorName, taskConfig.getInt(RuntimeConfigDefine.TASK_ID));
                connectorTaskConfigs.put(id, taskConfig.getProperties());
            });
        });

        Map<String, Map<String, String>> connectorConfigs = new HashMap<>();
        Map<String, TargetState> connectorTargetStates = new HashMap<>();
        connectorKeyValueStore.getKVMap().forEach((connectorName, taskConfig) -> {
            connectorConfigs.put(connectorName, taskConfig.getProperties());
            connectorTargetStates.put(connectorName, taskConfig.getTargetState());
        });
        return new ClusterConfigState(connectorTaskCounts, connectorConfigs, connectorTargetStates, connectorTaskConfigs);
    }
}
