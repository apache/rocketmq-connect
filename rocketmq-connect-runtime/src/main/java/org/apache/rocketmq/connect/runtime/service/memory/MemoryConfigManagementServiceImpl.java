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
package org.apache.rocketmq.connect.runtime.service.memory;


import io.openmessaging.KeyValue;
import io.openmessaging.connector.api.component.connector.Connector;
import org.apache.rocketmq.connect.runtime.common.ConnectKeyValue;
import org.apache.rocketmq.connect.runtime.common.LoggerName;
import org.apache.rocketmq.connect.runtime.config.ConnectConfig;
import org.apache.rocketmq.connect.runtime.config.RuntimeConfigDefine;
import org.apache.rocketmq.connect.runtime.connectorwrapper.Worker;
import org.apache.rocketmq.connect.runtime.service.ConfigManagementService;
import org.apache.rocketmq.connect.runtime.service.StagingMode;
import org.apache.rocketmq.connect.runtime.store.KeyValueStore;
import org.apache.rocketmq.connect.runtime.store.MemoryBasedKeyValueStore;
import org.apache.rocketmq.connect.runtime.utils.Plugin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * memory config management service impl for standalone
 */
public class MemoryConfigManagementServiceImpl implements ConfigManagementService {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.ROCKETMQ_RUNTIME);

    /**
     * Current connector configs in the store.
     */
    private KeyValueStore<String, ConnectKeyValue> connectorKeyValueStore;

    /**
     * Current task configs in the store.
     */
    private KeyValueStore<String, List<ConnectKeyValue>> taskKeyValueStore;

    /**
     * All listeners to trigger while config change.
     */
    private ConnectorConfigUpdateListener connectorConfigUpdateListener;

    private Plugin plugin;

    public MemoryConfigManagementServiceImpl() {
    }

    @Override
    public void initialize(ConnectConfig connectConfig, Plugin plugin) {
        this.connectorKeyValueStore = new MemoryBasedKeyValueStore<>();
        this.taskKeyValueStore = new MemoryBasedKeyValueStore<>();
        this.plugin = plugin;
    }

    @Override
    public void start() {
        connectorKeyValueStore.load();
        taskKeyValueStore.load();
    }

    @Override
    public void stop() {
        connectorKeyValueStore.persist();
        taskKeyValueStore.persist();
    }

    /**
     * get all connector configs enabled
     *
     * @return
     */
    @Override
    public Map<String, ConnectKeyValue> getConnectorConfigs() {
        Map<String, ConnectKeyValue> result = new HashMap<>();
        Map<String, ConnectKeyValue> connectorConfigs = connectorKeyValueStore.getKVMap();
        for (String connectorName : connectorConfigs.keySet()) {
            ConnectKeyValue config = connectorConfigs.get(connectorName);
            if (0 != config.getInt(RuntimeConfigDefine.CONFIG_DELETED)) {
                continue;
            }
            result.put(connectorName, config);
        }
        return result;
    }

    /**
     * get all connector configs include deleted
     *
     * @return
     */
    @Override
    public Map<String, ConnectKeyValue> getConnectorConfigsIncludeDeleted() {
        Map<String, ConnectKeyValue> result = new HashMap<>();
        Map<String, ConnectKeyValue> connectorConfigs = connectorKeyValueStore.getKVMap();
        for (String connectorName : connectorConfigs.keySet()) {
            ConnectKeyValue config = connectorConfigs.get(connectorName);
            result.put(connectorName, config);
        }
        return result;
    }

    @Override
    public String putConnectorConfig(String connectorName, ConnectKeyValue configs) throws Exception {
        ConnectKeyValue exist = connectorKeyValueStore.get(connectorName);
        if (null != exist) {
            Long updateTimestamp = exist.getLong(RuntimeConfigDefine.UPDATE_TIMESTAMP);
            if (null != updateTimestamp) {
                configs.put(RuntimeConfigDefine.UPDATE_TIMESTAMP, updateTimestamp);
            }
        }
        if (configs.equals(exist)) {
            return "Connector with same config already exist.";
        }

        Long currentTimestamp = System.currentTimeMillis();
        configs.put(RuntimeConfigDefine.UPDATE_TIMESTAMP, currentTimestamp);
        for (String requireConfig : RuntimeConfigDefine.REQUEST_CONFIG) {
            if (!configs.containsKey(requireConfig)) {
                return "Request config key: " + requireConfig;
            }
        }

        String connectorClass = configs.getString(RuntimeConfigDefine.CONNECTOR_CLASS);
        ClassLoader classLoader = plugin.getPluginClassLoader(connectorClass);
        Class clazz;
        if (null != classLoader) {
            clazz = Class.forName(connectorClass, true, classLoader);
        } else {
            clazz = Class.forName(connectorClass);
        }
        final Connector connector = (Connector) clazz.getDeclaredConstructor().newInstance();
        connector.validate(configs);
        connector.start(configs);
        connectorKeyValueStore.put(connectorName, configs);
        recomputeTaskConfigs(connectorName, connector, currentTimestamp, configs);
        return "";
    }

    @Override
    public void recomputeTaskConfigs(String connectorName, Connector connector, Long currentTimestamp, ConnectKeyValue configs) {
        int maxTask = configs.getInt(RuntimeConfigDefine.MAX_TASK, 1);
        ConnectKeyValue connectConfig = connectorKeyValueStore.get(connectorName);
        boolean directEnable = Boolean.parseBoolean(connectConfig.getString(RuntimeConfigDefine.CONNECTOR_DIRECT_ENABLE));
        List<KeyValue> taskConfigs = connector.taskConfigs(maxTask);
        List<ConnectKeyValue> converterdConfigs = new ArrayList<>();
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
            newKeyValue.put(RuntimeConfigDefine.TASK_CLASS, connector.taskClass().getName());
            newKeyValue.put(RuntimeConfigDefine.UPDATE_TIMESTAMP, currentTimestamp);

            newKeyValue.put(RuntimeConfigDefine.CONNECT_TOPICNAME, configs.getString(RuntimeConfigDefine.CONNECT_TOPICNAME));
            newKeyValue.put(RuntimeConfigDefine.CONNECT_TOPICNAMES, configs.getString(RuntimeConfigDefine.CONNECT_TOPICNAMES));
            Set<String> connectConfigKeySet = configs.keySet();
            for (String connectConfigKey : connectConfigKeySet) {
                if (connectConfigKey.startsWith(RuntimeConfigDefine.TRANSFORMS)) {
                    newKeyValue.put(connectConfigKey, configs.getString(connectConfigKey));
                }
            }
            converterdConfigs.add(newKeyValue);
        }
        putTaskConfigs(connectorName, converterdConfigs);
        triggerListener();
    }

    @Override
    public void removeConnectorConfig(String connectorName) {
        ConnectKeyValue config = connectorKeyValueStore.get(connectorName);
        config.put(RuntimeConfigDefine.UPDATE_TIMESTAMP, System.currentTimeMillis());
        config.put(RuntimeConfigDefine.CONFIG_DELETED, 1);
        List<ConnectKeyValue> taskConfigList = taskKeyValueStore.get(connectorName);
        taskConfigList.add(config);
        connectorKeyValueStore.put(connectorName, config);
        putTaskConfigs(connectorName, taskConfigList);
        log.info("[ISSUE #2027] After removal The configs are:\n" + getConnectorConfigs().toString());
        triggerListener();
    }

    @Override
    public Map<String, List<ConnectKeyValue>> getTaskConfigs() {
        Map<String, List<ConnectKeyValue>> result = new HashMap<>();
        Map<String, List<ConnectKeyValue>> taskConfigs = taskKeyValueStore.getKVMap();
        Map<String, ConnectKeyValue> filteredConnector = getConnectorConfigs();
        for (String connectorName : taskConfigs.keySet()) {
            if (!filteredConnector.containsKey(connectorName)) {
                continue;
            }
            result.put(connectorName, taskConfigs.get(connectorName));
        }
        return result;
    }

    private void putTaskConfigs(String connectorName, List<ConnectKeyValue> configs) {
        List<ConnectKeyValue> exist = taskKeyValueStore.get(connectorName);
        if (null != exist && exist.size() > 0) {
            taskKeyValueStore.remove(connectorName);
        }
        taskKeyValueStore.put(connectorName, configs);
    }

    @Override
    public void persist() {
        this.connectorKeyValueStore.persist();
        this.taskKeyValueStore.persist();
    }

    @Override
    public void registerListener(ConnectorConfigUpdateListener listener) {
        this.connectorConfigUpdateListener = listener;
    }

    private void triggerListener() {
        if (null == this.connectorConfigUpdateListener) {
            return;
        }
        connectorConfigUpdateListener.onConfigUpdate();
    }

    @Override
    public Plugin getPlugin() {
        return this.plugin;
    }

    @Override
    public StagingMode getStagingMode() {
        return StagingMode.STANDALONE;
    }
}
