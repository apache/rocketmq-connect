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


import io.openmessaging.connector.api.component.connector.Connector;
import io.openmessaging.connector.api.component.task.sink.SinkConnector;
import io.openmessaging.connector.api.component.task.source.SourceConnector;
import io.openmessaging.connector.api.data.RecordConverter;
import io.openmessaging.connector.api.errors.ConnectException;
import org.apache.rocketmq.connect.runtime.common.ConnectKeyValue;
import org.apache.rocketmq.connect.runtime.common.LoggerName;
import org.apache.rocketmq.connect.runtime.config.ConnectorConfig;
import org.apache.rocketmq.connect.runtime.config.SinkConnectorConfig;
import org.apache.rocketmq.connect.runtime.config.SourceConnectorConfig;
import org.apache.rocketmq.connect.runtime.config.WorkerConfig;
import org.apache.rocketmq.connect.runtime.connectorwrapper.TargetState;
import org.apache.rocketmq.connect.runtime.controller.isolation.Plugin;
import org.apache.rocketmq.connect.runtime.service.AbstractConfigManagementService;
import org.apache.rocketmq.connect.runtime.store.MemoryBasedKeyValueStore;
import org.apache.rocketmq.connect.runtime.utils.datasync.DataSynchronizer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

import static org.apache.rocketmq.connect.runtime.config.ConnectorConfig.CONNECTOR_CLASS;

/**
 * memory config management service impl for standalone
 */
public class MemoryConfigManagementServiceImpl extends AbstractConfigManagementService {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.ROCKETMQ_RUNTIME);
    /**
     * store topic
     */
    public String topic;
    /**
     * All listeners to trigger while config change.
     */
    private ConnectorConfigUpdateListener connectorConfigUpdateListener;

    public MemoryConfigManagementServiceImpl() {
    }

    @Override
    public void initialize(WorkerConfig workerConfig, RecordConverter converter, Plugin plugin) {

        this.topic = workerConfig.getConfigStoreTopic();
        this.plugin = plugin;

        this.connectorKeyValueStore = new MemoryBasedKeyValueStore<>();
        this.taskKeyValueStore = new MemoryBasedKeyValueStore<>();
    }

    @Override
    public DataSynchronizer initializationDataSynchronizer(WorkerConfig workerConfig) {
        return null;
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
        return connectorKeyValueStore.getKVMap();
    }


    @Override
    public String putConnectorConfig(String connectorName, ConnectKeyValue configs) {
        /**
         * check request config
         */
        for (String requireConfig : ConnectorConfig.REQUEST_CONFIG) {
            if (!configs.containsKey(requireConfig)) {
                throw new ConnectException("Request config key: " + requireConfig);
            }
        }

        // check exist
        ConnectKeyValue oldConfig = connectorKeyValueStore.get(connectorName);
        if (configs.equals(oldConfig)) {
            throw new ConnectException("Connector with same config already exist.");
        }

        // validate config
        Connector connector = plugin.newConnector(configs.getString(CONNECTOR_CLASS));
        if (connector instanceof SourceConnector) {
            new SourceConnectorConfig(configs).validate();
        } else if (connector instanceof SinkConnector) {
            new SinkConnectorConfig(configs).validate();
        }

        configs.setTargetState(TargetState.STARTED);
        configs.setEpoch(System.currentTimeMillis());
        // update cache
        connectorKeyValueStore.put(connectorName, configs);
        recomputeTaskConfigs(connectorName, configs);
        return connectorName;
    }

    @Override
    public void recomputeTaskConfigs(String connectorName, ConnectKeyValue configs) {
        super.recomputeTaskConfigs(connectorName, configs);
        triggerListener();
    }

    @Override
    public void deleteConnectorConfig(String connectorName) {
        connectorKeyValueStore.remove(connectorName);
        taskKeyValueStore.remove(connectorName);
        triggerListener();
    }

    /**
     * pause connector
     *
     * @param connectorName
     */
    @Override
    public void pauseConnector(String connectorName) {
        if (!connectorKeyValueStore.containsKey(connectorName)) {
            throw new ConnectException("Connector [" + connectorName + "] does not exist");
        }
        ConnectKeyValue config = connectorKeyValueStore.get(connectorName);
        config.setTargetState(TargetState.PAUSED);
        connectorKeyValueStore.put(connectorName, config.nextGeneration());
        triggerListener();
    }

    /**
     * resume connector
     *
     * @param connectorName
     */
    @Override
    public void resumeConnector(String connectorName) {
        if (!connectorKeyValueStore.containsKey(connectorName)) {
            throw new ConnectException("Connector [" + connectorName + "] does not exist");
        }
        ConnectKeyValue config = connectorKeyValueStore.get(connectorName);
        config.setEpoch(System.currentTimeMillis());
        config.setTargetState(TargetState.STARTED);
        connectorKeyValueStore.put(connectorName, config.nextGeneration());
        triggerListener();
    }


    @Override
    public Map<String, List<ConnectKeyValue>> getTaskConfigs() {
        return taskKeyValueStore.getKVMap();
    }

    @Override
    protected void putTaskConfigs(String connectorName, List<ConnectKeyValue> configs) {
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

    @Override
    public void triggerListener() {
        if (null == this.connectorConfigUpdateListener) {
            return;
        }
        connectorConfigUpdateListener.onConfigUpdate();
    }

    @Override
    public Plugin getPlugin() {
        return this.plugin;
    }

}
