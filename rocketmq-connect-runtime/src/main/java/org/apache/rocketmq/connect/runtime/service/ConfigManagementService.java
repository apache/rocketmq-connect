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

import io.openmessaging.connector.api.data.RecordConverter;
import org.apache.rocketmq.connect.runtime.common.ConnectKeyValue;
import org.apache.rocketmq.connect.runtime.config.WorkerConfig;
import org.apache.rocketmq.connect.runtime.controller.isolation.Plugin;
import org.apache.rocketmq.connect.runtime.store.ClusterConfigState;

import java.util.List;
import java.util.Map;

/**
 * Interface for config manager. Contains connector configs and task configs. All worker in a cluster should keep the
 * same configs.
 */
public interface ConfigManagementService {


    /**
     * Start the config manager.
     */
    void start();

    /**
     * Stop the config manager.
     */
    void stop();

    /**
     * Configure class with the given key-value pairs
     *
     * @param config can be DistributedConfig or StandaloneConfig
     */
    default void configure(WorkerConfig config) {

    }

    /**
     * Get all connector configs from the cluster filter out DELETE set to 1
     *
     * @return
     */
    Map<String, ConnectKeyValue> getConnectorConfigs();

    /**
     * Put the configs of the specified connector in the cluster.
     *
     * @param connectorName
     * @param configs
     * @return
     * @throws Exception
     */
    String putConnectorConfig(String connectorName, ConnectKeyValue configs);

    /**
     * Remove the connector with the specified connector name in the cluster.
     *
     * @param connectorName
     */
    void deleteConnectorConfig(String connectorName);

    /**
     * pause connector
     *
     * @param connectorName
     */
    void pauseConnector(String connectorName);

    /**
     * resume connector
     *
     * @param connectorName
     */
    void resumeConnector(String connectorName);

    /**
     * Recompute task configs
     * @param connectorName
     * @param configs
     */
    void recomputeTaskConfigs(String connectorName, ConnectKeyValue configs);

    /**
     * Get all Task configs.
     *
     * @return
     */
    Map<String, List<ConnectKeyValue>> getTaskConfigs();

    /**
     * Persist all the configs in a store.
     */
    void persist();

    /**
     * Register a listener to listen all config update operations.
     *
     * @param listener
     */
    void registerListener(ConnectorConfigUpdateListener listener);

    void initialize(WorkerConfig workerConfig, RecordConverter converter, Plugin plugin);

    ClusterConfigState snapshot();

    Plugin getPlugin();

    interface ConnectorConfigUpdateListener {
        /**
         * Invoke while connector config changed.
         */
        void onConfigUpdate();
    }
}
