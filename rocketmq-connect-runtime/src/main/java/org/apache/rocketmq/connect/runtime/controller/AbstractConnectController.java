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

package org.apache.rocketmq.connect.runtime.controller;

import io.openmessaging.connector.api.errors.ConnectException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.rocketmq.connect.runtime.common.ConnectKeyValue;
import org.apache.rocketmq.connect.runtime.common.LoggerName;
import org.apache.rocketmq.connect.runtime.config.ConnectorConfig;
import org.apache.rocketmq.connect.runtime.config.WorkerConfig;
import org.apache.rocketmq.connect.runtime.connectorwrapper.Worker;
import org.apache.rocketmq.connect.runtime.connectorwrapper.status.ConnectorStatus;
import org.apache.rocketmq.connect.runtime.connectorwrapper.status.TaskStatus;
import org.apache.rocketmq.connect.runtime.controller.isolation.Plugin;
import org.apache.rocketmq.connect.runtime.rest.RestHandler;
import org.apache.rocketmq.connect.runtime.rest.entities.ConnectorInfo;
import org.apache.rocketmq.connect.runtime.rest.entities.ConnectorStateInfo;
import org.apache.rocketmq.connect.runtime.rest.entities.ConnectorType;
import org.apache.rocketmq.connect.runtime.rest.entities.TaskInfo;
import org.apache.rocketmq.connect.runtime.service.ClusterManagementService;
import org.apache.rocketmq.connect.runtime.service.ConfigManagementService;
import org.apache.rocketmq.connect.runtime.service.PositionManagementService;
import org.apache.rocketmq.connect.runtime.service.StateManagementService;
import org.apache.rocketmq.connect.runtime.stats.ConnectStatsManager;
import org.apache.rocketmq.connect.runtime.stats.ConnectStatsService;
import org.apache.rocketmq.connect.runtime.store.ClusterConfigState;
import org.apache.rocketmq.connect.runtime.utils.ConnectorTaskId;
import org.apache.rocketmq.connect.runtime.utils.CountDownLatch2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * connect controller
 */
public abstract class AbstractConnectController implements ConnectController {

    private static final Logger log = LoggerFactory.getLogger(LoggerName.ROCKETMQ_RUNTIME);

    /**
     * Configuration of current runtime.
     */
    protected final WorkerConfig connectConfig;

    /**
     * All the configurations of current running connectors and tasks in cluster.
     */
    protected final ConfigManagementService configManagementService;

    /**
     * Position management of source tasks.
     */
    protected final PositionManagementService positionManagementService;

    /**
     * Manage the online info of the cluster.
     */
    protected final ClusterManagementService clusterManagementService;

    /**
     * Manage the online task status of the cluster
     */
    protected final StateManagementService stateManagementService;

    /**
     * A worker to schedule all connectors and tasks assigned to current process.
     */
    protected final Worker worker;

    /**
     * A REST handler, interacting with user.
     */
    protected final RestHandler restHandler;

    protected final Plugin plugin;

    protected final ConnectStatsManager connectStatsManager;

    protected final ConnectStatsService connectStatsService;

    /**
     * init connect controller
     *
     * @param connectConfig
     */
    public AbstractConnectController(
        Plugin plugin,
        WorkerConfig connectConfig,
        ClusterManagementService clusterManagementService,
        ConfigManagementService configManagementService,
        PositionManagementService positionManagementService,
        StateManagementService stateManagementService
    ) {
        // set config
        this.connectConfig = connectConfig;
        // set plugin
        this.plugin = plugin;
        // set metrics
        this.connectStatsManager = new ConnectStatsManager(connectConfig);
        this.connectStatsService = new ConnectStatsService();

        this.clusterManagementService = clusterManagementService;
        this.configManagementService = configManagementService;
        this.positionManagementService = positionManagementService;
        this.stateManagementService = stateManagementService;
        this.worker = new Worker(connectConfig, positionManagementService, configManagementService, plugin, this, stateManagementService);
        this.restHandler = new RestHandler(this);
    }

    @Override
    public void start() {
        clusterManagementService.start();
        CountDownLatch2 countDownLatch2 = new CountDownLatch2(3);
        new Thread(() -> {
            positionManagementService.start();
            countDownLatch2.countDown();
        }, "Position data loading").start();

        new Thread(() -> {
            stateManagementService.start();
            countDownLatch2.countDown();
        }, "State data loading").start();

        new Thread(() -> {
            configManagementService.start();
            countDownLatch2.countDown();
        }, "Config data loading").start();

        long startTime = System.currentTimeMillis();
        try {
            countDownLatch2.await(); // wait for finished
        } catch (InterruptedException e) {
            throw new ConnectException(e);
        }
        long endTime = System.currentTimeMillis();
        log.info("Load config/position/state data finished, cost {}ms. ", endTime - startTime);
        connectStatsService.start();
        worker.start();
    }

    @Override
    public void shutdown() {

        if (worker != null) {
            worker.stop();
        }

        if (configManagementService != null) {
            configManagementService.stop();
        }

        if (positionManagementService != null) {
            positionManagementService.stop();
        }

        if (clusterManagementService != null) {
            clusterManagementService.stop();
        }
        if (stateManagementService != null) {
            stateManagementService.stop();
        }

    }

    public WorkerConfig getConnectConfig() {
        return connectConfig;
    }

    public ConfigManagementService getConfigManagementService() {
        return configManagementService;
    }

    public PositionManagementService getPositionManagementService() {
        return positionManagementService;
    }

    public ClusterManagementService getClusterManagementService() {
        return clusterManagementService;
    }

    public Worker getWorker() {
        return worker;
    }

    public ConnectStatsManager getConnectStatsManager() {
        return connectStatsManager;
    }

    public ConnectStatsService getConnectStatsService() {
        return connectStatsService;
    }

    /**
     * reload plugins
     */
    public void reloadPlugins() {
        configManagementService.getPlugin().initLoaders();
    }

    public List<String> aliveWorkers() {
        return clusterManagementService.getAllAliveWorkers();
    }

    /**
     * add connector
     *
     * @param connectorName
     * @param configs
     * @return
     * @throws Exception
     */
    public String putConnectorConfig(String connectorName, ConnectKeyValue configs) throws Exception {
        return configManagementService.putConnectorConfig(connectorName, configs);
    }

    /**
     * Remove the connector with the specified connector name in the cluster.
     *
     * @param connectorName
     */
    public void deleteConnectorConfig(String connectorName) {
        configManagementService.deleteConnectorConfig(connectorName);
    }

    /**
     * Pause the connector. This call will asynchronously suspend processing by the connector and all
     * of its tasks.
     *
     * @param connector name of the connector
     */
    public void pauseConnector(String connector) {
        configManagementService.pauseConnector(connector);
    }

    /**
     * Resume the connector. This call will asynchronously start the connector and its tasks (if
     * not started already).
     *
     * @param connector name of the connector
     */
    public void resumeConnector(String connector) {
        configManagementService.resumeConnector(connector);
    }

    /**
     * Get a list of connectors currently running in this cluster.
     *
     * @return A list of connector names
     */
    public Collection<String> connectors() {
        return configManagementService.snapshot().connectors();
    }

    public Collection<String> allocatedConnectors() {
        return worker.allocatedConnectors();
    }

    public Map<String, List<ConnectKeyValue>> allocatedTasks() {
        return worker.allocatedTasks();
    }

    public ConnectorStateInfo connectorStatus(String connName) {
        ConnectorStatus connector = stateManagementService.get(connName);
        if (connector == null) {
            throw new ConnectException("No status found for connector " + connName);
        }
        Collection<TaskStatus> tasks = stateManagementService.getAll(connName);

        ConnectorStateInfo.ConnectorState connectorState = new ConnectorStateInfo.ConnectorState(
            connector.getState().toString(), connector.getWorkerId(), connector.getTrace());
        List<ConnectorStateInfo.TaskState> taskStates = new ArrayList<>();

        for (TaskStatus status : tasks) {
            taskStates.add(
                new ConnectorStateInfo.TaskState(
                    status.getId().task(),
                    status.getState().toString(),
                    status.getWorkerId(),
                    status.getTrace()
                )
            );
        }
        Collections.sort(taskStates);
        Map<String, String> conf = rawConfig(connName);
        return new ConnectorStateInfo(connName, connectorState, taskStates,
            conf == null ? ConnectorType.UNKNOWN : connectorTypeForClass(conf.get(ConnectorConfig.CONNECTOR_CLASS)));
    }

    protected synchronized Map<String, String> rawConfig(String connName) {
        return configManagementService.snapshot().rawConnectorConfig(connName);
    }

    /**
     * Get the definition and status of a connector.
     *
     * @param connector name of the connector
     */
    public ConnectorInfo connectorInfo(String connector) {
        final ClusterConfigState configState = configManagementService.snapshot();
        if (!configState.contains(connector)) {
            throw new ConnectException("Connector[" + connector + "] does not exist");
        }
        Map<String, String> config = configState.rawConnectorConfig(connector);
        return new ConnectorInfo(
            connector,
            config,
            configState.tasks(connector),
            connectorTypeForClass(config.get(ConnectorConfig.CONNECTOR_CLASS))
        );
    }

    /**
     * task configs
     *
     * @param connName
     * @return
     */
    public List<TaskInfo> taskConfigs(final String connName) {
        ClusterConfigState configState = configManagementService.snapshot();
        List<TaskInfo> result = new ArrayList<>();
        for (int i = 0; i < configState.taskCount(connName); i++) {
            ConnectorTaskId id = new ConnectorTaskId(connName, i);
            result.add(new TaskInfo(id, configState.rawTaskConfig(id)));
        }
        return result;
    }

    public ConnectorStateInfo.TaskState taskStatus(ConnectorTaskId id) {
        TaskStatus status = stateManagementService.get(id);

        if (status == null) {
            throw new ConnectException("No status found for task " + id);
        }
        return new ConnectorStateInfo.TaskState(id.task(), status.getState().toString(),
            status.getWorkerId(), status.getTrace());
    }

    /**
     * Retrieves ConnectorType for the corresponding connector class
     *
     * @param connClass class of the connector
     */
    public ConnectorType connectorTypeForClass(String connClass) {
        return ConnectorType.from(plugin.newConnector(connClass).getClass());
    }

    public Plugin plugin() {
        return this.plugin;
    }
}
