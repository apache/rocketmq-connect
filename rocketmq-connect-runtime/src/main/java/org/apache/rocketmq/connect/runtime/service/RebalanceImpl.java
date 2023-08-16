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

import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.connect.runtime.common.ConnAndTaskConfigs;
import org.apache.rocketmq.connect.runtime.common.ConnectKeyValue;
import org.apache.rocketmq.connect.runtime.common.LoggerName;
import org.apache.rocketmq.connect.runtime.config.WorkerConfig;
import org.apache.rocketmq.connect.runtime.connectorwrapper.Worker;
import org.apache.rocketmq.connect.runtime.controller.AbstractConnectController;
import org.apache.rocketmq.connect.runtime.service.memory.MemoryClusterManagementServiceImpl;
import org.apache.rocketmq.connect.runtime.service.strategy.AllocateConnAndTaskStrategy;
import org.apache.rocketmq.connect.runtime.utils.ConnectUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

/**
 * Distribute connectors and tasks in current cluster.
 */
public class RebalanceImpl {

    private static final Logger log = LoggerFactory.getLogger(LoggerName.ROCKETMQ_RUNTIME);

    /**
     * Worker to schedule connectors and tasks in current process.
     */
    private final Worker worker;

    /**
     * ConfigManagementService to access current config info.
     */
    private final ConfigManagementService configManagementService;

    /**
     * ClusterManagementService to access current cluster info.
     */
    private final ClusterManagementService clusterManagementService;
    private final AbstractConnectController connectController;
    /**
     * Strategy to allocate connectors and tasks.
     */
    private AllocateConnAndTaskStrategy allocateConnAndTaskStrategy;

    public RebalanceImpl(Worker worker,
                         ConfigManagementService configManagementService,
                         ClusterManagementService clusterManagementService,
                         AllocateConnAndTaskStrategy strategy,
                         AbstractConnectController connectController) {

        this.worker = worker;
        this.configManagementService = configManagementService;
        this.clusterManagementService = clusterManagementService;
        this.allocateConnAndTaskStrategy = strategy;
        this.connectController = connectController;
    }

    public void checkClusterStoreTopic() {
        if (!clusterManagementService.hasClusterStoreTopic()) {
            WorkerConfig connectConfig = this.connectController.getConnectConfig();
            String clusterStoreTopic = this.connectController.getConnectConfig().getClusterStoreTopic();
            log.info("cluster store topic not exist, try to create it!");
            TopicConfig topicConfig = new TopicConfig(clusterStoreTopic, 1, 1, 6);
            ConnectUtil.createTopic(connectConfig, topicConfig);
        }
    }

    /**
     * Distribute connectors and tasks according to the {@link RebalanceImpl#allocateConnAndTaskStrategy}.
     */
    public void doRebalance() {
        List<String> curAliveWorkers = clusterManagementService.getAllAliveWorkers();
        if (curAliveWorkers != null) {
            if (clusterManagementService instanceof ClusterManagementServiceImpl) {
                log.info("Current Alive workers : " + curAliveWorkers.size());
            } else if (clusterManagementService instanceof MemoryClusterManagementServiceImpl) {
                log.info("Current alive worker : " + curAliveWorkers.iterator().next());
            }
        }

        // exculde delete connector
        Map<String, ConnectKeyValue> curConnectorConfigs = configManagementService.getConnectorConfigs();
        log.trace("Current ConnectorConfigs : " + curConnectorConfigs);
        Map<String, List<ConnectKeyValue>> curTaskConfigs = configManagementService.getTaskConfigs();
        log.trace("Current TaskConfigs : " + curTaskConfigs);
        ConnAndTaskConfigs allocateResult = allocateConnAndTaskStrategy.allocate(curAliveWorkers, clusterManagementService.getCurrentWorker(), curConnectorConfigs, curTaskConfigs);
        log.trace("Allocated connector:{}", allocateResult.getConnectorConfigs());
        log.trace("Allocated task:{}", allocateResult.getTaskConfigs());
        updateProcessConfigsInRebalance(allocateResult);
    }

    /**
     * Start all the connectors and tasks allocated to current process.
     *
     * @param allocateResult
     */
    private void updateProcessConfigsInRebalance(ConnAndTaskConfigs allocateResult) {
        try {
            worker.startConnectors(allocateResult.getConnectorConfigs(), connectController);
        } catch (Throwable e) {
            log.error("RebalanceImpl#updateProcessConfigsInRebalance start connector failed", e);
        }
        try {
            worker.startTasks(allocateResult.getTaskConfigs());
        } catch (Throwable e) {
            log.error("RebalanceImpl#updateProcessConfigsInRebalance start task failed", e);
        }
    }

}
