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

import org.apache.rocketmq.connect.runtime.common.LoggerName;
import org.apache.rocketmq.connect.runtime.config.ConnectConfig;
import org.apache.rocketmq.connect.runtime.connectorwrapper.Worker;
import org.apache.rocketmq.connect.runtime.rest.RestHandler;
import org.apache.rocketmq.connect.runtime.service.ClusterManagementService;
import org.apache.rocketmq.connect.runtime.service.ConfigManagementService;
import org.apache.rocketmq.connect.runtime.service.PositionManagementService;
import org.apache.rocketmq.connect.runtime.stats.ConnectStatsManager;
import org.apache.rocketmq.connect.runtime.stats.ConnectStatsService;
import org.apache.rocketmq.connect.runtime.utils.Plugin;
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
    protected  final ConnectConfig connectConfig;

    /**
     * All the configurations of current running connectors and tasks in cluster.
     */
    protected final ConfigManagementService configManagementService;

    /**
     * Position management of source tasks.
     */
    protected final PositionManagementService positionManagementService;

    /**
     * Offset management of sink tasks.
     */
    protected final PositionManagementService offsetManagementService;

    /**
     * Manage the online info of the cluster.
     */
    protected final ClusterManagementService clusterManagementService;

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
     * @param connectConfig
     */
    public AbstractConnectController(
            Plugin plugin,
            ConnectConfig connectConfig,
            ClusterManagementService clusterManagementService,
            ConfigManagementService configManagementService,
            PositionManagementService positionManagementService,
            PositionManagementService offsetManagementService
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
        this.offsetManagementService = offsetManagementService;
        this.worker = new Worker(connectConfig, positionManagementService, configManagementService, plugin, this);
        this.restHandler = new RestHandler(this);
    }


    @Override
    public void start() {
        clusterManagementService.start();
        configManagementService.start();
        positionManagementService.start();
        offsetManagementService.start();
        worker.start();
        connectStatsService.start();
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

        if (offsetManagementService != null) {
            offsetManagementService.stop();
        }

        if (clusterManagementService != null) {
            clusterManagementService.stop();
        }

    }

    public ConnectConfig getConnectConfig() {
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

}
