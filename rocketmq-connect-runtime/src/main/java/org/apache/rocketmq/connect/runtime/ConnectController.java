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

package org.apache.rocketmq.connect.runtime;

import java.util.ArrayList;
import java.util.List;
import java.util.ServiceLoader;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.connect.runtime.common.LoggerName;
import org.apache.rocketmq.connect.runtime.config.ConnectConfig;
import org.apache.rocketmq.connect.runtime.connectorwrapper.Worker;
import org.apache.rocketmq.connect.runtime.rest.RestHandler;
import org.apache.rocketmq.connect.runtime.service.ClusterManagementService;
import org.apache.rocketmq.connect.runtime.service.ConfigManagementService;
import org.apache.rocketmq.connect.runtime.service.ConfigManagementServiceImpl;
import org.apache.rocketmq.connect.runtime.service.PositionManagementService;
import org.apache.rocketmq.connect.runtime.service.PositionManagementServiceImpl;
import org.apache.rocketmq.connect.runtime.service.RebalanceImpl;
import org.apache.rocketmq.connect.runtime.service.RebalanceService;
import org.apache.rocketmq.connect.runtime.service.strategy.AllocateConnAndTaskStrategy;
import org.apache.rocketmq.connect.runtime.stats.ConnectStatsManager;
import org.apache.rocketmq.connect.runtime.stats.ConnectStatsService;
import org.apache.rocketmq.connect.runtime.utils.ConnectUtil;
import org.apache.rocketmq.connect.runtime.utils.Plugin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Connect controller to access and control all resource in runtime.
 */
public class ConnectController {

    private static final Logger log = LoggerFactory.getLogger(LoggerName.ROCKETMQ_RUNTIME);

    /**
     * Configuration of current runtime.
     */
    private final ConnectConfig connectConfig;

    /**
     * All the configurations of current running connectors and tasks in cluster.
     */
    private final ConfigManagementService configManagementService;

    /**
     * Position management of source tasks.
     */
    private final PositionManagementService positionManagementService;

    /**
     * Manage the online info of the cluster.
     */
    private final ClusterManagementService clusterManagementService;

    /**
     * A worker to schedule all connectors and tasks assigned to current process.
     */
    private final Worker worker;

    /**
     * A REST handler, interacting with user.
     */
    private final RestHandler restHandler;

    /**
     * Assign all connectors and tasks to all alive process in the cluster.
     */
    private final RebalanceImpl rebalanceImpl;

    /**
     * A scheduled task to rebalance all connectors and tasks in the cluster.
     */
    private final RebalanceService rebalanceService;

    /**
     * Thread pool to run schedule task.
     */
    private ScheduledExecutorService scheduledExecutorService;

    private final Plugin plugin;

    private ConnectStatsManager connectStatsManager;

    private final ConnectStatsService connectStatsService;

    public ConnectController(
        ConnectConfig connectConfig) throws ClassNotFoundException, IllegalAccessException, InstantiationException {

        List<String> pluginPaths = new ArrayList<>(16);
        if (StringUtils.isNotEmpty(connectConfig.getPluginPaths())) {
            String[] strArr = connectConfig.getPluginPaths().split(",");
            for (String path : strArr) {
                if (StringUtils.isNotEmpty(path)) {
                    pluginPaths.add(path);
                }
            }
        }
        plugin = new Plugin(pluginPaths);
        plugin.initPlugin();
        this.connectStatsManager = new ConnectStatsManager(connectConfig);
        this.connectStatsService = new ConnectStatsService();
        this.connectConfig = connectConfig;
        ServiceLoader<ClusterManagementService> clusterManagementServiceServiceLoader = ServiceLoader.load(ClusterManagementService.class);
        this.clusterManagementService = clusterManagementServiceServiceLoader.iterator().next();
        this.clusterManagementService.initialize(connectConfig);
        this.configManagementService = new ConfigManagementServiceImpl(connectConfig, plugin);
        this.positionManagementService = new PositionManagementServiceImpl(connectConfig);
        this.worker = new Worker(connectConfig, positionManagementService, configManagementService, plugin, this);
        AllocateConnAndTaskStrategy strategy = ConnectUtil.initAllocateConnAndTaskStrategy(connectConfig);
        this.rebalanceImpl = new RebalanceImpl(worker, configManagementService, clusterManagementService, strategy, this);
        this.restHandler = new RestHandler(this);
        this.rebalanceService = new RebalanceService(rebalanceImpl, configManagementService, clusterManagementService);
    }

    public void initialize() {
        this.scheduledExecutorService = Executors.newSingleThreadScheduledExecutor((Runnable r) -> new Thread(r, "ConnectScheduledThread"));
    }

    public void start() {

        clusterManagementService.start();
        configManagementService.start();
        positionManagementService.start();
        worker.start();
        rebalanceService.start();
        connectStatsService.start();

        // Persist configurations of current connectors and tasks.
        this.scheduledExecutorService.scheduleAtFixedRate(() -> {

            try {
                ConnectController.this.configManagementService.persist();
            } catch (Exception e) {
                log.error("schedule persist config error.", e);
            }
        }, 1000, this.connectConfig.getConfigPersistInterval(), TimeUnit.MILLISECONDS);

        // Persist position information of source tasks.
        this.scheduledExecutorService.scheduleAtFixedRate(() -> {

            try {
                ConnectController.this.positionManagementService.persist();
            } catch (Exception e) {
                log.error("schedule persist position error.", e);
            }
        }, 1000, this.connectConfig.getPositionPersistInterval(), TimeUnit.MILLISECONDS);
    }

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

        this.scheduledExecutorService.shutdown();
        try {
            this.scheduledExecutorService.awaitTermination(5000, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            log.error("shutdown scheduledExecutorService error.", e);
        }

        if (rebalanceService != null) {
            rebalanceService.stop();
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

    public RestHandler getRestHandler() {
        return restHandler;
    }

    public RebalanceImpl getRebalanceImpl() {
        return rebalanceImpl;
    }

    public ConnectStatsManager getConnectStatsManager() {
        return connectStatsManager;
    }

    public void setConnectStatsManager(ConnectStatsManager connectStatsManager) {
        this.connectStatsManager = connectStatsManager;
    }

    public ConnectStatsService getConnectStatsService() {
        return connectStatsService;
    }
}
