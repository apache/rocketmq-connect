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

package org.apache.rocketmq.connect.runtime.controller.distributed;

import org.apache.rocketmq.connect.runtime.common.LoggerName;
import org.apache.rocketmq.connect.runtime.controller.AbstractConnectController;
import org.apache.rocketmq.connect.runtime.service.ClusterManagementService;
import org.apache.rocketmq.connect.runtime.service.ConfigManagementService;
import org.apache.rocketmq.connect.runtime.service.PositionManagementService;
import org.apache.rocketmq.connect.runtime.service.RebalanceImpl;
import org.apache.rocketmq.connect.runtime.service.RebalanceService;
import org.apache.rocketmq.connect.runtime.service.strategy.AllocateConnAndTaskStrategy;
import org.apache.rocketmq.connect.runtime.utils.ConnectUtil;
import org.apache.rocketmq.connect.runtime.utils.Plugin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Connect controller to access and control all resource in runtime.
 */
public class DistributedConnectController extends AbstractConnectController {

    private static final Logger log = LoggerFactory.getLogger(LoggerName.ROCKETMQ_RUNTIME);

    private final RebalanceImpl rebalanceImpl;

    /**
     * Thread pool to run schedule task.
     */
    protected ScheduledExecutorService scheduledExecutorService;

    /**
     * A scheduled task to rebalance all connectors and tasks in the cluster.
     */
    private final RebalanceService rebalanceService;

    public DistributedConnectController( Plugin plugin,
                                         DistributedConfig connectConfig,
                                         ClusterManagementService clusterManagementService,
                                         ConfigManagementService configManagementService,
                                         PositionManagementService positionManagementService,
                                         PositionManagementService offsetManagementService) {

        super(plugin, connectConfig, clusterManagementService, configManagementService, positionManagementService, offsetManagementService);
        AllocateConnAndTaskStrategy strategy = ConnectUtil.initAllocateConnAndTaskStrategy(connectConfig);
        this.rebalanceImpl = new RebalanceImpl(worker, configManagementService, clusterManagementService, strategy, this);
        this.rebalanceService = new RebalanceService(rebalanceImpl, configManagementService, clusterManagementService);
        this.scheduledExecutorService = Executors.newSingleThreadScheduledExecutor((Runnable r) -> new Thread(r, "ConnectScheduledThread"));

    }

    @Override
    public void start() {
        super.start();
        rebalanceService.start();
        // Persist configurations of current connectors and tasks.
        scheduledExecutorService.scheduleAtFixedRate(() -> {
            try {
                this.configManagementService.persist();
            } catch (Exception e) {
                log.error("schedule persist config error.", e);
            }
        }, 1000, this.connectConfig.getConfigPersistInterval(), TimeUnit.MILLISECONDS);

        // Persist position information of source tasks.
        scheduledExecutorService.scheduleAtFixedRate(() -> {

            try {
                this.positionManagementService.persist();
            } catch (Exception e) {
                log.error("schedule persist position error.", e);
            }

        }, 1000, this.connectConfig.getPositionPersistInterval(), TimeUnit.MILLISECONDS);

        // Persist offset information of sink tasks.
        scheduledExecutorService.scheduleAtFixedRate(() -> {
            try {
                this.offsetManagementService.persist();
            } catch (Exception e) {
                log.error("schedule persist offset error.", e);
            }
        }, 1000, this.connectConfig.getOffsetPersistInterval(), TimeUnit.MILLISECONDS);

    }

    @Override
    public void shutdown() {
        super.shutdown();
        if (rebalanceService != null) {
            rebalanceService.stop();
        }
        scheduledExecutorService.shutdown();
        try {
            scheduledExecutorService.awaitTermination(5000, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            log.error("shutdown scheduledExecutorService error.", e);
        }
    }
}
