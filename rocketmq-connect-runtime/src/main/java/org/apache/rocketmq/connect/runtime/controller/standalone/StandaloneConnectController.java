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

package org.apache.rocketmq.connect.runtime.controller.standalone;

import org.apache.rocketmq.connect.runtime.controller.AbstractConnectController;
import org.apache.rocketmq.connect.runtime.controller.isolation.Plugin;
import org.apache.rocketmq.connect.runtime.service.ClusterManagementService;
import org.apache.rocketmq.connect.runtime.service.ConfigManagementService;
import org.apache.rocketmq.connect.runtime.service.PositionManagementService;
import org.apache.rocketmq.connect.runtime.service.RebalanceImpl;
import org.apache.rocketmq.connect.runtime.service.StateManagementService;
import org.apache.rocketmq.connect.runtime.service.memory.StandaloneRebalanceService;
import org.apache.rocketmq.connect.runtime.service.strategy.AllocateConnAndTaskStrategy;
import org.apache.rocketmq.connect.runtime.utils.ConnectUtil;

/**
 * Connect controller to access and control all resource in runtime.
 */
public class StandaloneConnectController extends AbstractConnectController {

    private final RebalanceImpl rebalanceImpl;
    private final StandaloneRebalanceService rebalanceService;

    public StandaloneConnectController(Plugin plugin,
                                       StandaloneConfig connectConfig,
                                       ClusterManagementService clusterManagementService,
                                       ConfigManagementService configManagementService,
                                       PositionManagementService positionManagementService,
                                       StateManagementService stateManagementService) {
        super(plugin, connectConfig, clusterManagementService, configManagementService, positionManagementService, stateManagementService);
        AllocateConnAndTaskStrategy strategy = ConnectUtil.initAllocateConnAndTaskStrategy(connectConfig);
        this.rebalanceImpl = new RebalanceImpl(worker, configManagementService, clusterManagementService, strategy, this);
        this.rebalanceService = new StandaloneRebalanceService(rebalanceImpl, configManagementService, clusterManagementService);

    }

    @Override
    public void start() {
        super.start();
        rebalanceService.start();
    }

    @Override
    public void shutdown() {
        super.shutdown();
        if (rebalanceService != null) {
            rebalanceService.stop();
        }
    }
}
