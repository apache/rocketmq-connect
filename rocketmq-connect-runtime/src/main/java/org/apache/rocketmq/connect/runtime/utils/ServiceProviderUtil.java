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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.rocketmq.connect.runtime.utils;

import org.apache.rocketmq.connect.runtime.service.ClusterManagementService;
import org.apache.rocketmq.connect.runtime.service.ConfigManagementService;
import org.apache.rocketmq.connect.runtime.service.PositionManagementService;
import org.apache.rocketmq.connect.runtime.service.StagingMode;
import org.apache.rocketmq.connect.runtime.service.StateManagementService;
import org.jetbrains.annotations.NotNull;

import java.util.Iterator;
import java.util.ServiceLoader;

public class ServiceProviderUtil {

    @NotNull
    public static ClusterManagementService getClusterManagementServices(StagingMode stagingMode) {
        ClusterManagementService clusterManagementService = null;
        ClusterManagementService universalClusterManagementService = null;
        ServiceLoader<ClusterManagementService> clusterManagementServiceServiceLoader = ServiceLoader.load(ClusterManagementService.class);
        Iterator<ClusterManagementService> clusterManagementServiceIterator = clusterManagementServiceServiceLoader.iterator();
        while (clusterManagementServiceIterator.hasNext()) {
            ClusterManagementService clusterManagementService1 = clusterManagementServiceIterator.next();
            if (clusterManagementService1.getStagingMode() == stagingMode) {
                clusterManagementService = clusterManagementService1;
                break;
            }
            if (clusterManagementService1.getStagingMode() == StagingMode.UNIVERSAL) {
                universalClusterManagementService = clusterManagementService1;
                break;
            }
        }
        if (null == clusterManagementService) {
            clusterManagementService = universalClusterManagementService;
        }
        return clusterManagementService;
    }

    @NotNull
    public static ConfigManagementService getConfigManagementServices(StagingMode stagingMode) {
        ConfigManagementService configManagementService = null;
        ConfigManagementService universalConfigManagementService = null;
        ServiceLoader<ConfigManagementService> configManagementServiceServiceLoader = ServiceLoader.load(ConfigManagementService.class);
        Iterator<ConfigManagementService> configManagementServiceIterator = configManagementServiceServiceLoader.iterator();
        while (configManagementServiceIterator.hasNext()) {
            ConfigManagementService configManagementService1 = configManagementServiceIterator.next();
            if (configManagementService1.getStagingMode() == stagingMode) {
                configManagementService = configManagementService1;
                break;
            }
            if (configManagementService1.getStagingMode() == StagingMode.UNIVERSAL) {
                universalConfigManagementService = configManagementService1;
                break;
            }
        }
        if (null == configManagementService) {
            configManagementService = universalConfigManagementService;
        }
        return configManagementService;
    }

    @NotNull
    public static PositionManagementService getPositionManagementServices(StagingMode stagingMode) {
        PositionManagementService positionManagementService = null;
        PositionManagementService universalPositionManagementService = null;
        ServiceLoader<PositionManagementService> positionManagementServiceServiceLoader = ServiceLoader.load(PositionManagementService.class);
        Iterator<PositionManagementService> positionManagementServiceIterator = positionManagementServiceServiceLoader.iterator();
        while (positionManagementServiceIterator.hasNext()) {
            PositionManagementService positionManagementService1 = positionManagementServiceIterator.next();
            if (positionManagementService1.getStagingMode() == stagingMode) {
                positionManagementService = positionManagementService1;
                break;
            }
            if (positionManagementService1.getStagingMode() == StagingMode.UNIVERSAL) {
                universalPositionManagementService = positionManagementService1;
                break;
            }
        }
        if (null == positionManagementService) {
            positionManagementService = universalPositionManagementService;
        }
        return positionManagementService;
    }

    /**
     * state management service
     *
     * @param stagingMode
     * @return
     */
    @NotNull
    public static StateManagementService getStateManagementServices(StagingMode stagingMode) {
        StateManagementService stateManagementService = null;
        StateManagementService universalStateManagementService = null;
        ServiceLoader<StateManagementService> stateManagementServices = ServiceLoader.load(StateManagementService.class);
        Iterator<StateManagementService> stateManagementServiceIterator = stateManagementServices.iterator();
        while (stateManagementServiceIterator.hasNext()) {
            StateManagementService stateManagementService1 = stateManagementServiceIterator.next();
            if (stateManagementService1.getStagingMode() == stagingMode) {
                stateManagementService = stateManagementService1;
                break;
            }
            if (stateManagementService1.getStagingMode() == StagingMode.UNIVERSAL) {
                universalStateManagementService = stateManagementService1;
                break;
            }
        }
        if (null == stateManagementService) {
            stateManagementService = universalStateManagementService;
        }
        return stateManagementService;
    }
}

