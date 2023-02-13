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

import io.openmessaging.connector.api.errors.ConnectException;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.connect.runtime.service.ClusterManagementService;
import org.apache.rocketmq.connect.runtime.service.ClusterManagementServiceImpl;
import org.apache.rocketmq.connect.runtime.service.ConfigManagementService;
import org.apache.rocketmq.connect.runtime.service.local.LocalConfigManagementServiceImpl;
import org.apache.rocketmq.connect.runtime.service.local.LocalPositionManagementServiceImpl;
import org.apache.rocketmq.connect.runtime.service.local.LocalStateManagementServiceImpl;
import org.apache.rocketmq.connect.runtime.service.PositionManagementService;
import org.apache.rocketmq.connect.runtime.service.StateManagementService;
import org.jetbrains.annotations.NotNull;

import java.util.Iterator;
import java.util.ServiceLoader;

public class ServiceProviderUtil {

    /**
     * Get custer management service by class name
     *
     * @param clusterManagementServiceClazz
     * @return
     */
    @NotNull
    public static ClusterManagementService getClusterManagementService(String clusterManagementServiceClazz) {
        if (StringUtils.isEmpty(clusterManagementServiceClazz)) {
            clusterManagementServiceClazz = ClusterManagementServiceImpl.class.getName();
        }

        ClusterManagementService clusterManagementService = null;
        ServiceLoader<ClusterManagementService> clusterManagementServiceServiceLoader = ServiceLoader.load(ClusterManagementService.class);
        Iterator<ClusterManagementService> clusterManagementServiceIterator = clusterManagementServiceServiceLoader.iterator();

        while (clusterManagementServiceIterator.hasNext()) {
            ClusterManagementService currentClusterManagementService = clusterManagementServiceIterator.next();
            if (currentClusterManagementService.getClass().getName().equals(clusterManagementServiceClazz)) {
                clusterManagementService = currentClusterManagementService;
                break;
            }
        }
        if (null == clusterManagementService) {
            throw new ConnectException("ClusterManagementService class " + clusterManagementServiceClazz + " not " +
                    "found");
        }
        return clusterManagementService;
    }


    /**
     * Get config management service by class name
     *
     * @param configManagementServiceClazz
     * @return
     */
    @NotNull
    public static ConfigManagementService getConfigManagementService(String configManagementServiceClazz) {
        if (StringUtils.isEmpty(configManagementServiceClazz)) {
            configManagementServiceClazz = LocalConfigManagementServiceImpl.class.getName();
        }
        ConfigManagementService configManagementService = null;
        ServiceLoader<ConfigManagementService> configManagementServiceServiceLoader = ServiceLoader.load(ConfigManagementService.class);
        Iterator<ConfigManagementService> configManagementServiceIterator = configManagementServiceServiceLoader.iterator();
        while (configManagementServiceIterator.hasNext()) {
            ConfigManagementService currentConfigManagementService = configManagementServiceIterator.next();
            if (currentConfigManagementService.getClass().getName().equals(configManagementServiceClazz)) {
                configManagementService = currentConfigManagementService;
                break;
            }
        }
        if (null == configManagementService) {
            throw new ConnectException("ConfigManagementService class " + configManagementServiceClazz + " not " +
                    "found");
        }
        return configManagementService;
    }


    /**
     * Get position management service by class name
     *
     * @param positionManagementServiceClazz
     * @return
     */
    @NotNull
    public static PositionManagementService getPositionManagementService(String positionManagementServiceClazz) {
        if (StringUtils.isEmpty(positionManagementServiceClazz)) {
            positionManagementServiceClazz = LocalPositionManagementServiceImpl.class.getName();
        }

        PositionManagementService positionManagementService = null;
        ServiceLoader<PositionManagementService> positionManagementServiceServiceLoader = ServiceLoader.load(PositionManagementService.class);
        Iterator<PositionManagementService> positionManagementServiceIterator = positionManagementServiceServiceLoader.iterator();
        while (positionManagementServiceIterator.hasNext()) {
            PositionManagementService currentPositionManagementService = positionManagementServiceIterator.next();
            if (currentPositionManagementService.getClass().getName().equals(positionManagementServiceClazz)) {
                positionManagementService = currentPositionManagementService;
                break;
            }
        }
        if (null == positionManagementService) {
            throw new ConnectException("PositionManagementService class " + positionManagementServiceClazz + " not " +
                    "found");
        }
        return positionManagementService;
    }


    /**
     * Get state management service by class name
     *
     * @param stateManagementServiceClazz
     * @return
     */
    @NotNull
    public static StateManagementService getStateManagementService(String stateManagementServiceClazz) {
        if (StringUtils.isEmpty(stateManagementServiceClazz)) {
            stateManagementServiceClazz = LocalStateManagementServiceImpl.class.getName();
        }
        StateManagementService stateManagementService = null;
        ServiceLoader<StateManagementService> stateManagementServices = ServiceLoader.load(StateManagementService.class);
        Iterator<StateManagementService> stateManagementServiceIterator = stateManagementServices.iterator();
        while (stateManagementServiceIterator.hasNext()) {
            StateManagementService currentStateManagementService = stateManagementServiceIterator.next();
            if (currentStateManagementService.getClass().getName().equals(stateManagementServiceClazz)) {
                stateManagementService = currentStateManagementService;
                break;
            }
        }
        if (null == stateManagementService) {
            throw new ConnectException("StateManagementService class " + stateManagementServiceClazz + " not " +
                    "found");
        }
        return stateManagementService;
    }
}

