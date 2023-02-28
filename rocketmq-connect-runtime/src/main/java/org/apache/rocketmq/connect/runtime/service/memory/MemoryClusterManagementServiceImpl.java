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

import org.apache.rocketmq.connect.runtime.config.WorkerConfig;
import org.apache.rocketmq.connect.runtime.controller.standalone.StandaloneConfig;
import org.apache.rocketmq.connect.runtime.service.ClusterManagementService;

import java.util.Collections;
import java.util.List;

/**
 * standalone cluster management service
 */
public class MemoryClusterManagementServiceImpl implements ClusterManagementService {

    private StandaloneConfig config;

    public MemoryClusterManagementServiceImpl() {

    }

    @Override
    public void initialize(WorkerConfig connectConfig) {
        this.configure(connectConfig);
    }

    /**
     * Configure class with the given key-value pairs
     *
     * @param config can be DistributedConfig or StandaloneConfig
     */
    @Override
    public void configure(WorkerConfig config) {
        this.config = (StandaloneConfig) config;
    }

    /**
     * Start the cluster manager.
     */
    @Override
    public void start() {

    }

    /**
     * Stop the cluster manager.
     */
    @Override
    public void stop() {

    }

    /**
     * Check if Cluster Store Topic exists.
     *
     * @return true if Cluster Store Topic exists, otherwise return false.
     */
    @Override
    public boolean hasClusterStoreTopic() {
        return false;
    }

    /**
     * Get all alive workers in the cluster.
     *
     * @return
     */
    @Override
    public List<String> getAllAliveWorkers() {
        return Collections.singletonList(this.config.getWorkerId());
    }

    /**
     * Register a worker status listener to listen the change of alive workers.
     *
     * @param listener
     */
    @Override
    public void registerListener(WorkerStatusListener listener) {
    }

    @Override
    public String getCurrentWorker() {
        return this.config.getWorkerId();
    }

}
