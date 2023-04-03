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

import org.apache.rocketmq.connect.runtime.config.WorkerConfig;

import java.util.List;

/**
 * Interface for cluster management.
 */
public interface ClusterManagementService {

    void initialize(WorkerConfig connectConfig);

    /**
     * Start the cluster manager.
     */
    void start();

    /**
     * Stop the cluster manager.
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
     * Check if Cluster Store Topic exists.
     *
     * @return true if Cluster Store Topic exists, otherwise return false.
     */
    boolean hasClusterStoreTopic();

    /**
     * Get all alive workers in the cluster.
     *
     * @return
     */
    List<String> getAllAliveWorkers();

    /**
     * Register a worker status listener to listen the change of alive workers.
     *
     * @param listener
     */
    void registerListener(WorkerStatusListener listener);

    /**
     * get current run worker
     *
     * @return
     */
    String getCurrentWorker();

    interface WorkerStatusListener {

        /**
         * If a worker online or offline, this method will be invoked. Can use method {@link
         * ClusterManagementService#getAllAliveWorkers()} to get the all current alive workers.
         */
        void onWorkerChange();
    }
}
