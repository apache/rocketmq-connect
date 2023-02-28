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
 *
 */

package org.apache.rocketmq.connect.runtime.service;

import io.openmessaging.connector.api.data.RecordConverter;
import io.openmessaging.connector.api.data.RecordOffset;
import org.apache.rocketmq.connect.runtime.config.WorkerConfig;
import org.apache.rocketmq.connect.runtime.store.ExtendRecordPartition;

import java.util.List;
import java.util.Map;

/**
 * Interface for position manager.
 */
public interface PositionManagementService {

    /**
     * Start the manager.
     */
    void start();

    /**
     * Stop the manager.
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
     * Persist position info in a persist store.
     */
    void persist();

    /**
     * Persist position info in a persist store.
     */
    void load();

    /**
     * Synchronize to other nodes.
     */
    void synchronize(boolean increment);

    /**
     * Get the current position table.
     *
     * @return
     */
    Map<ExtendRecordPartition, RecordOffset> getPositionTable();

    RecordOffset getPosition(ExtendRecordPartition partition);

    /**
     * Put a position info.
     */
    void putPosition(Map<ExtendRecordPartition, RecordOffset> positions);

    void putPosition(ExtendRecordPartition partition, RecordOffset position);

    /**
     * Remove a position info.
     *
     * @param partitions
     */
    void removePosition(List<ExtendRecordPartition> partitions);

    /**
     * Register a listener.
     *
     * @param listener
     */
    default void registerListener(PositionUpdateListener listener){
        // No-op
    }

    void initialize(WorkerConfig workerConfig, RecordConverter keyConverter, RecordConverter valueConverter);

    interface PositionUpdateListener {

        /**
         * Invoke while position info updated.
         */
        void onPositionUpdate();
    }
}
