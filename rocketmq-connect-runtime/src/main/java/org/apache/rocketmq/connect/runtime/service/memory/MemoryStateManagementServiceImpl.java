/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rocketmq.connect.runtime.service.memory;

import io.openmessaging.connector.api.data.RecordConverter;
import org.apache.rocketmq.connect.runtime.common.LoggerName;
import org.apache.rocketmq.connect.runtime.config.WorkerConfig;
import org.apache.rocketmq.connect.runtime.connectorwrapper.status.ConnectorStatus;
import org.apache.rocketmq.connect.runtime.connectorwrapper.status.TaskStatus;
import org.apache.rocketmq.connect.runtime.service.StateManagementService;
import org.apache.rocketmq.connect.runtime.utils.ConnectorTaskId;
import org.apache.rocketmq.connect.runtime.utils.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * status management service
 */
public class MemoryStateManagementServiceImpl implements StateManagementService {

    private static final Logger log = LoggerFactory.getLogger(LoggerName.ROCKETMQ_RUNTIME);

    private Table<String, Integer, TaskStatus> tasks;
    private Map<String, ConnectorStatus> connectors;

    /**
     * initialize cb config
     *
     * @param config
     */
    @Override
    public void initialize(WorkerConfig config, RecordConverter converter) {
        this.tasks = new Table<>();
        this.connectors = new ConcurrentHashMap<>();
    }

    /**
     * Start dependent services (if needed)
     */
    @Override
    public void start() {
    }


    /**
     * Stop dependent services (if needed)
     */
    @Override
    public void stop() {
    }

    /**
     * Set the state of the connector to the given value.
     *
     * @param status the status of the connector
     */
    @Override
    public synchronized void put(ConnectorStatus status) {
        if (status.getState() == ConnectorStatus.State.DESTROYED) {
            connectors.remove(status.getId());
        } else {
            connectors.put(status.getId(), status);
        }
    }

    /**
     * Safely set the state of the connector to the given value. What is
     * considered "safe" depends on the implementation, but basically it
     * means that the store can provide higher assurance that another worker
     * hasn't concurrently written any conflicting data.
     *
     * @param status the status of the connector
     */
    @Override
    public synchronized void putSafe(ConnectorStatus status) {
        put(status);
    }


    /**
     * Set the state of the connector to the given value.
     *
     * @param status the status of the task
     */
    @Override
    public synchronized void put(TaskStatus status) {
        if (status.getState() == TaskStatus.State.DESTROYED) {
            tasks.remove(status.getId().connector(), status.getId().task());
        } else {
            tasks.put(status.getId().connector(), status.getId().task(), status);
        }
    }

    /**
     * Safely set the state of the task to the given value. What is
     * considered "safe" depends on the implementation, but basically it
     * means that the store can provide higher assurance that another worker
     * hasn't concurrently written any conflicting data.
     *
     * @param status the status of the task
     */
    @Override
    public synchronized void putSafe(TaskStatus status) {
        put(status);
    }


    /**
     * Get the current state of the task.
     *
     * @param id the id of the task
     * @return the state or null if there is none
     */
    @Override
    public synchronized TaskStatus get(ConnectorTaskId id) {
        return tasks.get(id.connector(), id.task());
    }

    /**
     * Get the current state of the connector.
     *
     * @param connector the connector name
     * @return the state or null if there is none
     */
    @Override
    public synchronized ConnectorStatus get(String connector) {
        return connectors.get(connector);
    }

    /**
     * Get the states of all tasks for the given connector.
     *
     * @param connector the connector name
     * @return a map from task ids to their respective status
     */
    @Override
    public synchronized Collection<TaskStatus> getAll(String connector) {
        return new HashSet<>(tasks.row(connector).values());
    }

    /**
     * Get all cached connectors.
     *
     * @return the set of connector names
     */
    @Override
    public Set<String> connectors() {
        return new HashSet<>(connectors.keySet());
    }

}
