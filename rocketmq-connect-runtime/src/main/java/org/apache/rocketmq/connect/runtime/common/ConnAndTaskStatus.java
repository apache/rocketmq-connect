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

package org.apache.rocketmq.connect.runtime.common;

import org.apache.rocketmq.connect.runtime.connectorwrapper.status.AbstractStatus;
import org.apache.rocketmq.connect.runtime.connectorwrapper.status.ConnectorStatus;
import org.apache.rocketmq.connect.runtime.connectorwrapper.status.TaskStatus;
import org.apache.rocketmq.connect.runtime.utils.ConnectorTaskId;
import org.apache.rocketmq.connect.runtime.utils.Table;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;


/**
 * connect and tasks status config
 */
public class ConnAndTaskStatus {
    /**
     * connector task status
     */
    private Table<String, Integer, CacheEntry<TaskStatus>> tasks;
    /**
     * connector status
     */
    private Map<String, CacheEntry<ConnectorStatus>> connectors;

    public ConnAndTaskStatus() {
        tasks = new Table<>();
        connectors = new ConcurrentHashMap<>();
    }

    public Table<String, Integer, CacheEntry<TaskStatus>> getTasks() {
        return tasks;
    }

    public void setTasks(Table<String, Integer, CacheEntry<TaskStatus>> tasks) {
        this.tasks = tasks;
    }

    public Map<String, CacheEntry<ConnectorStatus>> getConnectors() {
        return connectors;
    }

    public void setConnectors(Map<String, CacheEntry<ConnectorStatus>> connectors) {
        this.connectors = connectors;
    }

    public synchronized CacheEntry<TaskStatus> getOrAdd(ConnectorTaskId task) {
        ConnAndTaskStatus.CacheEntry<TaskStatus> entry = tasks.get(task.connector(), task.task());
        if (entry == null) {
            entry = new CacheEntry<>();
            tasks.put(task.connector(), task.task(), entry);
        }
        return entry;
    }

    public synchronized CacheEntry<ConnectorStatus> getOrAdd(String connector) {
        CacheEntry<ConnectorStatus> entry = connectors.get(connector);
        if (entry == null) {
            entry = new CacheEntry<>();
            connectors.put(connector, entry);
        }
        return entry;
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ConnAndTaskStatus)) return false;
        ConnAndTaskStatus that = (ConnAndTaskStatus) o;
        return Objects.equals(tasks, that.tasks) && Objects.equals(connectors, that.connectors);
    }

    @Override
    public int hashCode() {
        return Objects.hash(tasks, connectors);
    }

    @Override
    public String toString() {
        return "ConnAndTaskStatus{" +
                "tasks=" + tasks +
                ", connectors=" + connectors +
                '}';
    }

    public static class CacheEntry<T extends AbstractStatus<?>> {
        private T value = null;
        private boolean deleted = false;

        public void put(T value) {
            this.value = value;
        }

        public T get() {
            return value;
        }

        /**
         * if it has been deleted, it is meaningless to send it again
         */
        public void delete() {
            this.deleted = true;
        }

        public boolean isDeleted() {
            return deleted;
        }

        public boolean canWrite(T status) {
            return value == null
                    || value.getWorkerId().equals(status.getWorkerId())
                    || status.getGeneration() >= value.getGeneration();
        }

    }
}
