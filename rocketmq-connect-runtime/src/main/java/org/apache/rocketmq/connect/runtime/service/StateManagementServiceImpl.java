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
package org.apache.rocketmq.connect.runtime.service;

import com.alibaba.fastjson.JSON;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.connect.runtime.common.ConnAndTaskStatus;
import org.apache.rocketmq.connect.runtime.common.LoggerName;
import org.apache.rocketmq.connect.runtime.config.WorkerConfig;
import org.apache.rocketmq.connect.runtime.connectorwrapper.status.AbstractStatus;
import org.apache.rocketmq.connect.runtime.connectorwrapper.status.ConnectorStatus;
import org.apache.rocketmq.connect.runtime.connectorwrapper.status.TaskStatus;
import org.apache.rocketmq.connect.runtime.converter.ConnAndTasksStatusConverter;
import org.apache.rocketmq.connect.runtime.converter.JsonConverter;
import org.apache.rocketmq.connect.runtime.converter.ListConverter;
import org.apache.rocketmq.connect.runtime.store.FileBaseKeyValueStore;
import org.apache.rocketmq.connect.runtime.store.KeyValueStore;
import org.apache.rocketmq.connect.runtime.utils.Callback;
import org.apache.rocketmq.connect.runtime.utils.ConnectUtil;
import org.apache.rocketmq.connect.runtime.utils.ConnectorTaskId;
import org.apache.rocketmq.connect.runtime.utils.FilePathConfigUtil;
import org.apache.rocketmq.connect.runtime.utils.Utils;
import org.apache.rocketmq.connect.runtime.utils.datasync.BrokerBasedLog;
import org.apache.rocketmq.connect.runtime.utils.datasync.DataSynchronizer;
import org.apache.rocketmq.connect.runtime.utils.datasync.DataSynchronizerCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * State management service
 */
public class StateManagementServiceImpl implements StateManagementService {

    private static final Logger log = LoggerFactory.getLogger(LoggerName.ROCKETMQ_RUNTIME);

    private final String statusManagePrefix = "StatusManage";
    public static final String TASK_STATUS_PREFIX = "status-task-";
    public static final String CONNECTOR_STATUS_PREFIX = "status-connector-";

    /**
     * Synchronize config with other workers.
     */
    private DataSynchronizer<String, String> dataSynchronizer;
    /**
     * Current connector status in the store.
     */
    protected KeyValueStore<String, ConnectorStatus> connectorStatusStore;

    /**
     * Current task status in the store.
     */
    protected KeyValueStore<String, List<TaskStatus>> taskStatusStore;
    protected ConnAndTaskStatus connAndTaskStatus = new ConnAndTaskStatus();

    /**
     * Preparation before startup
     *
     * @param connectConfig
     */
    private void prepare(WorkerConfig connectConfig) {
        String connectStatusTopic = connectConfig.getConnectStatusTopic();
        if (!ConnectUtil.isTopicExist(connectConfig, connectStatusTopic)) {
            log.info("try to create status topic: {}!", connectStatusTopic);
            TopicConfig topicConfig = new TopicConfig(connectStatusTopic, 1, 1, 6);
            ConnectUtil.createTopic(connectConfig, topicConfig);
        }
    }

    /**
     * initialize cb config
     *
     * @param config
     */
    @Override
    public void initialize(WorkerConfig config) {
        this.dataSynchronizer = new BrokerBasedLog(config,
                config.getConnectStatusTopic(),
                ConnectUtil.createGroupName(statusManagePrefix, config.getWorkerId()),
                new StatusChangeCallback(),
                new JsonConverter(),
                new ConnAndTasksStatusConverter());

        /**connector status store*/
        this.connectorStatusStore = new FileBaseKeyValueStore<>(
                FilePathConfigUtil.getConnectorStatusConfigPath(config.getStorePathRootDir()),
                new JsonConverter(),
                new JsonConverter(ConnectorStatus.class));

        /**task status store*/
        this.taskStatusStore = new FileBaseKeyValueStore<>(
                FilePathConfigUtil.getTaskStatusConfigPath(config.getStorePathRootDir()),
                new JsonConverter(),
                new ListConverter(TaskStatus.class));
        // create topic
        this.prepare(config);
    }

    /**
     * Start dependent services (if needed)
     */
    @Override
    public void start() {
        connectorStatusStore.load();
        taskStatusStore.load();
        dataSynchronizer.start();
        sendOnlineConfig();
    }

    /**
     * sync send online config
     */
    private synchronized void sendOnlineConfig() {
        /**connector status map*/
        Map<String, ConnectorStatus> connectorStatusMap = connectorStatusStore.getKVMap();
        connectorStatusMap.forEach((connectorName, connectorStatus) -> {
            if (connectorStatus == null){
                return;
            }
            // send status
            put(connectorStatus);
        });

        /** task status map */
        Map<String, List<TaskStatus>> taskStatusMap = taskStatusStore.getKVMap();
        if (taskStatusMap.isEmpty()){
            return;
        }
        taskStatusMap.forEach((connectorName, taskStatusList) -> {
            if (taskStatusList == null || taskStatusList.isEmpty()){
                return;
            }
            taskStatusList.forEach(taskStatus -> {
                // send status
                put(taskStatus);
            });
        });
    }
    /**
     * Stop dependent services (if needed)
     */
    @Override
    public void stop() {
        sendOnlineConfig();
        prePersist();
        connectorStatusStore.persist();
        taskStatusStore.persist();
        dataSynchronizer.stop();
    }

    /**
     * pre persist
     */
    private void prePersist() {
        Map<String, ConnAndTaskStatus.CacheEntry<ConnectorStatus>> connectors = connAndTaskStatus.getConnectors();
        if (connectors.isEmpty()){
            return;
        }
        connectors.forEach((connectName, connectorStatus) -> {
            connectorStatusStore.put(connectName, connectorStatus.get());
            Map<Integer, ConnAndTaskStatus.CacheEntry<TaskStatus>> cacheTaskStatus = connAndTaskStatus.getTasks().row(connectName);
            if (cacheTaskStatus == null){
                return;
            }
            taskStatusStore.put(connectName, new ArrayList<>());
            cacheTaskStatus.forEach((taskId, taskStatus) -> {
                if (taskStatus != null){
                    taskStatusStore.get(connectName).add(taskStatus.get());
                }
            });
        });
    }

    @Override
    public void persist() {
        prePersist();
        connectorStatusStore.persist();
        taskStatusStore.persist();
    }

    /**
     * Set the state of the connector to the given value.
     *
     * @param status the status of the connector
     */
    @Override
    public void put(ConnectorStatus status) {
        sendConnectorStatus(status, false);
    }

    /**
     * @param status the status of the connector
     */
    @Override
    public void putSafe(ConnectorStatus status) {
        sendConnectorStatus(status, true);
    }


    /**
     * Set the state of the connector to the given value.
     *
     * @param status the status of the task
     */
    @Override
    public void put(TaskStatus status) {
        sendTaskStatus(status, false);
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
    public void putSafe(TaskStatus status) {
        sendTaskStatus(status, true);
    }

    private void sendConnectorStatus(final ConnectorStatus status, boolean safeWrite) {
        String connector = status.getId();
        ConnAndTaskStatus.CacheEntry<ConnectorStatus> entry = connAndTaskStatus.getOrAdd(connector);
        String key = CONNECTOR_STATUS_PREFIX + connector;
        // send status
        send(key, status, entry, safeWrite);
    }

    private void sendTaskStatus(final TaskStatus status, boolean safeWrite) {
        ConnectorTaskId taskId = status.getId();
        ConnAndTaskStatus.CacheEntry<TaskStatus> entry = connAndTaskStatus.getOrAdd(taskId);
        String key = TASK_STATUS_PREFIX + taskId.connector() + "-" + taskId.task();
        // send status
        send(key, status, entry, safeWrite);
    }


    private <V extends AbstractStatus<?>> void send(final String key,
                                                    final V status,
                                                    final ConnAndTaskStatus.CacheEntry<V> entry,
                                                    final boolean safeWrite) {
        synchronized (this) {
            if (safeWrite && !entry.canWrite(status)) {
                return;
            }
        }

        dataSynchronizer.send(key, JSON.toJSONString(status), new Callback() {
            @Override
            public void onCompletion(Throwable error, Object result) {
                if (error != null) {
                    log.error("Failed to write status update", error);
                }
            }
        });
    }


    /**
     * Get the current state of the task.
     *
     * @param id the id of the task
     * @return the state or null if there is none
     */
    @Override
    public TaskStatus get(ConnectorTaskId id) {
        ConnAndTaskStatus.CacheEntry<TaskStatus> cacheEntry = connAndTaskStatus.getTasks().get(id.connector(), id.task());
        if (cacheEntry == null) {
            return null;
        }
        return cacheEntry.get();
    }

    /**
     * Get the current state of the connector.
     *
     * @param connector the connector name
     * @return the state or null if there is none
     */
    @Override
    public ConnectorStatus get(String connector) {
        ConnAndTaskStatus.CacheEntry<ConnectorStatus> cacheEntry = connAndTaskStatus.getConnectors().get(connector);
        if (cacheEntry == null) {
            return null;
        }
        return cacheEntry.get();
    }

    /**
     * Get the states of all tasks for the given connector.
     *
     * @param connector the connector name
     * @return a map from task ids to their respective status
     */
    @Override
    public Collection<TaskStatus> getAll(String connector) {
        List<TaskStatus> res = new ArrayList<>();
        for (ConnAndTaskStatus.CacheEntry<TaskStatus> statusEntry : connAndTaskStatus.getTasks().row(connector).values()) {
            TaskStatus status = statusEntry.get();
            if (status != null) {
                res.add(status);
            }
        }
        return res;
    }

    /**
     * Get all cached connectors.
     *
     * @return the set of connector names
     */
    @Override
    public Set<String> connectors() {
        return new HashSet<>(connAndTaskStatus.getConnectors().keySet());
    }

    /**
     * get staging mode
     *
     * @return
     */
    @Override
    public StagingMode getStagingMode() {
        return StagingMode.DISTRIBUTED;
    }


    private class StatusChangeCallback implements DataSynchronizerCallback<String, String> {
        @Override
        public void onCompletion(Throwable error, String key, String result) {
            if (key.startsWith(CONNECTOR_STATUS_PREFIX)) {
                readConnectorStatus(key, JSON.parseObject(result, ConnectorStatus.class));
            } else if (key.startsWith(TASK_STATUS_PREFIX)) {
                readTaskStatus(key, JSON.parseObject(result, TaskStatus.class));
            } else {
                log.warn("Discarding record with invalid key {}", key);
            }
        }
    }

    /**
     * read connector status
     *
     * @param key
     * @param status
     */
    private void readConnectorStatus(String key, ConnectorStatus status) {
        String connector = parseConnectorStatusKey(key);
        if (connector.isEmpty()) {
            log.warn("Discarding record with invalid connector status key {}", key);
            return;
        }

        if (status == null || ConnectorStatus.State.DESTROYED == status.getState()) {
            log.trace("Removing connector status for {}", connector);
            remove(connector);
            return;
        }
        synchronized (this) {
            log.trace("Received connector {} status update {}", connector, status);
            ConnAndTaskStatus.CacheEntry<ConnectorStatus> entry = connAndTaskStatus.getOrAdd(connector);
            entry.put(status);
        }
    }

    private String parseConnectorStatusKey(String key) {
        return key.substring(CONNECTOR_STATUS_PREFIX.length());
    }

    /**
     * read task status
     *
     * @param key
     * @param status
     */
    private void readTaskStatus(String key, TaskStatus status) {
        ConnectorTaskId id = parseConnectorTaskId(key);
        if (id == null) {
            log.warn("Receive record with invalid task status key {}", key);
            return;
        }

        if (status == null || TaskStatus.State.DESTROYED == status.getState()) {
            log.trace("Removing task status for {}", id);
            remove(id.connector());
            return;
        }

        synchronized (this) {
            log.trace("Received task {} status update {}", id, status);
            ConnAndTaskStatus.CacheEntry<TaskStatus> entry = connAndTaskStatus.getOrAdd(id);
            entry.put(status);
        }
    }

    private ConnectorTaskId parseConnectorTaskId(String key) {
        String[] parts = key.split("-");
        if (parts.length < 4) {
            return null;
        }
        try {
            int taskNum = Integer.parseInt(parts[parts.length - 1]);
            String connectorName = Utils.join(Arrays.copyOfRange(parts, 2, parts.length - 1), "-");
            return new ConnectorTaskId(connectorName, taskNum);
        } catch (NumberFormatException e) {
            log.warn("Invalid task status key {}", key);
            return null;
        }
    }

    /**
     * remove connector
     *
     * @param connector
     */
    private synchronized void remove(String connector) {
        ConnAndTaskStatus.CacheEntry<ConnectorStatus> removed = connAndTaskStatus.getConnectors().remove(connector);
        if (removed != null) {
            removed.delete();
        }

        Map<Integer, ConnAndTaskStatus.CacheEntry<TaskStatus>> tasks = connAndTaskStatus.getTasks().remove(connector);
        if (tasks != null) {
            for (ConnAndTaskStatus.CacheEntry<TaskStatus> taskEntry : tasks.values()) {
                taskEntry.delete();
            }
        }
    }

}
