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

import io.openmessaging.connector.api.data.RecordConverter;
import io.openmessaging.connector.api.data.Schema;
import io.openmessaging.connector.api.data.SchemaAndValue;
import io.openmessaging.connector.api.data.SchemaBuilder;
import io.openmessaging.connector.api.data.Struct;
import org.apache.rocketmq.connect.runtime.common.ConnAndTaskStatus;
import org.apache.rocketmq.connect.runtime.common.LoggerName;
import org.apache.rocketmq.connect.runtime.config.WorkerConfig;
import org.apache.rocketmq.connect.runtime.connectorwrapper.status.AbstractStatus;
import org.apache.rocketmq.connect.runtime.connectorwrapper.status.ConnectorStatus;
import org.apache.rocketmq.connect.runtime.connectorwrapper.status.TaskStatus;
import org.apache.rocketmq.connect.runtime.serialization.Serdes;
import org.apache.rocketmq.connect.runtime.store.KeyValueStore;
import org.apache.rocketmq.connect.runtime.utils.Callback;
import org.apache.rocketmq.connect.runtime.utils.ConnectUtil;
import org.apache.rocketmq.connect.runtime.utils.ConnectorTaskId;
import org.apache.rocketmq.connect.runtime.utils.Utils;
import org.apache.rocketmq.connect.runtime.utils.datasync.BrokerBasedLog;
import org.apache.rocketmq.connect.runtime.utils.datasync.DataSynchronizer;
import org.apache.rocketmq.connect.runtime.utils.datasync.DataSynchronizerCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * State management service
 */
public abstract class AbstractStateManagementService implements StateManagementService, IChangeNotifier<String, byte[]>, ICommonConfiguration {

    public static final String TASK_STATUS_PREFIX = "status-task-";
    public static final String CONNECTOR_STATUS_PREFIX = "status-connector-";
    public static final String STATE_KEY_NAME = "state";
    public static final String TRACE_KEY_NAME = "trace";
    public static final String WORKER_ID_KEY_NAME = "worker_id";
    public static final String GENERATION_KEY_NAME = "generation";
    protected static final Logger log = LoggerFactory.getLogger(LoggerName.ROCKETMQ_RUNTIME);
    private static final Schema STATUS_SCHEMA_V0 = SchemaBuilder.struct()
        .field(STATE_KEY_NAME, SchemaBuilder.string().build())
        .field(TRACE_KEY_NAME, SchemaBuilder.string().optional().build())
        .field(WORKER_ID_KEY_NAME, SchemaBuilder.string().build())
        .field(GENERATION_KEY_NAME, SchemaBuilder.int64().build())
        .build();
    protected final String statusManagePrefix = "StatusManage";
    /**
     * Synchronize config with other workers.
     */
    protected DataSynchronizer<String, byte[]> dataSynchronizer;

    /**
     * Current connector status in the store.
     */
    protected KeyValueStore<String, ConnectorStatus> connectorStatusStore;
    /**
     * Current task status in the store.
     */
    protected KeyValueStore<String, List<TaskStatus>> taskStatusStore;

    protected ConnAndTaskStatus connAndTaskStatus = new ConnAndTaskStatus();

    protected RecordConverter converter = new org.apache.rocketmq.connect.runtime.converter.record.json.JsonConverter();
    protected String statusTopic;
    protected boolean enabledCompactTopic = false;


    /**
     * initialize cb config
     *
     * @param config
     */
    @Override
    public void initialize(WorkerConfig config, RecordConverter converter) {
        // set config
        this.converter = converter;
        this.converter.configure(new HashMap<>());
        this.statusTopic = config.getConnectStatusTopic();
        this.dataSynchronizer = initializationDataSynchronizer(config);

        new BrokerBasedLog(config,
            this.statusTopic,
            ConnectUtil.createGroupName(statusManagePrefix, config.getWorkerId()),
            new StatusChangeCallback(),
            Serdes.serdeFrom(String.class),
            Serdes.serdeFrom(byte[].class),
            enabledCompactTopic()
        );
    }

    @Override
    public boolean enabledCompactTopic() {
        return false;
    }

    /**
     * sync send online config
     */
    protected abstract void replicaTargetState();

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
     * Safely set the state of the task to the given value. What is considered "safe" depends on the implementation, but
     * basically it means that the store can provide higher assurance that another worker hasn't concurrently written
     * any conflicting data.
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

        final byte[] value = serialize(status);
        dataSynchronizer.send(key, value, new Callback() {
            @Override
            public void onCompletion(Throwable error, Object result) {
                if (error != null) {
                    log.error("Failed to write status update", error);
                }
            }
        });
    }

    private byte[] serialize(AbstractStatus<?> status) {
        Struct struct = new Struct(STATUS_SCHEMA_V0);
        struct.put(STATE_KEY_NAME, status.getState().name());
        if (status.getTrace() != null)
            struct.put(TRACE_KEY_NAME, status.getTrace());
        struct.put(WORKER_ID_KEY_NAME, status.getWorkerId());
        struct.put(GENERATION_KEY_NAME, status.getGeneration());
        return converter.fromConnectData(this.statusTopic, STATUS_SCHEMA_V0, struct);
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
     * notify other connect node
     *
     * @param key
     * @param bytes
     */
    @Override
    public void notify(String key, byte[] bytes) {
        dataSynchronizer.send(key, bytes);
    }

    // ======= Start receives the status message and transforms the storage ======

    protected abstract void process(String key, byte[] value);

    /**
     * read connector status
     *
     * @param key
     * @param value
     */
    protected void readConnectorStatus(String key, byte[] value) {
        String connector = parseConnectorStatusKey(key);
        if (connector.isEmpty()) {
            log.warn("Discarding record with invalid connector status key {}", key);
            return;
        }
        ConnectorStatus status = parseConnectorStatus(connector, value);
        if (status == null || ConnectorStatus.State.DESTROYED == status.getState()) {
            log.trace("Removing connector status for {}", connector);
            remove(connector);
            return;
        }
        synchronized (this) {
            log.trace("Received connector {} status update {}", connector, status);
            ConnAndTaskStatus.CacheEntry<ConnectorStatus> entry = connAndTaskStatus.getOrAdd(connector);
            if (entry.get() != null) {
                if (status.getGeneration() > entry.get().getGeneration()) {
                    entry.put(status);
                }
            } else {
                entry.put(status);
            }
        }
    }

    private String parseConnectorStatusKey(String key) {
        return key.substring(CONNECTOR_STATUS_PREFIX.length());
    }

    private ConnectorStatus parseConnectorStatus(String connector, byte[] data) {
        try {
            SchemaAndValue schemaAndValue = converter.toConnectData(this.statusTopic, data);
            if (!(schemaAndValue.value() instanceof Struct)) {
                log.error("Invalid connector status type {}", schemaAndValue.value().getClass());
                return null;
            }
            Struct struct = (Struct) schemaAndValue.value();
            TaskStatus.State state = TaskStatus.State.valueOf((String) struct.get(STATE_KEY_NAME));
            String trace = (String) struct.get(TRACE_KEY_NAME);
            String workerUrl = (String) struct.get(WORKER_ID_KEY_NAME);
            Long generation = (Long) struct.get(GENERATION_KEY_NAME);
            return new ConnectorStatus(connector, state, workerUrl, generation, trace);
        } catch (Exception e) {
            log.error("Failed to deserialize connector status", e);
            return null;
        }
    }

    /**
     * read task status
     *
     * @param key
     * @param value
     */
    protected void readTaskStatus(String key, byte[] value) {
        ConnectorTaskId id = parseConnectorTaskId(key);
        if (id == null) {
            log.warn("Receive record with invalid task status key {}", key);
            return;
        }
        TaskStatus status = parseTaskStatus(id, value);
        if (status == null || TaskStatus.State.DESTROYED == status.getState()) {
            log.trace("Removing task status for {}", id);
            remove(id.connector());
            return;
        }

        synchronized (this) {
            log.trace("Received task {} status update {}", id, status);
            ConnAndTaskStatus.CacheEntry<TaskStatus> entry = connAndTaskStatus.getOrAdd(id);
            if (entry.get() != null) {
                if (status.getGeneration() > entry.get().getGeneration()) {
                    entry.put(status);
                }
            } else {
                entry.put(status);
            }
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

    private TaskStatus parseTaskStatus(ConnectorTaskId taskId, byte[] data) {
        try {
            SchemaAndValue schemaAndValue = converter.toConnectData(statusTopic, data);
            if (!(schemaAndValue.value() instanceof Struct)) {
                log.error("Invalid task status type {}", schemaAndValue.value().getClass());
                return null;
            }
            Struct struct = (Struct) schemaAndValue.value();
            TaskStatus.State state = TaskStatus.State.valueOf((String) struct.get(STATE_KEY_NAME));
            String trace = (String) struct.get(TRACE_KEY_NAME);
            String workerUrl = (String) struct.get(WORKER_ID_KEY_NAME);
            Long generation = (Long) struct.get(GENERATION_KEY_NAME);
            return new TaskStatus(taskId, state, workerUrl, generation, trace);
        } catch (Exception e) {
            log.error("Failed to deserialize task status", e);
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

    public class StatusChangeCallback implements DataSynchronizerCallback<String, byte[]> {
        @Override
        public void onCompletion(Throwable error, String key, byte[] value) {
            process(key, value);
        }
    }

}