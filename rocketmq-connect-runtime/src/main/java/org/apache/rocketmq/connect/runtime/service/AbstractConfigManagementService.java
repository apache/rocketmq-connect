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

import io.openmessaging.KeyValue;
import io.openmessaging.connector.api.component.connector.Connector;
import org.apache.commons.collections.CollectionUtils;
import org.apache.rocketmq.connect.runtime.common.ConfigException;
import io.openmessaging.connector.api.component.task.sink.SinkConnector;
import io.openmessaging.connector.api.component.task.source.SourceConnector;
import io.openmessaging.connector.api.data.RecordConverter;
import io.openmessaging.connector.api.data.Schema;
import io.openmessaging.connector.api.data.SchemaAndValue;
import io.openmessaging.connector.api.data.SchemaBuilder;
import io.openmessaging.connector.api.data.Struct;
import io.openmessaging.connector.api.errors.ConnectException;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.connect.runtime.common.ConnectKeyValue;
import org.apache.rocketmq.connect.runtime.common.LoggerName;
import org.apache.rocketmq.connect.runtime.config.ConnectorConfig;
import org.apache.rocketmq.connect.runtime.config.SinkConnectorConfig;
import org.apache.rocketmq.connect.runtime.config.SourceConnectorConfig;
import org.apache.rocketmq.connect.runtime.config.WorkerConfig;
import org.apache.rocketmq.connect.runtime.connectorwrapper.TargetState;
import org.apache.rocketmq.connect.runtime.connectorwrapper.Worker;
import org.apache.rocketmq.connect.runtime.controller.isolation.Plugin;
import org.apache.rocketmq.connect.runtime.store.ClusterConfigState;
import org.apache.rocketmq.connect.runtime.store.KeyValueStore;
import org.apache.rocketmq.connect.runtime.utils.ConnectorTaskId;
import org.apache.rocketmq.connect.runtime.utils.Utils;
import org.apache.rocketmq.connect.runtime.utils.datasync.DataSynchronizer;
import org.apache.rocketmq.connect.runtime.utils.datasync.DataSynchronizerCallback;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static org.apache.rocketmq.connect.runtime.config.ConnectorConfig.CONNECTOR_CLASS;

/**
 * Interface for config manager. Contains connector configs and task configs. All worker in a cluster should keep the
 * same configs.
 */
public abstract class AbstractConfigManagementService implements ConfigManagementService, IChangeNotifier<String, byte[]>, ICommonConfiguration {

    public static final String TARGET_STATE_PREFIX = "target-state-";
    public static final String CONNECTOR_PREFIX = "connector-";
    public static final String TASK_PREFIX = "task-";
    public static final String DELETE_CONNECTOR_PREFIX = "delete-";
    protected static final String FIELD_STATE = "state";
    protected static final String FIELD_EPOCH = "epoch";
    protected static final String FIELD_PROPS = "properties";
    protected static final String FIELD_DELETED = "deleted";
    /**
     * delete connector V0
     */
    @Deprecated
    public static final Schema CONNECTOR_DELETE_CONFIGURATION_V0 = SchemaBuilder.struct()
            .field(FIELD_EPOCH, SchemaBuilder.int64().build())
            .build();

    /**
     * delete connector V1
     */
    public static final Schema CONNECTOR_DELETE_CONFIGURATION_V1 = SchemaBuilder.struct()
            .field(FIELD_EPOCH, SchemaBuilder.int64().build())
            .field(FIELD_DELETED, SchemaBuilder.bool().build())
            .build();
    /**
     * connector state
     */
    public static final Schema TARGET_STATE_V0 = SchemaBuilder.struct()
            .field(FIELD_STATE, SchemaBuilder.string().build())
            .field(FIELD_EPOCH, SchemaBuilder.int64().build())
            .build();
    /**
     * connector configuration
     */
    public static final Schema CONNECTOR_CONFIGURATION_V0 = SchemaBuilder.struct()
            .field(FIELD_STATE, SchemaBuilder.string().build())
            .field(FIELD_EPOCH, SchemaBuilder.int64().build())
            .field(FIELD_PROPS,
                    SchemaBuilder.map(
                            SchemaBuilder.string().optional().build(),
                            SchemaBuilder.string().optional().build()
                    ).build())
            .build();
    /**
     * task configuration
     */
    public static final Schema TASK_CONFIGURATION_V0 = SchemaBuilder.struct()
            .field(FIELD_EPOCH, SchemaBuilder.int64().build())
            .field(FIELD_PROPS,
                    SchemaBuilder.map(
                            SchemaBuilder.string().build(),
                            SchemaBuilder.string().optional().build()
                    ).build())
            .build();
    private static final Logger log = LoggerFactory.getLogger(LoggerName.ROCKETMQ_RUNTIME);
    protected final String configManagePrefix = "ConfigManage";
    /**
     * All listeners to trigger while config change.
     */
    protected Set<ConnectorConfigUpdateListener> connectorConfigUpdateListener;
    /**
     * Synchronize config with other workers.
     */
    protected DataSynchronizer<String, byte[]> dataSynchronizer;
    // config store topic
    protected String topic;
    // converter
    protected RecordConverter converter;
    // worker config
    protected WorkerConfig workerConfig;
    protected Plugin plugin;
    /**
     * Current task configs in the store.
     */
    protected KeyValueStore<String, List<ConnectKeyValue>> taskKeyValueStore;
    /**
     * Current connector configs in the store.
     */
    protected KeyValueStore<String, ConnectKeyValue> connectorKeyValueStore;

    public static String TARGET_STATE_KEY(String connectorName) {
        return TARGET_STATE_PREFIX + connectorName;
    }

    public static String CONNECTOR_KEY(String connectorName) {
        return CONNECTOR_PREFIX + connectorName;
    }

    public static String TASK_KEY(ConnectorTaskId taskId) {
        return TASK_PREFIX + taskId.connector() + "-" + taskId.task();
    }

    public static String DELETE_CONNECTOR_KEY(String connectorName) {
        return DELETE_CONNECTOR_PREFIX + connectorName;
    }

    @Override
    public void initialize(WorkerConfig workerConfig, RecordConverter converter, Plugin plugin) {
        // set config
        this.topic = workerConfig.getConfigStoreTopic();
        this.converter = converter;
        this.converter.configure(new HashMap<>());
        this.plugin = plugin;
        this.connectorConfigUpdateListener = new HashSet<>();
        this.dataSynchronizer = initializationDataSynchronizer(workerConfig);
    }


    @Override
    public boolean enabledCompactTopic() {
        return false;
    }

    @Override
    public void start() {
        dataSynchronizer.start();
    }

    @Override
    public void stop() {
        if (dataSynchronizer != null) {
            dataSynchronizer.stop();
        }
    }

    @Override
    public Map<String, ConnectKeyValue> getConnectorConfigs() {
        return connectorKeyValueStore.getKVMap();
    }

    @Override
    public String putConnectorConfig(String connectorName, ConnectKeyValue configs) {
        // check request config
        for (String requireConfig : ConnectorConfig.REQUEST_CONFIG) {
            if (!configs.containsKey(requireConfig)) {
                throw new ConnectException("Request config key: " + requireConfig);
            }
        }
        // check exist
        ConnectKeyValue oldConfig = connectorKeyValueStore.get(connectorName);
        if (configs.equals(oldConfig)) {
            throw new ConnectException("Connector with same config already exist.");
        }

        // validate config
        Connector connector = plugin.newConnector(configs.getString(CONNECTOR_CLASS));
        if (connector instanceof SourceConnector) {
            new SourceConnectorConfig(configs).validate();
        } else if (connector instanceof SinkConnector) {
            new SinkConnectorConfig(configs).validate();
        }

        // overlap of old config
        connector.validate(configs);
        TargetState state = oldConfig != null ? oldConfig.getTargetState() : TargetState.STARTED;
        configs.setTargetState(state);
        configs.setEpoch(System.currentTimeMillis());

        // new Struct
        Struct connectConfig = new Struct(CONNECTOR_CONFIGURATION_V0);
        connectConfig.put(FIELD_STATE, configs.getTargetState().name());
        connectConfig.put(FIELD_EPOCH, configs.getEpoch());
        connectConfig.put(FIELD_PROPS, configs.getProperties());
        byte[] config = converter.fromConnectData(topic, CONNECTOR_CONFIGURATION_V0, connectConfig);
        notify(CONNECTOR_KEY(connectorName), config);
        return connectorName;
    }

    /**
     * delete config
     *
     * @param connectorName
     */
    @Override
    public void deleteConnectorConfig(String connectorName) {
        if (!connectorKeyValueStore.containsKey(connectorName)) {
            throw new ConnectException("Connector [" + connectorName + "] does not exist");
        }
        // new struct
        Struct struct = new Struct(CONNECTOR_DELETE_CONFIGURATION_V1);
        struct.put(FIELD_EPOCH, System.currentTimeMillis());
        struct.put(FIELD_DELETED, true);
        byte[] config = converter.fromConnectData(topic, CONNECTOR_DELETE_CONFIGURATION_V1, struct);
        notify(DELETE_CONNECTOR_KEY(connectorName), config);
    }

    /**
     * pause connector
     *
     * @param connectorName
     */
    @Override
    public void pauseConnector(String connectorName) {
        if (!connectorKeyValueStore.containsKey(connectorName)) {
            throw new ConnectException("Connector [" + connectorName + "] does not exist");
        }
        Struct connectTargetState = new Struct(TARGET_STATE_V0);
        connectTargetState.put(FIELD_STATE, TargetState.PAUSED.name());
        connectTargetState.put(FIELD_EPOCH, System.currentTimeMillis());
        byte[] serializedTargetState = converter.fromConnectData(topic, TARGET_STATE_V0, connectTargetState);
        log.debug("Writing target state {} for connector {}", TargetState.PAUSED.name(), connectorName);
        notify(TARGET_STATE_KEY(connectorName), serializedTargetState);
    }

    /**
     * resume connector
     *
     * @param connectorName
     */
    @Override
    public void resumeConnector(String connectorName) {
        if (!connectorKeyValueStore.containsKey(connectorName)) {
            throw new ConnectException("Connector [" + connectorName + "] does not exist");
        }
        Struct connectTargetState = new Struct(TARGET_STATE_V0);
        connectTargetState.put(FIELD_STATE, TargetState.STARTED.name());
        connectTargetState.put(FIELD_EPOCH, System.currentTimeMillis());
        byte[] serializedTargetState = converter.fromConnectData(topic, TARGET_STATE_V0, connectTargetState);
        log.debug("Writing target state {} for connector {}", TargetState.STARTED.name(), connectorName);
        notify(TARGET_STATE_KEY(connectorName), serializedTargetState);
    }

    @Override
    public Map<String, List<ConnectKeyValue>> getTaskConfigs() {
        return taskKeyValueStore.getKVMap();
    }

    /**
     * remove and add
     *
     * @param connectorName
     * @param configs
     */
    protected void putTaskConfigs(String connectorName, List<ConnectKeyValue> configs) {
        List<ConnectKeyValue> exist = taskKeyValueStore.get(connectorName);
        if (null != exist && exist.size() > 0) {
            taskKeyValueStore.remove(connectorName);
        }
        taskKeyValueStore.put(connectorName, configs);
    }


    @Override
    public void recomputeTaskConfigs(String connectorName, ConnectKeyValue configs) {
        int maxTask = configs.getInt(ConnectorConfig.MAX_TASK, ConnectorConfig.TASKS_MAX_DEFAULT);
        ConnectKeyValue connectConfig = connectorKeyValueStore.get(connectorName);
        boolean directEnable = Boolean.parseBoolean(connectConfig.getString(ConnectorConfig.CONNECTOR_DIRECT_ENABLE, "false"));
        // load connector
        Connector connector = loadConnector(configs);
        List<KeyValue> taskConfigs = connector.taskConfigs(maxTask);
        if (CollectionUtils.isEmpty(taskConfigs)) {
            throw new ConfigException("The connector  " + connectorName + " taskConfigs is empty, Please check configuration");
        }
        List<ConnectKeyValue> converterdConfigs = new ArrayList<>();
        int taskId = 0;
        for (KeyValue keyValue : taskConfigs) {
            ConnectKeyValue newKeyValue = new ConnectKeyValue();
            newKeyValue.setEpoch(configs.getEpoch());
            for (String key : keyValue.keySet()) {
                newKeyValue.put(key, keyValue.getString(key));
            }
            if (directEnable) {
                newKeyValue.put(ConnectorConfig.TASK_TYPE, Worker.TaskType.DIRECT.name());
                newKeyValue.put(ConnectorConfig.SOURCE_TASK_CLASS, connectConfig.getString(ConnectorConfig.SOURCE_TASK_CLASS));
                newKeyValue.put(ConnectorConfig.SINK_TASK_CLASS, connectConfig.getString(ConnectorConfig.SINK_TASK_CLASS));
            }
            // put task id
            newKeyValue.put(ConnectorConfig.TASK_ID, taskId);
            newKeyValue.put(ConnectorConfig.TASK_CLASS, connector.taskClass().getName());

            // source topic
            if (configs.containsKey(SourceConnectorConfig.CONNECT_TOPICNAME)) {
                newKeyValue.put(SourceConnectorConfig.CONNECT_TOPICNAME, configs.getString(SourceConnectorConfig.CONNECT_TOPICNAME));
            }
            // sink consume topic
            if (configs.containsKey(SinkConnectorConfig.CONNECT_TOPICNAMES)) {
                newKeyValue.put(SinkConnectorConfig.CONNECT_TOPICNAMES, configs.getString(SinkConnectorConfig.CONNECT_TOPICNAMES));
            }

            Set<String> connectConfigKeySet = configs.keySet();
            for (String connectConfigKey : connectConfigKeySet) {
                if (connectConfigKey.startsWith(ConnectorConfig.TRANSFORMS)) {
                    newKeyValue.put(connectConfigKey, configs.getString(connectConfigKey));
                }
            }
            converterdConfigs.add(newKeyValue);
            taskId++;
        }
        putTaskConfigs(connectorName, converterdConfigs);
    }

    @Override
    public void registerListener(ConnectorConfigUpdateListener listener) {
        this.connectorConfigUpdateListener.add(listener);
    }

    @NotNull
    protected Connector loadConnector(ConnectKeyValue configs) {
        String connectorClass = configs.getString(ConnectorConfig.CONNECTOR_CLASS);
        Connector connector = plugin.newConnector(connectorClass);
        connector.validate(configs);
        connector.start(configs);
        return connector;
    }

    @Override
    public ClusterConfigState snapshot() {
        if (taskKeyValueStore == null && connectorKeyValueStore == null) {
            return ClusterConfigState.EMPTY;
        }
        Map<String, Integer> connectorTaskCounts = new HashMap<>();
        Map<ConnectorTaskId, Map<String, String>> connectorTaskConfigs = new ConcurrentHashMap<>();
        taskKeyValueStore.getKVMap().forEach((connectorName, taskConfigs) -> {
            connectorTaskCounts.put(connectorName, taskConfigs.size());
            taskConfigs.forEach(taskConfig -> {
                ConnectorTaskId id = new ConnectorTaskId(connectorName, taskConfig.getInt(ConnectorConfig.TASK_ID));
                connectorTaskConfigs.put(id, taskConfig.getProperties());
            });
        });

        Map<String, Map<String, String>> connectorConfigs = new HashMap<>();
        Map<String, TargetState> connectorTargetStates = new HashMap<>();
        connectorKeyValueStore.getKVMap().forEach((connectorName, taskConfig) -> {
            connectorConfigs.put(connectorName, taskConfig.getProperties());
            connectorTargetStates.put(connectorName, taskConfig.getTargetState());
        });
        return new ClusterConfigState(connectorTaskCounts, connectorConfigs, connectorTargetStates, connectorTaskConfigs);
    }


    @Override
    public Plugin getPlugin() {
        return this.plugin;
    }

    @Override
    public void notify(String key, byte[] value) {
        dataSynchronizer.send(key, value);
    }

    /**
     * trigger listener
     */
    @Override
    public void triggerListener() {
        if (null == this.connectorConfigUpdateListener) {
            return;
        }
        for (ConnectorConfigUpdateListener listener : this.connectorConfigUpdateListener) {
            listener.onConfigUpdate();
        }
    }


    // ======= Start receives the config message and transforms the storage ======

    protected void process(String key, SchemaAndValue schemaAndValue) {
        if (key.startsWith(TARGET_STATE_PREFIX)) {
            // target state listener
            String connectorName = key.substring(TARGET_STATE_PREFIX.length());
            if (schemaAndValue.schema().equals(CONNECTOR_DELETE_CONFIGURATION_V1)) {
                processDeleteConnectorRecord(connectorName, schemaAndValue);
            } else {
                processTargetStateRecord(connectorName, schemaAndValue);
            }
        } else if (key.startsWith(CONNECTOR_PREFIX)) {
            // connector config update
            String connectorName = key.substring(CONNECTOR_PREFIX.length());
            processConnectorConfigRecord(connectorName, schemaAndValue);
        } else if (key.startsWith(TASK_PREFIX)) {
            // task config update
            ConnectorTaskId taskId = parseTaskId(key);
            if (taskId == null) {
                log.error("Ignoring task configuration because {} couldn't be parsed as a task config key", key);
                return;
            }
            processTaskConfigRecord(taskId, schemaAndValue);
        } else if (key.startsWith(DELETE_CONNECTOR_PREFIX)) {
            // delete connector[ Compatible with V0 ]
            String connectorName = key.substring(DELETE_CONNECTOR_PREFIX.length());
            processDeleteConnectorRecord(connectorName, schemaAndValue);
        } else {
            log.warn("Discarding config update record with invalid key: {}", key);
        }
    }

    /**
     * process deleted
     *
     * @param connectorName
     * @param schemaAndValue
     */
    private void processDeleteConnectorRecord(String connectorName, SchemaAndValue schemaAndValue) {
        if (!connectorKeyValueStore.containsKey(connectorName)) {
            return;
        }
        Struct value = (Struct) schemaAndValue.value();
        Object epoch = value.get(FIELD_EPOCH);
        // validate
        ConnectKeyValue oldConfig = connectorKeyValueStore.get(connectorName);
        // config update
        if ((Long) epoch > oldConfig.getEpoch()) {
            // remove
            connectorKeyValueStore.remove(connectorName);
            taskKeyValueStore.remove(connectorName);
            // reblance
            triggerListener();
        }
    }

    /**
     * process task config record
     *
     * @param taskId
     * @param schemaAndValue
     */
    private void processTaskConfigRecord(ConnectorTaskId taskId, SchemaAndValue schemaAndValue) {
        // No-op [Wait for implementation]
    }

    /**
     * process connector config record
     *
     * @param connectorName
     * @param schemaAndValue
     */
    private void processConnectorConfigRecord(String connectorName, SchemaAndValue schemaAndValue) {
        if (mergeConnectConfig(connectorName, schemaAndValue)) {
            // reblance for connector
            triggerListener();
        }

    }

    /**
     * process target state record
     *
     * @param connectorName
     * @param schemaAndValue
     */
    private void processTargetStateRecord(String connectorName, SchemaAndValue schemaAndValue) {
        if (!connectorKeyValueStore.containsKey(connectorName)) {
            return;
        }

        Struct struct = (Struct) schemaAndValue.value();
        Object targetState = struct.get(FIELD_STATE);
        if (!(targetState instanceof String)) {
            // target state
            log.error("Invalid data for target state for connector '{}': 'state' field should be a String but is {}",
                    connectorName, className(targetState));
            return;
        }
        Object epoch = struct.get(FIELD_EPOCH);
        if (!(epoch instanceof Long)) {
            // epoch
            log.error("Invalid data for epoch for connector '{}': 'epoch' field should be a Long but is {}",
                    connectorName, className(epoch));
            return;
        }

        ConnectKeyValue oldConfig = connectorKeyValueStore.get(connectorName);
        // config update
        if ((Long) epoch > oldConfig.getEpoch()) {
            TargetState state = TargetState.valueOf(targetState.toString());
            // remove
            oldConfig.setTargetState(state);
            // reblance
            triggerListener();
        }
    }

    /**
     * Merge new received configs with the configs in memory.
     *
     * @param connectName
     * @param schemaAndValue
     * @return
     */
    private boolean mergeConnectConfig(String connectName, SchemaAndValue schemaAndValue) {
        Struct value = (Struct) schemaAndValue.value();
        Object targetState = value.get(FIELD_STATE);
        if (!(targetState instanceof String)) {
            // target state
            log.error("Invalid data for target state for connector '{}': 'state' field should be a String but is {}",
                    connectName, className(targetState));
            return false;
        }
        Object epoch = value.get(FIELD_EPOCH);
        if (!(epoch instanceof Long)) {
            // epoch
            log.error("Invalid data for epoch for connector '{}': 'state' field should be a long but is {}",
                    connectName, className(epoch));
            return false;
        }
        Object props = value.get(FIELD_PROPS);
        if (!(props instanceof Map)) {
            // properties
            log.error("Invalid data for properties for connector '{}': 'state' field should be a Map but is {}",
                    connectName, className(props));
            return false;
        }
        // new configs
        ConnectKeyValue newConfig = new ConnectKeyValue();
        newConfig.setEpoch((Long) epoch);
        newConfig.setTargetState(TargetState.valueOf((String) targetState));
        newConfig.setProperties((Map<String, String>) props);

        // not exist
        if (!connectorKeyValueStore.containsKey(connectName)) {
            connectorKeyValueStore.put(connectName, newConfig);
            recomputeTaskConfigs(connectName, newConfig);
            return true;
        }

        // exist and update config
        ConnectKeyValue oldConfig = connectorKeyValueStore.get(connectName);
        if (!newConfig.equals(oldConfig)) {
            // compare and swap
            if (newConfig.getEpoch() > oldConfig.getEpoch()) {
                connectorKeyValueStore.put(connectName, newConfig);
                recomputeTaskConfigs(connectName, newConfig);
            }
            return true;
        }
        return false;
    }

    private ConnectorTaskId parseTaskId(String key) {
        String[] parts = key.split("-");
        if (parts.length < 3) {
            return null;
        }

        try {
            int taskNum = Integer.parseInt(parts[parts.length - 1]);
            String connectorName = Utils.join(Arrays.copyOfRange(parts, 1, parts.length - 1), "-");
            return new ConnectorTaskId(connectorName, taskNum);
        } catch (NumberFormatException e) {
            return null;
        }
    }

    private String className(Object o) {
        return o != null ? o.getClass().getName() : "null";
    }

    public class ConfigChangeCallback implements DataSynchronizerCallback<String, byte[]> {
        @Override
        public void onCompletion(Throwable error, String key, byte[] value) {
            if (StringUtils.isEmpty(key)) {
                log.error("Config change message is illegal, key is empty, the message will be skipped");
                return;
            }
            SchemaAndValue schemaAndValue = converter.toConnectData(topic, value);
            process(key, schemaAndValue);
        }
    }

}
