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

import io.openmessaging.connector.api.component.connector.Connector;
import io.openmessaging.connector.api.component.task.sink.SinkConnector;
import io.openmessaging.connector.api.component.task.source.SourceConnector;
import io.openmessaging.connector.api.data.RecordConverter;
import io.openmessaging.connector.api.data.Schema;
import io.openmessaging.connector.api.data.SchemaAndValue;
import io.openmessaging.connector.api.data.SchemaBuilder;
import io.openmessaging.connector.api.data.Struct;
import io.openmessaging.connector.api.errors.ConnectException;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.connect.runtime.common.ConnAndTaskConfigs;
import org.apache.rocketmq.connect.runtime.common.ConnectKeyValue;
import org.apache.rocketmq.connect.runtime.common.LoggerName;
import org.apache.rocketmq.connect.runtime.config.SinkConnectorConfig;
import org.apache.rocketmq.connect.runtime.config.SourceConnectorConfig;
import org.apache.rocketmq.connect.runtime.config.WorkerConfig;
import org.apache.rocketmq.connect.runtime.config.ConnectorConfig;
import org.apache.rocketmq.connect.runtime.connectorwrapper.TargetState;
import org.apache.rocketmq.connect.runtime.controller.isolation.Plugin;

import org.apache.rocketmq.connect.runtime.serialization.JsonSerde;
import org.apache.rocketmq.connect.runtime.serialization.ListSerde;
import org.apache.rocketmq.connect.runtime.serialization.Serdes;
import org.apache.rocketmq.connect.runtime.store.FileBaseKeyValueStore;
import org.apache.rocketmq.connect.runtime.utils.ConnectUtil;
import org.apache.rocketmq.connect.runtime.utils.ConnectorTaskId;
import org.apache.rocketmq.connect.runtime.utils.FilePathConfigUtil;
import org.apache.rocketmq.connect.runtime.utils.Utils;
import org.apache.rocketmq.connect.runtime.utils.datasync.BrokerBasedLog;
import org.apache.rocketmq.connect.runtime.utils.datasync.DataSynchronizer;
import org.apache.rocketmq.connect.runtime.utils.datasync.DataSynchronizerCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.rocketmq.connect.runtime.config.ConnectorConfig.CONNECTOR_CLASS;

public class ConfigManagementServiceImpl extends AbstractConfigManagementService {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.ROCKETMQ_RUNTIME);

    public static final String START_SIGNAL = "start-signal";

    public static final String TARGET_STATE_PREFIX = "target-state-";

    public static String TARGET_STATE_KEY(String connectorName) {
        return TARGET_STATE_PREFIX + connectorName;
    }

    public static final String CONNECTOR_PREFIX = "connector-";

    public static String CONNECTOR_KEY(String connectorName) {
        return CONNECTOR_PREFIX + connectorName;
    }

    public static final String TASK_PREFIX = "task-";

    public static String TASK_KEY(ConnectorTaskId taskId) {
        return TASK_PREFIX + taskId.connector() + "-" + taskId.task();
    }

    public static final String DELETE_CONNECTOR_PREFIX = "delete-";

    public static String DELETE_CONNECTOR_KEY(String connectorName) {
        return DELETE_CONNECTOR_PREFIX + connectorName;
    }

    private static final String FIELD_STATE = "state";
    private static final String FIELD_EPOCH = "epoch";
    private static final String FIELD_PROPS = "properties";

    /**
     * start signal
     */
    public static final Schema START_SIGNAL_V0 = SchemaBuilder.struct()
            .field(START_SIGNAL, SchemaBuilder.string().build())
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
     * delete connector
     */
    public static final Schema CONNECTOR_DELETE_CONFIGURATION_V0 = SchemaBuilder.struct()
            .field(FIELD_EPOCH, SchemaBuilder.int64().build())
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

    /**
     * connector state
     */
    public static final Schema TARGET_STATE_V0 = SchemaBuilder.struct()
            .field(FIELD_STATE, SchemaBuilder.string().build())
            .field(FIELD_EPOCH, SchemaBuilder.int64().build())
            .build();

    /**
     * All listeners to trigger while config change.
     */
    private Set<ConnectorConfigUpdateListener> connectorConfigUpdateListener;

    /**
     * Synchronize config with other workers.
     */
    private DataSynchronizer<String, byte[]> dataSynchronizer;

    private final String configManagePrefix = "ConfigManage";

    // store topic
    public String topic;
    // converter
    public RecordConverter converter;

    public ConfigManagementServiceImpl() {
    }

    @Override
    public void initialize(WorkerConfig workerConfig, RecordConverter converter, Plugin plugin) {
        // set config
        this.topic = workerConfig.getConfigStoreTopic();
        this.converter = converter;
        this.converter.configure(new HashMap<>());
        this.plugin = plugin;

        this.connectorConfigUpdateListener = new HashSet<>();
        this.dataSynchronizer = new BrokerBasedLog<>(workerConfig,
                this.topic,
                ConnectUtil.createGroupName(configManagePrefix, workerConfig.getWorkerId()),
                new ConfigChangeCallback(),
                Serdes.serdeFrom(String.class),
                Serdes.serdeFrom(byte[].class)
        );

        // store connector config
        this.connectorKeyValueStore = new FileBaseKeyValueStore<>(
                FilePathConfigUtil.getConnectorConfigPath(workerConfig.getStorePathRootDir()),
                new Serdes.StringSerde(),
                new JsonSerde(ConnectKeyValue.class));

        // store task config
        this.taskKeyValueStore = new FileBaseKeyValueStore<>(
                FilePathConfigUtil.getTaskConfigPath(workerConfig.getStorePathRootDir()),
                new Serdes.StringSerde(),
                new ListSerde(ConnectKeyValue.class));

        this.prepare(workerConfig);
    }

    /**
     * Preparation before startup
     *
     * @param connectConfig
     */
    private void prepare(WorkerConfig connectConfig) {
        String configStoreTopic = connectConfig.getConfigStoreTopic();
        if (!ConnectUtil.isTopicExist(connectConfig, configStoreTopic)) {
            log.info("try to create config store topic: {}!", configStoreTopic);
            TopicConfig topicConfig = new TopicConfig(configStoreTopic, 1, 1, 6);
            ConnectUtil.createTopic(connectConfig, topicConfig);
        }
    }

    @Override
    public void start() {
        connectorKeyValueStore.load();
        taskKeyValueStore.load();
        dataSynchronizer.start();
        sendStartSignal();
    }

    private void sendStartSignal() {
        Struct struct = new Struct(START_SIGNAL_V0);
        struct.put(START_SIGNAL, "start");
        dataSynchronizer.send(START_SIGNAL, converter.fromConnectData(topic, START_SIGNAL_V0, struct));
    }

    @Override
    public void stop() {
        triggerSendMessage();
        connectorKeyValueStore.persist();
        taskKeyValueStore.persist();
        dataSynchronizer.stop();
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
        dataSynchronizer.send(CONNECTOR_KEY(connectorName), config);
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
        Struct struct = new Struct(CONNECTOR_DELETE_CONFIGURATION_V0);
        struct.put(FIELD_EPOCH, System.currentTimeMillis());

        byte[] config = converter.fromConnectData(topic, CONNECTOR_DELETE_CONFIGURATION_V0, struct);
        dataSynchronizer.send(DELETE_CONNECTOR_KEY(connectorName), config);
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
        dataSynchronizer.send(TARGET_STATE_KEY(connectorName), serializedTargetState);
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
        dataSynchronizer.send(TARGET_STATE_KEY(connectorName), serializedTargetState);
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
    @Override
    protected void putTaskConfigs(String connectorName, List<ConnectKeyValue> configs) {
        List<ConnectKeyValue> exist = taskKeyValueStore.get(connectorName);
        if (null != exist && exist.size() > 0) {
            taskKeyValueStore.remove(connectorName);
        }
        taskKeyValueStore.put(connectorName, configs);
    }

    @Override
    public void persist() {
        this.connectorKeyValueStore.persist();
        this.taskKeyValueStore.persist();
    }

    @Override
    public void registerListener(ConnectorConfigUpdateListener listener) {
        this.connectorConfigUpdateListener.add(listener);
    }

    /**
     * trigger listener
     */
    private void triggerListener() {
        if (null == this.connectorConfigUpdateListener) {
            return;
        }
        for (ConnectorConfigUpdateListener listener : this.connectorConfigUpdateListener) {
            listener.onConfigUpdate();
        }
    }

    /**
     * send all connector config
     */
    private void triggerSendMessage() {
        ConnAndTaskConfigs configs = new ConnAndTaskConfigs();
        configs.setConnectorConfigs(connectorKeyValueStore.getKVMap());
        connectorKeyValueStore.getKVMap().forEach((connectName, connectKeyValue) -> {
            Struct struct = new Struct(CONNECTOR_CONFIGURATION_V0)
                    .put(FIELD_EPOCH, connectKeyValue.getEpoch())
                    .put(FIELD_STATE, connectKeyValue.getTargetState().name())
                    .put(FIELD_PROPS, connectKeyValue.getProperties());
            byte[] body = converter.fromConnectData(topic, CONNECTOR_CONFIGURATION_V0, struct);
            dataSynchronizer.send(CONNECTOR_KEY(connectName), body);
        });

        taskKeyValueStore.getKVMap().forEach((connectName, taskConfigs) -> {
            if (taskConfigs == null || taskConfigs.isEmpty()) {
                return;
            }
            taskConfigs.forEach(taskConfig -> {
                ConnectorTaskId taskId = new ConnectorTaskId(connectName, taskConfig.getInt(ConnectorConfig.TASK_ID));
                Struct struct = new Struct(TASK_CONFIGURATION_V0)
                        .put(FIELD_EPOCH, System.currentTimeMillis())
                        .put(FIELD_PROPS, taskConfig.getProperties());
                byte[] body = converter.fromConnectData(topic, TASK_CONFIGURATION_V0, struct);
                dataSynchronizer.send(TASK_KEY(taskId), body);
            });
        });
    }

    private class ConfigChangeCallback implements DataSynchronizerCallback<String, byte[]> {
        @Override
        public void onCompletion(Throwable error, String key, byte[] value) {
            if (StringUtils.isEmpty(key)) {
                log.error("Config change message is illegal, key is empty, the message will be skipped");
                return;
            }
            SchemaAndValue schemaAndValue = converter.toConnectData(topic, value);
            if (key.equals(START_SIGNAL)) {
                // send message in full
                triggerSendMessage();
                // reblance
                triggerListener();
            } else if (key.startsWith(TARGET_STATE_PREFIX)) {
                // target state listener
                String connectorName = key.substring(TARGET_STATE_PREFIX.length());
                processTargetStateRecord(connectorName, schemaAndValue);
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
                // delete connector
                String connectorName = key.substring(DELETE_CONNECTOR_PREFIX.length());
                processDeleteConnectorRecord(connectorName, schemaAndValue);

            } else {
                log.error("Discarding config update record with invalid key: {}", key);
            }
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
        // No-op
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

    @Override
    public Plugin getPlugin() {
        return this.plugin;
    }

    @Override
    public StagingMode getStagingMode() {
        return StagingMode.DISTRIBUTED;
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
}