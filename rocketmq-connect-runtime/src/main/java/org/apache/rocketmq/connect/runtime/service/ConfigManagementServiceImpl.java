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

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import io.openmessaging.connector.api.errors.ConnectException;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.connect.runtime.common.ConnAndTaskConfigs;
import org.apache.rocketmq.connect.runtime.common.ConnectKeyValue;
import org.apache.rocketmq.connect.runtime.common.LoggerName;
import org.apache.rocketmq.connect.runtime.config.ConnectConfig;
import org.apache.rocketmq.connect.runtime.config.RuntimeConfigDefine;
import org.apache.rocketmq.connect.runtime.connectorwrapper.TargetState;
import org.apache.rocketmq.connect.runtime.converter.ConnAndTaskConfigConverter;
import org.apache.rocketmq.connect.runtime.converter.ConnectKeyValueConverter;
import org.apache.rocketmq.connect.runtime.converter.JsonConverter;
import org.apache.rocketmq.connect.runtime.converter.ListConverter;
import org.apache.rocketmq.connect.runtime.store.FileBaseKeyValueStore;
import org.apache.rocketmq.connect.runtime.store.KeyValueStore;
import org.apache.rocketmq.connect.runtime.utils.ConnectUtil;
import org.apache.rocketmq.connect.runtime.utils.ConnectorTaskId;
import org.apache.rocketmq.connect.runtime.utils.FilePathConfigUtil;
import org.apache.rocketmq.connect.runtime.controller.isolation.Plugin;
import org.apache.rocketmq.connect.runtime.utils.Utils;
import org.apache.rocketmq.connect.runtime.utils.datasync.BrokerBasedLog;
import org.apache.rocketmq.connect.runtime.utils.datasync.DataSynchronizer;
import org.apache.rocketmq.connect.runtime.utils.datasync.DataSynchronizerCallback;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConfigManagementServiceImpl extends AbstractConfigManagementService {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.ROCKETMQ_RUNTIME);

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

    /**
     * All listeners to trigger while config change.
     */
    private Set<ConnectorConfigUpdateListener> connectorConfigUpdateListener;

    /**
     * Synchronize config with other workers.
     */
    private DataSynchronizer<String, ConnectKeyValue> dataSynchronizer;

    private final String configManagePrefix = "ConfigManage";

    @Override public void initialize(ConnectConfig connectConfig, Plugin plugin) {
        this.connectorConfigUpdateListener = new HashSet<>();
        this.dataSynchronizer = new BrokerBasedLog<>(connectConfig,
                connectConfig.getConfigStoreTopic(),
                ConnectUtil.createGroupName(configManagePrefix, connectConfig.getWorkerId()),
                new ConfigChangeCallback(),
                new JsonConverter(),
                new ConnectKeyValueConverter());
        this.connectorKeyValueStore = new FileBaseKeyValueStore<>(
                FilePathConfigUtil.getConnectorConfigPath(connectConfig.getStorePathRootDir()),
                new JsonConverter(),
                new JsonConverter(ConnectKeyValue.class));
        this.taskKeyValueStore = new FileBaseKeyValueStore<>(
                FilePathConfigUtil.getTaskConfigPath(connectConfig.getStorePathRootDir()),
                new JsonConverter(),
                new ListConverter(ConnectKeyValue.class));
        this.plugin = plugin;
        this.prepare(connectConfig);
    }



    /**
     * Preparation before startup
     * @param connectConfig
     */
    private void prepare(ConnectConfig connectConfig) {
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
        triggerSendMessage();
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
    public String putConnectorConfig(String connectorName, ConnectKeyValue configs) throws Exception {
        ConnectKeyValue exist = connectorKeyValueStore.get(connectorName);
        // update version
        if (null != exist) {
            Long updateTimestamp = exist.getLong(RuntimeConfigDefine.UPDATE_TIMESTAMP);
            if (null != updateTimestamp) {
                configs.put(RuntimeConfigDefine.UPDATE_TIMESTAMP, updateTimestamp);
            }
        }

        if (configs.equals(exist)) {
            throw new ConnectException("Connector with same config already exist.");
        }

        for (String requireConfig : RuntimeConfigDefine.REQUEST_CONFIG) {
            if (!configs.containsKey(requireConfig)) {
                throw new ConnectException("Request config key: " + requireConfig);
            }
        }
        configs.put(RuntimeConfigDefine.UPDATE_TIMESTAMP, System.currentTimeMillis());
        configs.setTargetState(TargetState.STARTED);
        dataSynchronizer.send(CONNECTOR_KEY(connectorName), configs);
        return connectorName;
    }



    @Override
    public void recomputeTaskConfigs(String connectorName, Connector connector, Long currentTimestamp, ConnectKeyValue configs) {
        super.recomputeTaskConfigs(connectorName, connector, currentTimestamp, configs);
    }

    /**
     * delete config
     * @param connectorName
     */
    @Override
    public void deleteConnectorConfig(String connectorName) {
        // copy and send
        ConnectKeyValue deleteConfig = copyAndWriteNewConfig(connectorName);
        dataSynchronizer.send(DELETE_CONNECTOR_KEY(connectorName), deleteConfig);
    }

    /**
     * pause connector
     *
     * @param connectorName
     */
    @Override
    public void pauseConnector(String connectorName) {
        ConnectKeyValue pauseConfig = copyAndWriteNewConfig(connectorName);
        pauseConfig.setTargetState(TargetState.PAUSED);
        dataSynchronizer.send(TARGET_STATE_KEY(connectorName), pauseConfig);
    }

    /**
     * resume connector
     * @param connectorName
     */
    @Override
    public void resumeConnector(String connectorName) {
        ConnectKeyValue resumeConfig = copyAndWriteNewConfig(connectorName);
        resumeConfig.setTargetState(TargetState.STARTED);
        dataSynchronizer.send(TARGET_STATE_KEY(connectorName), resumeConfig);
    }


    /**
     * copy and write new config
     * @param connectorName
     * @return
     */
    private ConnectKeyValue copyAndWriteNewConfig(String connectorName){
        if (!connectorKeyValueStore.containsKey(connectorName)) {
            throw new ConnectException("Connector ["+connectorName+"] does not exist");
        }

        ConnectKeyValue config = connectorKeyValueStore.get(connectorName);
        // copy old config
        ConnectKeyValue newConfig = new ConnectKeyValue();
        newConfig.setProperties(new HashMap<>(config.getProperties()));
        newConfig.setTargetState(config.getTargetState());
        // update version by timestamp
        newConfig.put(RuntimeConfigDefine.UPDATE_TIMESTAMP, System.currentTimeMillis());
        return newConfig;
    }

    @Override
    public Map<String, List<ConnectKeyValue>> getTaskConfigs() {
        return taskKeyValueStore.getKVMap();
    }

    /**
     * remove and add
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
        connectorKeyValueStore.getKVMap().forEach((connectName, connectKeyValue)->{
            dataSynchronizer.send(CONNECTOR_KEY(connectName), connectKeyValue);
        });
        taskKeyValueStore.getKVMap().forEach((connectName,taskConfigs)->{
            taskConfigs.forEach(taskConfig->{
                ConnectorTaskId taskId = new ConnectorTaskId(connectName, taskConfig.getInt(RuntimeConfigDefine.TASK_ID));
                dataSynchronizer.send(TASK_KEY(taskId), taskConfig);
            });
        });
    }


    private class ConfigChangeCallback implements DataSynchronizerCallback<String, ConnectKeyValue> {
        @Override
        public void onCompletion(Throwable error, String key, ConnectKeyValue value) {
            // target state listener
            if (key.startsWith(TARGET_STATE_PREFIX)) {
                String connectorName = key.substring(TARGET_STATE_PREFIX.length());
                processTargetStateRecord(connectorName, value);
            } else if (key.startsWith(CONNECTOR_PREFIX)) {
                // connector config update
                String connectorName = key.substring(CONNECTOR_PREFIX.length());
                processConnectorConfigRecord(connectorName, value);
            } else if (key.startsWith(TASK_PREFIX)) {
                // task config update
                ConnectorTaskId taskId = parseTaskId(key);
                if (taskId == null) {
                    log.error("Ignoring task configuration because {} couldn't be parsed as a task config key", key);
                    return;
                }
                processTaskConfigRecord(taskId, value);
            }else if (key.startsWith(DELETE_CONNECTOR_PREFIX)){
                // delete connector
                String connectorName = key.substring(DELETE_CONNECTOR_PREFIX.length());
                processDeleteConnectorRecord(connectorName, value);
            } else {
                log.error("Discarding config update record with invalid key: {}", key);
            }
        }
    }

    /**
     * process deleted
     * @param connectorName
     * @param value
     */
    private void processDeleteConnectorRecord(String connectorName, ConnectKeyValue value) {
        if (!connectorKeyValueStore.containsKey(connectorName)){
            return;
        }
        ConnectKeyValue oldConfig = connectorKeyValueStore.get(connectorName);
        // config update
        if (!value.equals(oldConfig)){
            Long oldUpdateTime = oldConfig.getLong(RuntimeConfigDefine.UPDATE_TIMESTAMP);
            Long newUpdateTime = value.getLong(RuntimeConfigDefine.UPDATE_TIMESTAMP);
            if (newUpdateTime > oldUpdateTime) {
                // remove
                connectorKeyValueStore.remove(connectorName);
                taskKeyValueStore.remove(connectorName);
                // reblance
                triggerListener();
            }
        }
    }


    /**
     * process task config record
     * @param taskId
     * @param value
     */
    private void processTaskConfigRecord(ConnectorTaskId taskId, ConnectKeyValue value) {
        // No-op
    }

    /**
     * process connector config record
     * @param connectorName
     * @param value
     */
    private void processConnectorConfigRecord(String connectorName, ConnectKeyValue value) {
        if (mergeConnectConfig(connectorName, value)){
            // reblance
            triggerListener();
        }

    }

    /**
     * process target state record
     * @param connectorName
     * @param value
     */
    private void processTargetStateRecord(String connectorName, ConnectKeyValue value) {
        if (!connectorKeyValueStore.containsKey(connectorName)){
            return;
        }
        ConnectKeyValue oldConfig = connectorKeyValueStore.get(connectorName);
        // config update
        if (!value.equals(oldConfig)){
            Long oldUpdateTime = oldConfig.getLong(RuntimeConfigDefine.UPDATE_TIMESTAMP);
            Long newUpdateTime = value.getLong(RuntimeConfigDefine.UPDATE_TIMESTAMP);
            if (newUpdateTime > oldUpdateTime) {
                // remove
                oldConfig.setTargetState(value.getTargetState());
                // reblance
                triggerListener();
            }
        }
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

    /**
     * Merge new received configs with the configs in memory.
     * @param connectName
     * @param connectKeyValue
     * @return
     */
    private boolean mergeConnectConfig(String connectName, ConnectKeyValue connectKeyValue) {
        if (!connectorKeyValueStore.containsKey(connectName)){
            connectorKeyValueStore.put(connectName, connectKeyValue);
            try {
                recomputeTaskConfigs(connectName, loadConnector(connectKeyValue), connectKeyValue.getLong(RuntimeConfigDefine.UPDATE_TIMESTAMP), connectKeyValue);
            } catch (Exception e) {
                e.printStackTrace();
            }
            return true;
        }
        ConnectKeyValue oldConfig = connectorKeyValueStore.get(connectName);
        // config update
        if (!connectKeyValue.equals(oldConfig)){
            Long oldUpdateTime = oldConfig.getLong(RuntimeConfigDefine.UPDATE_TIMESTAMP);
            Long newUpdateTime = connectKeyValue.getLong(RuntimeConfigDefine.UPDATE_TIMESTAMP);
            if (newUpdateTime > oldUpdateTime) {
                connectorKeyValueStore.put(connectName, connectKeyValue);
                try {
                    recomputeTaskConfigs(connectName, loadConnector(connectKeyValue),newUpdateTime, connectKeyValue);
                } catch (Exception e) {
                    throw new ConnectException(e);
                }
            }
            return true;
        }
        return  false;
    }

    @Override
    public Plugin getPlugin() {
        return this.plugin;
    }

    @Override public StagingMode getStagingMode() {
        return StagingMode.DISTRIBUTED;
    }
}
