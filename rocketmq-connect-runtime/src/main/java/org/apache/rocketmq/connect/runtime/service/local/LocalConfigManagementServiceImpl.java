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

package org.apache.rocketmq.connect.runtime.service.local;

import io.openmessaging.connector.api.data.RecordConverter;
import io.openmessaging.connector.api.data.Schema;
import io.openmessaging.connector.api.data.SchemaAndValue;
import io.openmessaging.connector.api.data.SchemaBuilder;
import io.openmessaging.connector.api.data.Struct;
import org.apache.rocketmq.connect.runtime.common.ConnAndTaskConfigs;
import org.apache.rocketmq.connect.runtime.common.ConnectKeyValue;
import org.apache.rocketmq.connect.runtime.config.ConnectorConfig;
import org.apache.rocketmq.connect.runtime.config.WorkerConfig;
import org.apache.rocketmq.connect.runtime.controller.isolation.Plugin;

import org.apache.rocketmq.connect.runtime.serialization.JsonSerde;
import org.apache.rocketmq.connect.runtime.serialization.ListSerde;
import org.apache.rocketmq.connect.runtime.serialization.Serdes;
import org.apache.rocketmq.connect.runtime.service.AbstractConfigManagementService;
import org.apache.rocketmq.connect.runtime.store.FileBaseKeyValueStore;
import org.apache.rocketmq.connect.runtime.utils.ConnectUtil;
import org.apache.rocketmq.connect.runtime.utils.ConnectorTaskId;
import org.apache.rocketmq.connect.runtime.utils.FilePathConfigUtil;
import org.apache.rocketmq.connect.runtime.utils.datasync.BrokerBasedLog;
import org.apache.rocketmq.connect.runtime.utils.datasync.DataSynchronizer;

/**
 * Local config management service impl
 */
public class LocalConfigManagementServiceImpl extends AbstractConfigManagementService {


    public static final String START_SIGNAL = "start-signal";
    public static final String START_SIGNAL_VALUE = "start";

    /**
     * start signal
     */
    public static final Schema START_SIGNAL_V0 = SchemaBuilder.struct()
            .field(START_SIGNAL, SchemaBuilder.string().build())
            .build();

    public LocalConfigManagementServiceImpl() {}

    @Override
    public void initialize(WorkerConfig workerConfig, RecordConverter converter, Plugin plugin) {
        super.initialize(workerConfig, converter, plugin);

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

    }

    @Override
    public DataSynchronizer initializationDataSynchronizer(WorkerConfig workerConfig) {
        return new BrokerBasedLog<>(workerConfig,
            this.topic,
            ConnectUtil.createGroupName(configManagePrefix, workerConfig.getWorkerId()),
            new ConfigChangeCallback(),
            Serdes.serdeFrom(String.class),
            Serdes.serdeFrom(byte[].class),
            enabledCompactTopic()
        );
    }


    @Override
    public void start() {
        connectorKeyValueStore.load();
        taskKeyValueStore.load();
        super.start();
        sendStartSignal();
    }

    private void sendStartSignal() {
        Struct struct = new Struct(START_SIGNAL_V0);
        struct.put(START_SIGNAL, START_SIGNAL_VALUE);
        notify(START_SIGNAL, converter.fromConnectData(topic, START_SIGNAL_V0, struct));
    }

    @Override
    public void stop() {
        triggerSendMessage();
        if (connectorKeyValueStore != null) {
            connectorKeyValueStore.persist();
        }
        if (taskKeyValueStore != null) {
            taskKeyValueStore.persist();
        }
        super.stop();
    }

    @Override
    public void persist() {
        this.connectorKeyValueStore.persist();
        this.taskKeyValueStore.persist();
    }

    @Override
    protected void process(String key, SchemaAndValue schemaAndValue) {
        if (key.equals(START_SIGNAL)) {
            // send message in full
            triggerSendMessage();
        } else {
            super.process(key, schemaAndValue);
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
            notify(CONNECTOR_KEY(connectName), body);
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
                notify(TASK_KEY(taskId), body);
            });
        });
    }

}