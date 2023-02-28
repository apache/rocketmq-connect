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
package org.apache.rocketmq.connect.runtime.service.local;

import io.openmessaging.connector.api.data.RecordConverter;
import io.openmessaging.connector.api.data.Schema;
import io.openmessaging.connector.api.data.SchemaBuilder;
import io.openmessaging.connector.api.data.Struct;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.connect.runtime.common.ConnAndTaskStatus;
import org.apache.rocketmq.connect.runtime.config.WorkerConfig;
import org.apache.rocketmq.connect.runtime.connectorwrapper.status.ConnectorStatus;
import org.apache.rocketmq.connect.runtime.connectorwrapper.status.TaskStatus;
import org.apache.rocketmq.connect.runtime.serialization.JsonSerde;
import org.apache.rocketmq.connect.runtime.serialization.ListSerde;
import org.apache.rocketmq.connect.runtime.serialization.Serdes;
import org.apache.rocketmq.connect.runtime.service.AbstractStateManagementService;
import org.apache.rocketmq.connect.runtime.store.FileBaseKeyValueStore;
import org.apache.rocketmq.connect.runtime.utils.ConnectUtil;
import org.apache.rocketmq.connect.runtime.utils.FilePathConfigUtil;
import org.apache.rocketmq.connect.runtime.utils.datasync.BrokerBasedLog;
import org.apache.rocketmq.connect.runtime.utils.datasync.DataSynchronizer;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Local state management service
 */
public class LocalStateManagementServiceImpl extends AbstractStateManagementService {


    public static final String START_SIGNAL = "start-signal";
    /**
     * start signal
     */
    private static final Schema START_SIGNAL_V0 = SchemaBuilder.struct()
            .field(START_SIGNAL, SchemaBuilder.string().build())
            .build();
    /**
     * initialize cb config
     *
     * @param config
     */
    @Override
    public void initialize(WorkerConfig config, RecordConverter converter) {
        super.initialize(config, converter);
        /**connector status store*/
        this.connectorStatusStore = new FileBaseKeyValueStore<>(
                FilePathConfigUtil.getConnectorStatusConfigPath(config.getStorePathRootDir()),
            new Serdes.StringSerde(),
            new JsonSerde(ConnectorStatus.class));

        /**task status store*/
        this.taskStatusStore = new FileBaseKeyValueStore<>(
            FilePathConfigUtil.getTaskStatusConfigPath(config.getStorePathRootDir()),
            new Serdes.StringSerde(),
            new ListSerde(TaskStatus.class));
    }

    @Override
    public DataSynchronizer initializationDataSynchronizer(WorkerConfig config) {
        return new BrokerBasedLog(config,
            statusTopic,
            ConnectUtil.createGroupName(statusManagePrefix, config.getWorkerId()),
            new StatusChangeCallback(),
            Serdes.serdeFrom(String.class),
            Serdes.serdeFrom(byte[].class),
            enabledCompactTopic()
        );
    }

    /**
     * Start dependent services (if needed)
     */
    @Override
    public void start() {
        connectorStatusStore.load();
        taskStatusStore.load();
        dataSynchronizer.start();
        startSignal();
    }

    private void startSignal() {
        Struct struct = new Struct(START_SIGNAL_V0);
        struct.put(START_SIGNAL, START_SIGNAL);
        dataSynchronizer.send(START_SIGNAL, converter.fromConnectData(statusTopic, START_SIGNAL_V0, struct));
    }

    /**
     * Stop dependent services (if needed)
     */
    @Override
    public void stop() {
        replicaTargetState();
        this.persist();
        dataSynchronizer.stop();
    }

    @Override
    public void persist() {
        prePersist();
        connectorStatusStore.persist();
        taskStatusStore.persist();
    }

    /**
     * sync send online config
     */
    @Override
    public void replicaTargetState() {
        /** connector status store*/
        Map<String, ConnectorStatus> connectorStatusMap = connectorStatusStore.getKVMap();
        connectorStatusMap.forEach((connectorName, connectorStatus) -> {
            if (connectorStatus == null) {
                return;
            }
            // send status
            put(connectorStatus);
        });

        /** task status store */
        Map<String, List<TaskStatus>> taskStatusMap = taskStatusStore.getKVMap();
        if (taskStatusMap.isEmpty()) {
            return;
        }
        taskStatusMap.forEach((connectorName, taskStatusList) -> {
            if (taskStatusList == null || taskStatusList.isEmpty()) {
                return;
            }
            taskStatusList.forEach(taskStatus -> {
                // send status
                put(taskStatus);
            });
        });
    }

    @Override
    protected void process(String key, byte[] value) {
        if (StringUtils.isEmpty(key)) {
            log.error("State change message is illegal, key is empty, the message will be skipped ");
            return;
        }
        if (key.equals(START_SIGNAL)) {
            replicaTargetState();
        } else if (key.startsWith(CONNECTOR_STATUS_PREFIX)) {
            readConnectorStatus(key, value);
        } else if (key.startsWith(TASK_STATUS_PREFIX)) {
            readTaskStatus(key, value);
        } else {
            log.warn("Discarding record with invalid key {}", key);
        }
    }

    /**
     * pre persist
     */
    private void prePersist() {
        Map<String, ConnAndTaskStatus.CacheEntry<ConnectorStatus>> connectors = connAndTaskStatus.getConnectors();
        if (connectors.isEmpty()) {
            return;
        }
        connectors.forEach((connectName, connectorStatus) -> {
            connectorStatusStore.put(connectName, connectorStatus.get());
            Map<Integer, ConnAndTaskStatus.CacheEntry<TaskStatus>> cacheTaskStatus = connAndTaskStatus.getTasks().row(connectName);
            if (cacheTaskStatus == null) {
                return;
            }
            taskStatusStore.put(connectName, new ArrayList<>());
            cacheTaskStatus.forEach((taskId, taskStatus) -> {
                if (taskStatus != null) {
                    taskStatusStore.get(connectName).add(taskStatus.get());
                }
            });
        });
    }

}