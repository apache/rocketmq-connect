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

package org.apache.rocketmq.connect.kafka.connect.adaptor.connector;

import io.openmessaging.KeyValue;
import io.openmessaging.connector.api.component.task.sink.SinkConnector;
import io.openmessaging.connector.api.errors.ConnectException;
import io.openmessaging.internal.DefaultKeyValue;
import org.apache.kafka.connect.runtime.ConnectorConfig;
import org.apache.kafka.connect.runtime.TaskConfig;
import org.apache.rocketmq.connect.kafka.connect.adaptor.config.ConnectKeyValue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * kafka source connector
 */
public abstract class AbstractKafkaSinkConnector extends SinkConnector implements ConnectorClassSetter {

    /**
     * kafka connect init
     */
    protected ConnectKeyValue configValue;

    /**
     * source connector
     */
    protected org.apache.kafka.connect.sink.SinkConnector sinkConnector;

    /**
     * task config
     */
    protected Map<String, String> taskConfig;

    /**
     * try override start and stop
     *
     * @return
     */
    protected org.apache.kafka.connect.sink.SinkConnector originalSinkConnector() {
        return sinkConnector;
    }

    /**
     * Returns a set of configurations for Tasks based on the current configuration,
     * producing at most count configurations.
     *
     * @param maxTasks maximum number of configurations to generate
     * @return configurations for Tasks
     */
    @Override
    public List<KeyValue> taskConfigs(int maxTasks) {
        List<Map<String, String>> groupConnectors = sinkConnector.taskConfigs(maxTasks);
        List<KeyValue> configs = new ArrayList<>();
        for (Map<String, String> configMaps : groupConnectors) {
            KeyValue keyValue = new DefaultKeyValue();
            configMaps.forEach((k, v) -> {
                keyValue.put(k, v);
            });
            configs.add(keyValue);
        }
        return configs;
    }

    /**
     * Start the component
     *
     * @param config component context
     */
    @Override
    public void start(KeyValue config) {
        this.configValue = new ConnectKeyValue();
        config.keySet().forEach(key -> {
            this.configValue.put(key, config.getString(key));
        });
        setConnectorClass(configValue);
        taskConfig = new HashMap<>(configValue.config());
        // get the source class name from config and create source task from reflection
        try {
            sinkConnector = Class.forName(taskConfig.get(ConnectorConfig.CONNECTOR_CLASS_CONFIG))
                    .asSubclass(org.apache.kafka.connect.sink.SinkConnector.class)
                    .getDeclaredConstructor()
                    .newInstance();
        } catch (Exception e) {
            throw new ConnectException("Load task class failed, " + taskConfig.get(TaskConfig.TASK_CLASS_CONFIG));
        }
    }

    /**
     * Stop the component.
     */
    @Override
    public void stop() {
        if (sinkConnector != null) {
            sinkConnector = null;
            configValue = null;
            taskConfig = null;
        }
    }
}
