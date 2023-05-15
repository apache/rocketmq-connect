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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.rocketmq.connect.jdbc.sink;

import io.openmessaging.KeyValue;
import io.openmessaging.connector.api.component.task.Task;
import io.openmessaging.connector.api.component.task.sink.SinkConnector;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * jdbc sink connector
 */
public class JdbcSinkConnector extends SinkConnector {
    private static final Logger log = LoggerFactory.getLogger(JdbcSinkConnector.class);
    private KeyValue connectConfig;

    @Override
    public void start(KeyValue config) {
        this.connectConfig = config;
    }

    /**
     * Should invoke before start the connector.
     *
     * @param config
     * @return error message
     */
    @Override
    public void validate(KeyValue config) {
        // do validate config
    }

    @Override
    public void stop() {
        this.connectConfig = null;
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
        log.info("Starting task config !!! ");
        List<KeyValue> configs = new ArrayList<>();
        for (int i = 0; i < maxTasks; i++) {
            configs.add(this.connectConfig);
        }
        return configs;
    }

    @Override
    public Class<? extends Task> taskClass() {
        return JdbcSinkTask.class;
    }
}
