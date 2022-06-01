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

package org.apache.rocketmq.connect.debezium;

import io.openmessaging.KeyValue;
import io.openmessaging.connector.api.component.task.Task;
import io.openmessaging.connector.api.component.task.sink.SinkConnector;

import java.util.Collections;
import java.util.List;


/**
 * debezium connector
 */
public abstract class DebeziumConnector extends SinkConnector {

    protected KeyValue config;

    /**
     * Returns a set of configurations for Tasks based on the current configuration,
     * producing at most count configurations.
     * @param maxTasks maximum number of configurations to generate
     * @return configurations for Tasks
     */
    @Override
    public List<KeyValue> taskConfigs(int maxTasks) {
        if (maxTasks > 1) {
            throw new IllegalArgumentException("Only a single connector task may be started");
        }
        return Collections.singletonList(config);
    }

    /**
     * Init the component
     * @param config component config
     */
    @Override
    public void init(KeyValue config) {
        this.config = config;
    }

    /**
     * Stop the component.
     */
    @Override
    public void stop() {
        this.config = null;
    }

    /**
     * Pause the connector.
     */
    @Override
    public void pause() {

    }

    /**
     * Resume the connector.
     */
    @Override
    public void resume() {

    }
}
