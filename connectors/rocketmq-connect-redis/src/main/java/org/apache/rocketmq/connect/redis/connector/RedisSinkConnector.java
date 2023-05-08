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

package org.apache.rocketmq.connect.redis.connector;

import io.openmessaging.KeyValue;
import io.openmessaging.connector.api.component.task.Task;
import io.openmessaging.connector.api.component.task.sink.SinkConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class RedisSinkConnector extends SinkConnector {
    
    private static final Logger LOGGER = LoggerFactory.getLogger(RedisSinkConnector.class);
    
    private KeyValue connectConfig;
    
    @Override
    public List<KeyValue> taskConfigs(final int maxTasks) {
        LOGGER.info("Starting task config !!! ");
        List<KeyValue> configs = new ArrayList<>();
        for (int i = 0; i < maxTasks; i++) {
            configs.add(this.connectConfig);
        }
        return configs;
    }
    
    @Override
    public Class<? extends Task> taskClass() {
        return RedisSinkTask.class;
    }
    
    @Override
    public void start(final KeyValue keyValue) {
        this.connectConfig = keyValue;
    }
    
    @Override
    public void stop() {
        this.connectConfig = null;
    }
}
