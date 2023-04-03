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

import io.openmessaging.connector.api.component.task.Task;
import io.openmessaging.connector.api.component.task.source.SourceConnector;
import java.util.ArrayList;
import java.util.List;
import io.openmessaging.KeyValue;
import org.apache.rocketmq.connect.redis.common.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RedisSourceConnector extends SourceConnector {

    private static final Logger LOGGER = LoggerFactory.getLogger(RedisSourceConnector.class);

    private Config redisConfig = new Config();

    private KeyValue originalConfig;

    /**
     * Should invoke before start the connector.
     *
     * @param config
     * @return error message
     */
    @Override
    public void validate(KeyValue config) {
        this.redisConfig.load(config);
    }

    @Override public void start(KeyValue config) {
        this.originalConfig = config;

    }

    @Override public void stop() {
        this.redisConfig = null;
    }

    @Override public List<KeyValue> taskConfigs(int maxTasks) {
        List<KeyValue> taskConfigs = new ArrayList<>();
        taskConfigs.add(this.originalConfig);
        return taskConfigs;
    }

    @Override public Class<? extends Task> taskClass() {
        return RedisSourceTask.class;
    }

}
