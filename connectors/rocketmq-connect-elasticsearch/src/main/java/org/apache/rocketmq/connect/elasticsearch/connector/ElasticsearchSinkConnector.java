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

package org.apache.rocketmq.connect.elasticsearch.connector;

import io.openmessaging.KeyValue;
import io.openmessaging.connector.api.component.task.Task;
import io.openmessaging.connector.api.component.task.sink.SinkConnector;
import java.util.ArrayList;
import java.util.List;
import org.apache.rocketmq.connect.elasticsearch.config.ElasticsearchConfig;

public class ElasticsearchSinkConnector extends SinkConnector {

    private KeyValue config;

    @Override
    public void validate(KeyValue config) {
        for (String requestKey : ElasticsearchConfig.REQUEST_CONFIG_SINK) {
            if (!config.containsKey(requestKey)) {
                throw new RuntimeException("Request config key: " + requestKey);
            }
        }
    }

    @Override
    public List<KeyValue> taskConfigs(int maxTasks) {
        if (maxTasks == 0) {
            maxTasks = 1;
        }
        List<KeyValue> configs = new ArrayList<>();
        for (int i = 0; i < maxTasks; i++) {
            configs.add(this.config);
        }
        return configs;
    }

    @Override
    public Class<? extends Task> taskClass() {
        return ElasticsearchSinkTask.class;
    }

    @Override
    public void start(KeyValue config) {
        this.config = config;
    }

    @Override
    public void stop() {
        this.config = null;
    }
}
