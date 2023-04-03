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

package org.apache.rocketmq.connect.jms.connector;

import io.openmessaging.connector.api.component.task.Task;
import io.openmessaging.connector.api.component.task.source.SourceConnector;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import io.openmessaging.KeyValue;

public abstract class BaseJmsSourceConnector extends SourceConnector {

    protected KeyValue config;

    @Override
    public void start(KeyValue config) {
        for (String requestKey : getRequiredConfig()) {
            if (!config.containsKey(requestKey)) {
                throw new RuntimeException("Request config key: " + requestKey);
            }
        }
        this.config = config;
    }

    @Override
    public void stop() {
        this.config = null;
    }


    @Override
    public abstract Class<? extends Task> taskClass();

    @Override
    public List<KeyValue> taskConfigs(int maxTask) {
        List<KeyValue> configs = new ArrayList<>();
        for (int i = 0; i < maxTask; i++) {
            configs.add(this.config);
        }
        return configs;
    }
    
    public abstract Set<String> getRequiredConfig();
}
