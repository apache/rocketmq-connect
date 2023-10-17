#set( $symbol_pound = '#' )
#set( $symbol_dollar = '$' )
#set( $symbol_escape = '\' )
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

package ${package}.sink;

import io.openmessaging.KeyValue;
import io.openmessaging.connector.api.component.task.Task;
import io.openmessaging.connector.api.component.task.sink.SinkConnector;
import java.util.ArrayList;
import java.util.List;
import ${package}.config.${dbNameToCamel}SinkConfig;

public class ${dbNameToCamel}SinkConnector extends SinkConnector {

    private KeyValue keyValue;

    @Override public List<KeyValue> taskConfigs(int maxTasks) {
        List<KeyValue> configs = new ArrayList<>();
        for (int i = 0; i < maxTasks; i++) {
            configs.add(this.keyValue);
        }
        return configs;
    }

    @Override public Class<? extends Task> taskClass() {
        return ${dbNameToCamel}SinkTask.class;
    }

    @Override public void start(KeyValue value) {

        for (String requestKey : ${dbNameToCamel}SinkConfig.SINK_REQUEST_CONFIG) {
            if (!value.containsKey(requestKey)) {
                throw new RuntimeException("Request config key: " + requestKey);
            }
        }

        this.keyValue = value;
    }

    @Override public void stop() {
        this.keyValue = null;
    }
}
