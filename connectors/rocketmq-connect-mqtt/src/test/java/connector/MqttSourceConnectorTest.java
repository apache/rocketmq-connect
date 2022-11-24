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

package connector;

import io.openmessaging.KeyValue;
import io.openmessaging.internal.DefaultKeyValue;
import org.apache.rocketmq.connect.mqtt.config.SourceConnectorConfig;
import org.apache.rocketmq.connect.mqtt.connector.MqttSourceConnector;
import org.apache.rocketmq.connect.mqtt.connector.MqttSourceTask;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class MqttSourceConnectorTest {

    MqttSourceConnector connector = new MqttSourceConnector();

    @Test
    public void taskClassTest() {
        assertEquals(connector.taskClass(), MqttSourceTask.class);
    }

    @Test
    public void taskConfigsTest() {
        assertEquals(connector.taskConfigs(2).get(0), null);
        KeyValue keyValue = new DefaultKeyValue();
        for (String requestKey : SourceConnectorConfig.REQUEST_CONFIG) {
            keyValue.put(requestKey, requestKey);
        }
        connector.start(keyValue);
        assertEquals(connector.taskConfigs(2).get(0), keyValue);
    }
}
