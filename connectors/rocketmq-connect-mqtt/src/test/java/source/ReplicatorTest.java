package source;/*
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

import org.apache.rocketmq.connect.mqtt.config.SourceConnectorConfig;
import org.apache.rocketmq.connect.mqtt.source.Replicator;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class ReplicatorTest {

    Replicator replicator;
    SourceConnectorConfig config;

    @Before
    public void before() throws Exception {
        config = new SourceConnectorConfig();
        config.setMqttBrokerUrl("tcp://100.76.11.96:1883");
        config.setMqttAccessKey("rocketmq");
        config.setMqttSecretKey("12345678");
        config.setSourceTopic("topic_producer");
        replicator = new Replicator(config);
        replicator.start();
    }

    @Test
    public void stop() throws Exception {
        replicator.stop();
    }

    @Test
    public void commitAddGetQueueTest() {
        MqttMessage message = new MqttMessage();
        replicator.commit(message, false);
        Assert.assertEquals(replicator.getQueue().poll(), message);
    }

    @Test
    public void getConfigTest() {
        Assert.assertEquals(replicator.getConfig(), config);
    }
}
