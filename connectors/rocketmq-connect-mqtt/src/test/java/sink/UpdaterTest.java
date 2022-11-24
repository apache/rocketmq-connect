package sink;/*
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

import io.openmessaging.connector.api.data.ConnectRecord;
import io.openmessaging.connector.api.data.Field;
import io.openmessaging.connector.api.data.RecordOffset;
import io.openmessaging.connector.api.data.RecordPartition;
import io.openmessaging.connector.api.data.Schema;
import io.openmessaging.connector.api.data.SchemaBuilder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.rocketmq.connect.mqtt.config.SinkConnectorConfig;
import org.apache.rocketmq.connect.mqtt.config.SourceConnectorConfig;
import org.apache.rocketmq.connect.mqtt.connector.MqttSourceTask;
import org.apache.rocketmq.connect.mqtt.sink.Updater;
import org.apache.rocketmq.connect.mqtt.source.Replicator;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class UpdaterTest {

    Updater updater;
    SinkConnectorConfig config;

    @Before
    public void before() throws Exception {
        config = new SinkConnectorConfig();
        config.setMqttBrokerUrl("tcp://100.76.11.96:1883");
        config.setMqttAccessKey("rocketmq");
        config.setMqttSecretKey("12345678");
        config.setSinkTopic("topic_consumer");
        updater = new Updater(config);
        updater.start();
    }

    @Test
    public void stop() {
        updater.stop();
    }

    @Test
    public void pushTest() {
        MqttMessage message = new MqttMessage();
        message.setPayload("hello rocketmq".getBytes(StandardCharsets.UTF_8));
        Schema schema = SchemaBuilder.struct().name("mqtt").build();
        final List<Field> fields = buildFields();
        schema.setFields(fields);
        final ConnectRecord connectRecord = new ConnectRecord(buildRecordPartition(),
            buildRecordOffset(message),
            System.currentTimeMillis(),
            schema,
            buildPayLoad(message));
        updater.push(connectRecord);
    }

    private RecordOffset buildRecordOffset(MqttMessage message) {
        Map<String, Long> offsetMap = new HashMap<>();
        offsetMap.put(SourceConnectorConfig.POSITION, 0l);
        RecordOffset recordOffset = new RecordOffset(offsetMap);
        return recordOffset;
    }

    private RecordPartition buildRecordPartition() {
        Map<String, String> partitionMap = new HashMap<>();
        partitionMap.put("partition", "defaultPartition");
        RecordPartition recordPartition = new RecordPartition(partitionMap);
        return recordPartition;
    }

    private List<Field> buildFields() {
        final Schema structSchema = SchemaBuilder.struct().build();
        List<Field> fields = new ArrayList<>();
        fields.add(new Field(0, SourceConnectorConfig.MESSAGE, structSchema));
        return fields;
    }

    private String buildPayLoad(MqttMessage message) {
        return new String(message.getPayload());

    }

}
