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
import io.openmessaging.connector.api.data.ConnectRecord;
import io.openmessaging.connector.api.data.RecordOffset;
import io.openmessaging.connector.api.data.RecordPartition;
import io.openmessaging.connector.api.data.Schema;
import io.openmessaging.connector.api.data.SchemaBuilder;
import io.openmessaging.internal.DefaultKeyValue;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import org.apache.rocketmq.connect.mqtt.config.SinkConnectorConfig;
import org.apache.rocketmq.connect.mqtt.config.SourceConnectorConfig;
import org.apache.rocketmq.connect.mqtt.connector.MqttSinkTask;
import org.apache.rocketmq.connect.mqtt.connector.MqttSourceTask;
import org.apache.rocketmq.connect.mqtt.sink.Updater;
import org.apache.rocketmq.connect.mqtt.source.Replicator;
import org.apache.rocketmq.connect.mqtt.util.HmacSHA1Util;
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallbackExtended;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

public class MqttSinkTaskTest {

    @Before
    public void before() {
        KeyValue kv = new DefaultKeyValue();
        kv.put("mqttBrokerUrl", "tcp://100.76.11.96:1883");
        kv.put("mqttAccessKey", "rocketmq");
        kv.put("mqttSecretKey", "12345678");
        kv.put("sinkTopic", "topic_consumer");
        MqttSinkTask task = new MqttSinkTask();
        task.start(kv);
    }

    @Test
    public void putTest() throws Exception {
        MqttSinkTask task = new MqttSinkTask();
        MqttMessage message = new MqttMessage();
        message.setPayload("hello rocketmq".getBytes(StandardCharsets.UTF_8));
        Schema schema = SchemaBuilder.struct().name("mqtt").build();
        final List<io.openmessaging.connector.api.data.Field> fields = buildFields();
        schema.setFields(fields);
        final ConnectRecord connectRecord = new ConnectRecord(buildRecordPartition(),
            buildRecordOffset(message),
            System.currentTimeMillis(),
            schema,
            buildPayLoad(message));
        ArrayList<ConnectRecord> list = new ArrayList<>();
        list.add(connectRecord);

        Updater updaterObject = Mockito.mock(Updater.class);

        Field updater = MqttSinkTask.class.getDeclaredField("updater");
        updater.setAccessible(true);
        updater.set(task, updaterObject);

        Field config = MqttSinkTask.class.getDeclaredField("sinkConnectConfig");
        config.setAccessible(true);
        config.set(task, new SinkConnectorConfig());
        task.put(list);
        Assert.assertEquals(list.size(), 1);

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

    private List<io.openmessaging.connector.api.data.Field> buildFields() {
        final Schema structSchema = SchemaBuilder.struct().build();
        List<io.openmessaging.connector.api.data.Field> fields = new ArrayList<>();
        fields.add(new io.openmessaging.connector.api.data.Field(0, SourceConnectorConfig.MESSAGE, structSchema));
        return fields;
    }

    private String buildPayLoad(MqttMessage message) {
        return new String(message.getPayload());

    }

}
