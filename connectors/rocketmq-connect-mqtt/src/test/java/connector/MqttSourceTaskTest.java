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
import io.openmessaging.internal.DefaultKeyValue;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.Collection;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.rocketmq.connect.mqtt.config.SourceConnectorConfig;
import org.apache.rocketmq.connect.mqtt.connector.MqttSourceTask;
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

public class MqttSourceTaskTest {
    private MqttClient mqttClient = null;

    @Before
    public void before() throws MqttException, NoSuchAlgorithmException, InvalidKeyException {
        MemoryPersistence memoryPersistence = new MemoryPersistence();
        String brokerUrl = "tcp://100.76.11.96:1883";
        String sendClientId = "send01";
        MqttConnectOptions mqttConnectOptions = buildMqttConnectOptions(sendClientId);
        mqttClient = new MqttClient(brokerUrl, sendClientId, memoryPersistence);
        mqttClient.setTimeToWait(5000L);
        mqttClient.setCallback(new MqttCallbackExtended() {
            @Override
            public void connectComplete(boolean reconnect, String serverURI) {
                System.out.println(sendClientId + " connect success to " + serverURI);
            }

            @Override
            public void connectionLost(Throwable throwable) {
                throwable.printStackTrace();
            }

            @Override
            public void messageArrived(String topic, MqttMessage mqttMessage) {
            }

            @Override
            public void deliveryComplete(IMqttDeliveryToken iMqttDeliveryToken) {
            }
        });
        try {
            mqttClient.connect(mqttConnectOptions);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void test() throws MqttException {
        KeyValue kv = new DefaultKeyValue();
        kv.put("mqttBrokerUrl", "tcp://100.76.11.96:1883");
        kv.put("mqttAccessKey", "rocketmq");
        kv.put("mqttSecretKey", "12345678");
        kv.put("sourceTopic", "topic_producer");
        MqttSourceTask task = new MqttSourceTask();
        task.start(kv);
        for (int i = 0; i < 5; ) {
            Collection<ConnectRecord> sourceDataEntry = task.poll();
            String msg = "r1_" + System.currentTimeMillis() + "_" + i;
            MqttMessage message = new MqttMessage(msg.getBytes(StandardCharsets.UTF_8));
            message.setQos(1);
            String mqttSendTopic = "topic_producer";
            mqttClient.publish(mqttSendTopic, message);
            System.out.println("send: " + mqttSendTopic + ", " + msg);
            i = i + sourceDataEntry.size();
            System.out.println(sourceDataEntry);
        }
    }

    @Test
    public void pollTest() throws Exception {
        MqttSourceTask task = new MqttSourceTask();
        MqttMessage textMessage = new MqttMessage();
        textMessage.setPayload("hello rocketmq".getBytes(StandardCharsets.UTF_8));

        Replicator replicatorObject = Mockito.mock(Replicator.class);
        BlockingQueue<MqttMessage> queue = new LinkedBlockingQueue<>();
        Mockito.when(replicatorObject.getQueue()).thenReturn(queue);

        Field replicator = MqttSourceTask.class.getDeclaredField("replicator");
        replicator.setAccessible(true);
        replicator.set(task, replicatorObject);

        Field config = MqttSourceTask.class.getDeclaredField("sourceConnectConfig");
        config.setAccessible(true);
        config.set(task, new SourceConnectorConfig());

        queue.put(textMessage);
        Collection<ConnectRecord> list = task.poll();
        Assert.assertEquals(list.size(), 1);

        list = task.poll();
        Assert.assertEquals(list.size(), 0);

    }

    @After
    public void after() throws MqttException {
        mqttClient.disconnect();
    }

    private static MqttConnectOptions buildMqttConnectOptions(
        String clientId) throws NoSuchAlgorithmException, InvalidKeyException {
        MqttConnectOptions connOpts = new MqttConnectOptions();
        connOpts.setCleanSession(true);
        connOpts.setKeepAliveInterval(60);
        connOpts.setAutomaticReconnect(true);
        connOpts.setMaxInflight(10000);
        connOpts.setUserName("rocketmq");
        connOpts.setPassword(HmacSHA1Util.macSignature(clientId, "12345678").toCharArray());
        return connOpts;
    }

}
