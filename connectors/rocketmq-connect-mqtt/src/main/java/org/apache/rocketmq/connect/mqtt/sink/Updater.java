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


package org.apache.rocketmq.connect.mqtt.sink;

import io.openmessaging.connector.api.data.ConnectRecord;
import io.openmessaging.connector.api.data.Field;
import io.openmessaging.connector.api.data.FieldType;
import io.openmessaging.connector.api.data.Struct;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import org.apache.rocketmq.connect.mqtt.config.SinkConnectorConfig;
import org.apache.rocketmq.connect.mqtt.util.MqttConnectionUtil;
import org.apache.rocketmq.connect.mqtt.util.Utils;
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallbackExtended;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Updater {

    private final Logger log = LoggerFactory.getLogger(Updater.class);
    private SinkConnectorConfig sinkConnectConfig;
    private MqttClient mqttClient;
    private volatile boolean started = false;

    public Updater(SinkConnectorConfig sinkConnectConfig) {
        this.sinkConnectConfig = sinkConnectConfig;

    }

    private MqttMessage sinkDataEntry2MqttMessage(ConnectRecord record) {
        try {
            Object object = record.getData();
            log.info("Updater.sinkDataEntry2MqttMessage() get called ");
            final Struct struct = (Struct) object;
            final Object[] values = struct.getValues();
            final List<Field> fields = struct.getSchema().getFields();
            for (int i = 0; i < fields.size(); i++) {
                final String name = fields.get(i).getName();
                Object value = values[i];
                if (name.equals(SinkConnectorConfig.MESSAGE) && value != null) {
                    byte[] recordBytes = ((String) value).getBytes(StandardCharsets.UTF_8);
                    MqttMessage mqttMessage = new MqttMessage(recordBytes);
                    return mqttMessage;
                }
            }
        } catch (Exception e) {
            log.error("convert record to mqttMessage error", e);
        }
        return null;
    }

    public void start() throws Exception {
        MemoryPersistence memoryPersistence = new MemoryPersistence();
        String brokerUrl = sinkConnectConfig.getMqttBrokerUrl();
        String sendClientId = Utils.createClientId("send");
        MqttConnectOptions mqttConnectOptions = MqttConnectionUtil.buildMqttConnectOptions(sendClientId, sinkConnectConfig);
        mqttClient = new MqttClient(brokerUrl, sendClientId, memoryPersistence);
        mqttClient.setCallback(new MqttCallbackExtended() {
            @Override
            public void connectComplete(boolean reconnect, String serverURI) {
                log.info("{} connect success to {}", sendClientId, serverURI);
            }

            @Override
            public void connectionLost(Throwable throwable) {
                log.error("connection lost {}", throwable.getMessage());
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
            started = true;
        } catch (Exception e) {
            log.error("MQTT Client of task {} start failed.", e);
        }
        log.info("MQTT sink task started");
    }

    public boolean push(ConnectRecord record) {
        log.info("Updater Trying to push data");
        Boolean isSuccess = true;
        if (record == null) {
            log.warn("Updater push sinkDataRecord null.");
            return true;
        }
        try {
            MqttMessage message = sinkDataEntry2MqttMessage(record);
            if (message != null) {
                mqttClient.publish(sinkConnectConfig.getSinkTopic(), message);
            }
        } catch (Exception e) {
            log.error("Updater commit occur error", e);
            isSuccess = false;
        }

        return isSuccess;
    }

    public void stop() {
        if (started) {
            if (this.mqttClient != null) {
                try {
                    this.mqttClient.disconnect();
                    log.info("MQTT sink updater stopped.");
                } catch (MqttException e) {
                    log.error("MQTT Client disConnect {} failed.", this.mqttClient, e);
                }
            }
            started = false;
        }
    }

}
