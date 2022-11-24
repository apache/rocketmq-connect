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

package org.apache.rocketmq.connect.mqtt.source;

import io.openmessaging.connector.api.errors.ConnectException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import org.apache.rocketmq.connect.mqtt.config.SourceConnectorConfig;
import org.apache.rocketmq.connect.mqtt.util.MqttConnectionUtil;
import org.apache.rocketmq.connect.mqtt.util.Utils;
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallbackExtended;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Replicator {

    private final Logger log = LoggerFactory.getLogger(Replicator.class);

    private MqttClient mqttClient;
    private SourceConnectorConfig sourceConnectConfig;
    private BlockingQueue<MqttMessage> queue = new LinkedBlockingQueue<>();

    public Replicator(SourceConnectorConfig sourceConnectConfig) {
        this.sourceConnectConfig = sourceConnectConfig;
    }

    public void start() throws Exception {
        String brokerUrl = sourceConnectConfig.getMqttBrokerUrl();
        MemoryPersistence memoryPersistence = new MemoryPersistence();
        String sourceTopic = sourceConnectConfig.getSourceTopic();
        String recvClientId = Utils.createClientId("recv");
        MqttConnectOptions mqttConnectOptions = MqttConnectionUtil.buildMqttConnectOptions(recvClientId, sourceConnectConfig);
        mqttClient = new MqttClient(brokerUrl, recvClientId, memoryPersistence);
        mqttClient.setCallback(new MqttCallbackExtended() {
            @Override
            public void connectComplete(boolean reconnect, String serverURI) {
                log.info("{} connect success to {}", recvClientId, serverURI);
                try {
                    mqttClient.subscribe(sourceTopic, 1);
                } catch (Exception e) {
                    log.error("{} subscribe failed", recvClientId);
                }
            }

            @Override
            public void connectionLost(Throwable throwable) {
                log.error("connection lost {}", throwable.getMessage());
            }

            @Override
            public void messageArrived(String topic, MqttMessage mqttMessage) {
                try {
                    commit(mqttMessage, true);
                } catch (Exception e) {
                    throw new ConnectException("commit MqttMessage failed", e);
                }
            }

            @Override
            public void deliveryComplete(IMqttDeliveryToken iMqttDeliveryToken) {
            }
        });

        try {
            mqttClient.connect(mqttConnectOptions);
            log.info("Replicator start succeed");
        } catch (Exception e) {
            log.error("connect fail {}", e.getMessage());
        }
    }

    public void stop() throws Exception {
        mqttClient.disconnect();
    }

    public void commit(MqttMessage message, boolean isComplete) {
        queue.add(message);
    }

    public SourceConnectorConfig getConfig() {
        return this.sourceConnectConfig;
    }

    public BlockingQueue<MqttMessage> getQueue() {
        return queue;
    }

}
