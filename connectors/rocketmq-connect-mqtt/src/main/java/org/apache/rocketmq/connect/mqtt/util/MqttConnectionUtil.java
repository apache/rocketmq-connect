package org.apache.rocketmq.connect.mqtt.util;

import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import org.apache.rocketmq.connect.mqtt.config.ConnectorConfig;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;

public class MqttConnectionUtil {
    public static MqttConnectOptions buildMqttConnectOptions(
        String clientId, ConnectorConfig connectorConfig) throws NoSuchAlgorithmException, InvalidKeyException {
        MqttConnectOptions connOpts = new MqttConnectOptions();
        connOpts.setCleanSession(true);
        connOpts.setKeepAliveInterval(60);
        connOpts.setAutomaticReconnect(true);
        connOpts.setMaxInflight(10000);
        connOpts.setUserName(connectorConfig.getMqttAccessKey());
        connOpts.setPassword(HmacSHA1Util.macSignature(clientId, connectorConfig.getMqttSecretKey()).toCharArray());
        return connOpts;
    }
}
