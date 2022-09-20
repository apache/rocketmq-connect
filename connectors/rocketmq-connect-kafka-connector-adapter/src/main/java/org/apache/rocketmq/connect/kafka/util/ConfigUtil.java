package org.apache.rocketmq.connect.kafka.util;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.openmessaging.KeyValue;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.rocketmq.connect.kafka.config.ConfigDefine;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class ConfigUtil {

    private final static ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    public static Map<String, String> keyValueConfigToMap(KeyValue keyValueConfig){
        if(keyValueConfig == null){
            return null;
        }

        Set<String> configKeySet = keyValueConfig.keySet();
        Map<String, String> mapConfig = new HashMap<>(configKeySet.size());
        configKeySet.forEach(key -> mapConfig.put(key, keyValueConfig.getString(key)));
        return mapConfig;
    }

    public static Map<String, String> getKafkaConnectorConfigs(KeyValue keyValueConfig){
        String kafkaConnectorConfigsStr = keyValueConfig.getString(ConfigDefine.KAFKA_CONNECTOR_CONFIGS);
        try {
            return OBJECT_MAPPER.readValue(kafkaConnectorConfigsStr,
                    new TypeReference<Map<String, String>>(){});
        } catch (Exception e){
            throw new ConnectException("error kafka.connector.configs config" , e);
        }
    }

    public static String toJson(Map<String, String> mapConfig) {
        try {
            return OBJECT_MAPPER.writeValueAsString(mapConfig);
        } catch (Exception e){
            throw new RuntimeException(e);
        }
    }
}
