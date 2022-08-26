package org.apache.rocketmq.connect.kafka.util;

import io.openmessaging.KeyValue;
import io.openmessaging.internal.DefaultKeyValue;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class ConfigUtil {

    public static Map<String, String> keyValueConfigToMap(KeyValue keyValueConfig){
        if(keyValueConfig == null){
            return null;
        }

        Set<String> configKeySet = keyValueConfig.keySet();
        Map<String, String> mapConfig = new HashMap<>(configKeySet.size());
        configKeySet.forEach(key -> mapConfig.put(key, keyValueConfig.getString(key)));
        return mapConfig;
    }


    public static KeyValue mapConfigToKeyValue(Map<String, String> mapConfig){
        if(mapConfig == null){
            return null;
        }

        KeyValue keyValue = new DefaultKeyValue();
        mapConfig.forEach((k, v)-> keyValue.put(k, v));

        return keyValue;
    }
}
