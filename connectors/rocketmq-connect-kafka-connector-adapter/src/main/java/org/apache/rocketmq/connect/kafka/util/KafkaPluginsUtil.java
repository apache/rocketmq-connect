package org.apache.rocketmq.connect.kafka.util;

import org.apache.kafka.connect.runtime.isolation.Plugins;

import java.util.HashMap;
import java.util.Map;

public class KafkaPluginsUtil {


    public static final String PLUGIN_PATH = "plugin.path";
    private static final Map<String, Plugins> CACHE = new HashMap<>();

    public static Plugins getPlugins(Map<String, String> props){
        String path =  props.get(PLUGIN_PATH);
        synchronized (CACHE){
            Plugins plugins = CACHE.get(path);
            if(plugins  == null){
                plugins = new Plugins(props);
                CACHE.put(path, plugins);
            }
            return plugins;
        }
    }
}
