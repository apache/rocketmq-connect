package org.apache.rocketmq.connect.kafka.config;

import org.apache.kafka.connect.runtime.ConnectorConfig;
import org.apache.kafka.connect.runtime.TaskConfig;
import org.apache.kafka.connect.runtime.WorkerConfig;

import java.util.HashSet;
import java.util.Set;

public class ConfigDefine {
    public static String ROCKETMQ_CONNECTOR_CLASS = "connector-class";
    public static String CONNECTOR_CLASS = ConnectorConfig.CONNECTOR_CLASS_CONFIG;
    public static String PLUGIN_PATH = "plugin.path";

    public static final String TASK_CLASS = TaskConfig.TASK_CLASS_CONFIG;

    public static final String KEY_CONVERTER = WorkerConfig.KEY_CONVERTER_CLASS_CONFIG;
    public static final String VALUE_CONVERTER = WorkerConfig.VALUE_CONVERTER_CLASS_CONFIG;
    public static final String HEADER_CONVERTER = WorkerConfig.HEADER_CONVERTER_CLASS_CONFIG;


    public static final Set<String> REQUEST_CONFIG = new HashSet<String>(){
        {
            add(CONNECTOR_CLASS);
            add(PLUGIN_PATH);
        }
    };
}
