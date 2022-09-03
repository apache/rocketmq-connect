package org.apache.rocketmq.connect.kafka.config;

import org.apache.kafka.connect.runtime.ConnectorConfig;
import org.apache.kafka.connect.runtime.TaskConfig;
import org.apache.kafka.connect.runtime.WorkerConfig;

import java.util.HashSet;
import java.util.Set;

public class ConfigDefine {
    public static final String ROCKETMQ_CONNECTOR_CLASS = "connector.class";
    public static final String ROCKETMQ_CONNECT_TOPIC_NAME = "connect.topicname";
    public static final String ROCKETMQ_CONNECT_TOPIC_NAMES = "connect.topicnames";

    public static final String KAFKA_CONNECTOR_CONFIGS = "kafka.connector.configs";
    public static final String CONNECTOR_CLASS = ConnectorConfig.CONNECTOR_CLASS_CONFIG;
    public static final String PLUGIN_PATH = "plugin.path";

    public static final String TASK_CLASS = TaskConfig.TASK_CLASS_CONFIG;

    public static final String KEY_CONVERTER = WorkerConfig.KEY_CONVERTER_CLASS_CONFIG;
    public static final String VALUE_CONVERTER = WorkerConfig.VALUE_CONVERTER_CLASS_CONFIG;
    public static final String HEADER_CONVERTER = WorkerConfig.HEADER_CONVERTER_CLASS_CONFIG;

    // encodedTopic/assignEncodedPartition/regexEncodedPartition
    public static final String ROCKETMQ_RECORDPARTITION_KAFKATOPICPARTITION_MAPPER = "rocketmq.recordPartition.kafkaTopicPartition.mapper";

    public static final Set<String> REQUEST_CONFIG = new HashSet<String>(){
        {
            add(KAFKA_CONNECTOR_CONFIGS);
        }
    };
}
