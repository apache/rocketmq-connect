package org.apache.rocketmq.connect.kafka.connector;

import io.openmessaging.KeyValue;
import io.openmessaging.connector.api.component.connector.Connector;
import io.openmessaging.connector.api.component.task.Task;
import io.openmessaging.connector.api.component.task.source.SourceConnector;
import io.openmessaging.internal.DefaultKeyValue;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.runtime.ConnectorConfig;
import org.apache.kafka.connect.runtime.SinkConnectorConfig;
import org.apache.kafka.connect.runtime.isolation.Plugins;
import org.apache.rocketmq.connect.kafka.config.ConfigDefine;
import org.apache.rocketmq.connect.kafka.util.ConfigUtil;
import org.apache.rocketmq.connect.kafka.util.KafkaPluginsUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

public class KafkaRocketmqConnector extends Connector {
    private static final Logger log = LoggerFactory.getLogger(KafkaRocketmqConnector.class);

    private Connector childConnector;

    private org.apache.kafka.connect.connector.Connector kafkaConnector;
    private Plugins kafkaPlugins;
    private Map<String, String> kafkaConnectorConfigs;

    public KafkaRocketmqConnector(Connector childConnector) {
        this.childConnector = childConnector;
    }

    @Override
    public List<KeyValue> taskConfigs(int maxTasks) {
        List<KeyValue> taskKeyValueConfigs = new ArrayList<>();
        runWithWithConnectorLoader(() ->{
            List<Map<String, String>> taskConfigs = this.kafkaConnector.taskConfigs(maxTasks);
            taskKeyValueConfigs.addAll(
                    taskConfigs
                            .stream()
                            .map(kv -> {

                                kv.put(ConfigDefine.PLUGIN_PATH, this.kafkaConnectorConfigs.get(ConfigDefine.PLUGIN_PATH));
                                kv.put(ConfigDefine.CONNECTOR_CLASS, this.kafkaConnectorConfigs.get(ConnectorConfig.CONNECTOR_CLASS_CONFIG));
                                kv.put(ConfigDefine.TASK_CLASS, this.kafkaConnector.taskClass().getName());

                                if( this.kafkaConnectorConfigs.containsKey(ConfigDefine.KEY_CONVERTER)){
                                    kv.put(ConfigDefine.KEY_CONVERTER, this.kafkaConnectorConfigs.get(ConfigDefine.KEY_CONVERTER));
                                }

                                if( this.kafkaConnectorConfigs.containsKey(ConfigDefine.VALUE_CONVERTER)){
                                    kv.put(ConfigDefine.VALUE_CONVERTER, this.kafkaConnectorConfigs.get(ConfigDefine.VALUE_CONVERTER));
                                }

                                if( this.kafkaConnectorConfigs.containsKey(ConfigDefine.HEADER_CONVERTER)){
                                    kv.put(ConfigDefine.HEADER_CONVERTER, this.kafkaConnectorConfigs.get(ConfigDefine.HEADER_CONVERTER));
                                }

                                KeyValue keyValue = new DefaultKeyValue();
                                keyValue.put(ConfigDefine.ROCKETMQ_CONNECTOR_CLASS, childConnector.getClass().getName());
                                keyValue.put(ConfigDefine.KAFKA_CONNECTOR_CONFIGS, ConfigUtil.toJson(kv));
                                return keyValue;
                            })
                            .collect(Collectors.toList())
            );
        });
        return taskKeyValueConfigs;
    }

    @Override
    public Class<? extends Task> taskClass() {
        return this.childConnector instanceof SourceConnector
                ? KafkaRocketmqSourceTask.class : KafkaRocketmqSinkTask.class;
    }

    @Override
    public void start(KeyValue config) {
        runWithWithConnectorLoader(() ->{
            this.kafkaConnector.start(this.kafkaConnectorConfigs);
        });
    }

    @Override
    public void stop() {
        runWithWithConnectorLoader(() ->{
            this.kafkaConnector.stop();
        });
    }


    @Override
    public void validate(KeyValue config) {
        for(String requestConfig: ConfigDefine.REQUEST_CONFIG){
            if(!config.containsKey(requestConfig)){
                throw new ConnectException("miss config:"+requestConfig);
            }
        }
        this.kafkaConnectorConfigs = ConfigUtil.getKafkaConnectorConfigs(config);
        for(String requestConfig: Arrays.asList(ConfigDefine.CONNECTOR_CLASS, ConfigDefine.PLUGIN_PATH)){
            if(!kafkaConnectorConfigs.containsKey(requestConfig)){
                throw new ConnectException("miss config:"+requestConfig);
            }
        }
        if(!kafkaConnectorConfigs.containsKey(SinkConnectorConfig.TOPICS_CONFIG)){
            if(config.containsKey(ConfigDefine.ROCKETMQ_CONNECT_TOPIC_NAMES)){
                kafkaConnectorConfigs.put(
                        SinkConnectorConfig.TOPICS_CONFIG,
                        config.getString(ConfigDefine.ROCKETMQ_CONNECT_TOPIC_NAMES)
                );
            }
        }

        log.info("kafka connector config is {}", this.kafkaConnectorConfigs);
        this.kafkaPlugins =  KafkaPluginsUtil.getPlugins(Collections.singletonMap(KafkaPluginsUtil.PLUGIN_PATH, this.kafkaConnectorConfigs.get(ConfigDefine.PLUGIN_PATH)));
        String connectorClassName = this.kafkaConnectorConfigs.get(ConfigDefine.CONNECTOR_CLASS);
        ClassLoader connectorLoader = this.kafkaPlugins.delegatingLoader().connectorLoader(connectorClassName);
        ClassLoader savedLoader = Plugins.compareAndSwapLoaders(connectorLoader);
        try {
            this.kafkaConnector =  this.kafkaPlugins.newConnector(connectorClassName);
            this.kafkaConnector.validate(this.kafkaConnectorConfigs);
            this.kafkaConnector.initialize(
                    new RocketmqKafkaConnectorContext(getConnectorContext())
            );
        } finally {
            Plugins.compareAndSwapLoaders(savedLoader);
        }

    }

    private void runWithWithConnectorLoader(Runnable runnable){
        ClassLoader current = this.kafkaPlugins.compareAndSwapLoaders(this.kafkaConnector);
        try {
            runnable.run();
        } finally {
            Plugins.compareAndSwapLoaders(current);
        }
    }
}
