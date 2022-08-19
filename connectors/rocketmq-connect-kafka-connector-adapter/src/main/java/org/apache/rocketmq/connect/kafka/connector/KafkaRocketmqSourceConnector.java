package org.apache.rocketmq.connect.kafka.connector;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.openmessaging.KeyValue;
import io.openmessaging.connector.api.component.task.Task;
import io.openmessaging.connector.api.component.task.source.SourceConnector;

import java.util.List;


public class KafkaRocketmqSourceConnector extends SourceConnector {

    private static final Logger log = LoggerFactory.getLogger(KafkaRocketmqSourceConnector.class);

    private KafkaRocketmqConnector kafkaRocketmqConnector = new KafkaRocketmqConnector(this);

    @Override
    public List<KeyValue> taskConfigs(int maxTasks) {
        return kafkaRocketmqConnector.taskConfigs(maxTasks);
    }
    
    @Override
    public Class<? extends Task> taskClass() {
        return kafkaRocketmqConnector.taskClass();
    }

    @Override
    public void start(KeyValue config) {
        kafkaRocketmqConnector.start(config);
    }

    @Override
    public void stop() {
        kafkaRocketmqConnector.stop();
    }

    @Override
    public void validate(KeyValue config) {
        kafkaRocketmqConnector.validate(config);
    }

}
