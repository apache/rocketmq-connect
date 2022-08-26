package org.apache.rocketmq.connect.kafka.connector;


import io.openmessaging.KeyValue;
import io.openmessaging.connector.api.component.task.Task;
import io.openmessaging.connector.api.component.task.sink.SinkConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class KafkaRocketmqSinkConnector extends SinkConnector {

    private static final Logger log = LoggerFactory.getLogger(KafkaRocketmqSinkConnector.class);

    private KafkaRocketmqConnector parentConnector = new KafkaRocketmqConnector(this);

    @Override
    public List<KeyValue> taskConfigs(int maxTasks) {
        return parentConnector.taskConfigs(maxTasks);
    }

    @Override
    public Class<? extends Task> taskClass() {
        return parentConnector.taskClass();
    }

    @Override
    public void start(KeyValue config) {
        parentConnector.start(config);
    }

    @Override
    public void stop() {
        parentConnector.stop();
    }

    @Override
    public void validate(KeyValue config) {
        parentConnector.validate(config);
    }

}
