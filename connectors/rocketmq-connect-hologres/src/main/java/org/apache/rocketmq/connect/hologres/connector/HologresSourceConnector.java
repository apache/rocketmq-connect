package org.apache.rocketmq.connect.hologres.connector;

import io.openmessaging.KeyValue;
import io.openmessaging.connector.api.component.task.Task;
import io.openmessaging.connector.api.component.task.source.SourceConnector;
import org.apache.rocketmq.connect.hologres.config.HologresSinkConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class HologresSourceConnector extends SourceConnector {
    private static final Logger log = LoggerFactory.getLogger(HologresSourceConnector.class);

    private KeyValue keyValue;

    @Override
    public void validate(KeyValue config) {
        for (String requestKey : HologresSinkConfig.REQUEST_CONFIG) {
            if (!config.containsKey(requestKey)) {
                throw new RuntimeException("Request config key not exist: " + requestKey);
            }
        }
    }

    @Override
    public List<KeyValue> taskConfigs(int maxTasks) {
        log.info("Init {} source task config", maxTasks);
        List<KeyValue> configs = new ArrayList<>();
        for (int i = 0; i < maxTasks; i++) {
            configs.add(this.keyValue);
        }
        return configs;
    }

    @Override
    public Class<? extends Task> taskClass() {
        return HologresSourceTask.class;
    }

    @Override
    public void start(KeyValue keyValue) {
        log.info("HologresSourceConnector start enter");
        this.keyValue = keyValue;
    }

    @Override
    public void stop() {
        log.info("HologresSourceConnector start enter");
        this.keyValue = null;
    }
}
