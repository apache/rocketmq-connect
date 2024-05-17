package org.apache.rocketmq.connect.oss.sink;

import io.openmessaging.KeyValue;
import io.openmessaging.connector.api.component.task.Task;
import io.openmessaging.connector.api.component.task.sink.SinkConnector;

import java.util.ArrayList;
import java.util.List;

public class OSSSinkConnector extends SinkConnector {
    private KeyValue keyValue;

    @Override
    public void start(KeyValue config) {
        this.keyValue = config;
    }

    @Override
    public void stop() {
        this.keyValue = null;
    }

    @Override
    public List<KeyValue> taskConfigs(int maxTasks) {
        List<KeyValue> configs = new ArrayList<>();
        for (int i = 0; i < maxTasks; i++) {
            configs.add(this.keyValue);
        }
        return configs;
    }

    @Override
    public Class<? extends Task> taskClass() {
        return OSSSinkTask.class;
    }
}
