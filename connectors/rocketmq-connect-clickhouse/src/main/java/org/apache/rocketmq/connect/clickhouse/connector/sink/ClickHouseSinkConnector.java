package org.apache.rocketmq.connect.clickhouse.connector.sink;


import io.openmessaging.KeyValue;
import io.openmessaging.connector.api.component.task.Task;
import io.openmessaging.connector.api.component.task.sink.SinkConnector;
import java.util.ArrayList;
import java.util.List;
import org.apache.rocketmq.connect.clickhouse.connector.config.ClickhouseConfig;

public class ClickHouseSinkConnector extends SinkConnector {

    private KeyValue keyValue;

    @Override public List<KeyValue> taskConfigs(int maxTasks) {
        List<KeyValue> configs = new ArrayList<>();
        for (int i = 0; i < maxTasks; i++) {
            configs.add(this.keyValue);
        }
        return configs;
    }

    @Override public Class<? extends Task> taskClass() {
        return ClickHouseSinkTask.class;
    }

    @Override public void start(KeyValue value) {

        for (String requestKey : ClickhouseConfig.REQUEST_CONFIG) {
            if (!value.containsKey(requestKey)) {
                throw new RuntimeException("Request config key: " + requestKey);
            }
        }

        this.keyValue = value;
    }

    @Override public void stop() {
        this.keyValue = null;
    }
}
