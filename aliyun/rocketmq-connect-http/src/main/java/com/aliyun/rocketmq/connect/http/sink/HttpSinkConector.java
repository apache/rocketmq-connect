package com.aliyun.rocketmq.connect.http.sink;

import io.openmessaging.KeyValue;
import io.openmessaging.connector.api.component.task.Task;
import io.openmessaging.connector.api.component.task.sink.SinkConnector;

import java.util.List;

public class HttpSinkConector extends SinkConnector {
    @Override
    public void pause() {

    }

    @Override
    public void resume() {

    }

    @Override
    public List<KeyValue> taskConfigs(int maxTasks) {
        return null;
    }

    @Override
    public Class<? extends Task> taskClass() {
        return null;
    }

    @Override
    public void validate(KeyValue config) {

    }

    @Override
    public void init(KeyValue config) {

    }

    @Override
    public void stop() {

    }
}
