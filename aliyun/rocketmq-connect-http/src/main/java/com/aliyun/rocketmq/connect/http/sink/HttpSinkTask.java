package com.aliyun.rocketmq.connect.http.sink;

import io.openmessaging.KeyValue;
import io.openmessaging.connector.api.component.task.sink.SinkTask;
import io.openmessaging.connector.api.component.task.sink.SinkTaskContext;
import io.openmessaging.connector.api.data.ConnectRecord;
import io.openmessaging.connector.api.errors.ConnectException;

import java.util.List;

public class HttpSinkTask extends SinkTask {
    @Override
    public void put(List<ConnectRecord> sinkRecords) throws ConnectException {

    }

    @Override
    public void pause() {

    }

    @Override
    public void resume() {

    }

    @Override
    public void validate(KeyValue config) {

    }

    @Override
    public void init(KeyValue config) {

    }

    @Override
    public void start(SinkTaskContext sinkTaskContext) {
        super.start(sinkTaskContext);
    }

    @Override
    public void stop() {

    }
}
