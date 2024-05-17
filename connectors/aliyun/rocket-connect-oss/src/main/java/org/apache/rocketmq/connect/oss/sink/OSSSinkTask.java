package org.apache.rocketmq.connect.oss.sink;

import io.openmessaging.KeyValue;
import io.openmessaging.connector.api.component.task.sink.SinkTask;
import io.openmessaging.connector.api.data.ConnectRecord;
import io.openmessaging.connector.api.errors.ConnectException;
import org.apache.rocketmq.connect.oss.common.oss.OSSClientProvider;
import org.apache.rocketmq.connect.oss.config.TaskConfig;
import org.apache.rocketmq.connect.oss.service.CloudEventManager;

import java.util.Calendar;
import java.util.List;

public class OSSSinkTask extends SinkTask {
    private TaskConfig config;
    private OSSClientProvider ossClient;

    private CloudEventManager cloudEventManager;

    @Override
    public void validate(KeyValue config) {
        this.cloudEventManager.validate(config);
    }


    @Override
    public void start(KeyValue keyValue) {
        this.cloudEventManager = new CloudEventManager(keyValue);
    }

    @Override
    public void stop() {
        this.cloudEventManager.shutdown();
    }

    @Override
    public void put(List<ConnectRecord> sinkRecords) throws ConnectException {
        if (sinkRecords == null || sinkRecords.isEmpty()) {
            return;
        }

        for (ConnectRecord record : sinkRecords) {
            cloudEventManager.send(record);
        }
    }



}
