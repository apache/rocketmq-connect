package org.apache.rocketmq.connect.http.sink;

import io.openmessaging.KeyValue;
import io.openmessaging.connector.api.component.task.Task;
import io.openmessaging.connector.api.component.task.sink.SinkConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class SftpSinkConnector extends SinkConnector {

    private Logger log = LoggerFactory.getLogger(SftpConstant.LOGGER_NAME);

    private KeyValue config;

    @Override
    public List<KeyValue> taskConfigs(int maxTasks) {
        List<KeyValue> taskConfigs = new ArrayList<>();
        if(config != null) {
            taskConfigs.add(config);
        }
        return taskConfigs;
    }

    @Override
    public Class<? extends Task> taskClass() {
        return SftpSinkTask.class;
    }

    @Override
    public void validate(KeyValue config) {
    }

    @Override
    public void start(KeyValue config) {
        log.info("Sftp connector started");
        this.config = config;
    }

    @Override
    public void stop() {
        log.info("Sftp connector stopped");
    }
}
