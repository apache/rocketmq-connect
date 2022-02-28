package com.aliyun.rocketmq.connect.mns.source;

import io.openmessaging.KeyValue;
import io.openmessaging.connector.api.component.ComponentContext;
import io.openmessaging.connector.api.component.task.Task;
import io.openmessaging.connector.api.component.task.source.SourceConnector;
import io.openmessaging.internal.DefaultKeyValue;

import java.util.ArrayList;
import java.util.List;

public class MNSSourceConnector extends SourceConnector {

    private static final String ACCESS_KEY_ID = "accessKeyId";
    private static final String ACCESS_KEY_SECRET = "accessKeySecret";
    private static final String ACCOUNT_ENDPOINT = "accountEndpoint";
    private static final String QUEUE_NAME = "queueName";

    private String accessKeyId;

    private String accessKeySecret;

    private String accountEndpoint;

    private String queueName;

    @Override
    public void pause() {

    }

    @Override
    public void resume() {

    }

    @Override
    public List<KeyValue> taskConfigs(int maxTasks) {
        List<KeyValue> taskConfigList = new ArrayList<>(11);
        KeyValue keyValue = new DefaultKeyValue();
        keyValue.put(ACCESS_KEY_ID, accessKeyId);
        keyValue.put(ACCESS_KEY_SECRET, accessKeySecret);
        keyValue.put(ACCOUNT_ENDPOINT, accountEndpoint);
        keyValue.put(QUEUE_NAME, queueName);
        taskConfigList.add(keyValue);
        return taskConfigList;
    }

    @Override
    public Class<? extends Task> taskClass() {
        return MNSSourceTask.class;
    }

    @Override
    public void validate(KeyValue config) {

    }

    @Override
    public void init(KeyValue config) {
        accessKeyId = config.getString("MNSAccessKeyId");
        accessKeySecret = config.getString("MNSAccessKeySecret");
        accountEndpoint = config.getString("MNSAccountEndpoint");
        queueName = config.getString("queueName");
    }

    @Override
    public void start(ComponentContext componentContext) {

    }

    @Override
    public void stop() {

    }
}
