package org.apache.rocketmq.connect.mns.source;

import io.openmessaging.KeyValue;
import io.openmessaging.connector.api.component.connector.ConnectorContext;
import io.openmessaging.connector.api.component.task.Task;
import io.openmessaging.connector.api.component.task.source.SourceConnector;
import io.openmessaging.internal.DefaultKeyValue;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.List;

import static org.apache.rocketmq.connect.mns.source.constant.MNSConstant.*;

public class MNSSourceConnector extends SourceConnector {

    private String accessKeyId;

    private String accessKeySecret;

    private String accountEndpoint;

    private String queueName;

    private String accountId;

    private String isBase64Decode;

    private Integer batchSize;

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
        keyValue.put(ACCOUNT_ID, accountId);
        if (batchSize == null) {
            keyValue.put(BATCH_SIZE, 8);
        }
        keyValue.put(IS_BASE64_DECODE, isBase64Decode);
        taskConfigList.add(keyValue);
        return taskConfigList;
    }

    @Override
    public Class<? extends Task> taskClass() {
        return MNSSourceTask.class;
    }

    @Override
    public void validate(KeyValue config) {
        if (StringUtils.isBlank(config.getString(ACCESS_KEY_ID))
                || StringUtils.isBlank(config.getString(ACCESS_KEY_SECRET))
                || StringUtils.isBlank(config.getString(ACCOUNT_ENDPOINT))
                || StringUtils.isBlank(config.getString(QUEUE_NAME))
                || StringUtils.isBlank(config.getString(ACCOUNT_ID))) {
            throw new RuntimeException("mns required parameter is null !");
        }
    }

    @Override
    public void init(KeyValue config) {
        accessKeyId = config.getString(ACCESS_KEY_ID);
        accessKeySecret = config.getString(ACCESS_KEY_SECRET);
        accountEndpoint = config.getString(ACCOUNT_ENDPOINT);
        queueName = config.getString(QUEUE_NAME);
        batchSize = config.getInt(BATCH_SIZE, 8);
        accountId = config.getString(ACCOUNT_ID);
        isBase64Decode = config.getString(IS_BASE64_DECODE, "true");
    }

    @Override
    public void start(ConnectorContext connectorContext) {
        super.start(connectorContext);
    }

    @Override
    public void stop() {

    }
}
