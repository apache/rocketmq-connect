package org.apache.rocketmq.connect.eventbridge.sink;

import io.openmessaging.KeyValue;
import io.openmessaging.connector.api.component.task.Task;
import io.openmessaging.connector.api.component.task.sink.SinkConnector;
import io.openmessaging.internal.DefaultKeyValue;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.connect.eventbridge.sink.constant.EventBridgeConstant;

import java.util.ArrayList;
import java.util.List;

public class EventBridgeSinkConnector extends SinkConnector {

    private String accessKeyId;

    private String accessKeySecret;

    private String roleArn;

    private String roleSessionName;

    private String eventSubject;

    private String aliyuneventbusname;

    private String accountEndpoint;

    private String stsEndpoint;

    @Override
    public void pause() {

    }

    @Override
    public void resume() {

    }

    @Override
    public List<KeyValue> taskConfigs(int maxTasks) {
        List<KeyValue> keyValueList = new ArrayList<>(11);
        KeyValue keyValue = new DefaultKeyValue();
        keyValue.put(EventBridgeConstant.ACCESS_KEY_ID, accessKeyId);
        keyValue.put(EventBridgeConstant.ACCESS_KEY_SECRET, accessKeySecret);
        keyValue.put(EventBridgeConstant.ROLE_ARN, roleArn);
        keyValue.put(EventBridgeConstant.ROLE_SESSION_NAME, roleSessionName);
        keyValue.put(EventBridgeConstant.EVENT_SUBJECT, eventSubject);
        keyValue.put(EventBridgeConstant.ALIYUN_EVENT_BUS_NAME, aliyuneventbusname);
        keyValue.put(EventBridgeConstant.ACCOUNT_ENDPOINT, accountEndpoint);
        keyValue.put(EventBridgeConstant.STS_ENDPOINT, stsEndpoint);
        keyValueList.add(keyValue);
        return keyValueList;
    }

    @Override
    public Class<? extends Task> taskClass() {
        return EventBridgeSinkTask.class;
    }

    @Override
    public void validate(KeyValue config) {
        if (StringUtils.isBlank(config.getString(EventBridgeConstant.ACCESS_KEY_ID))
                || StringUtils.isBlank(config.getString(EventBridgeConstant.ACCESS_KEY_SECRET))
                || StringUtils.isBlank(config.getString(EventBridgeConstant.ACCOUNT_ENDPOINT))
                || StringUtils.isBlank(config.getString(EventBridgeConstant.ALIYUN_EVENT_BUS_NAME))
                || StringUtils.isBlank(config.getString(EventBridgeConstant.EVENT_SUBJECT))) {
            throw new RuntimeException("EventBridge required parameter is null !");
        }
    }

    @Override
    public void init(KeyValue config) {
        accessKeyId = config.getString(EventBridgeConstant.ACCESS_KEY_ID);
        accessKeySecret = config.getString(EventBridgeConstant.ACCESS_KEY_SECRET);
        roleArn = config.getString(EventBridgeConstant.ROLE_ARN);
        roleSessionName = config.getString(EventBridgeConstant.ROLE_SESSION_NAME);
        eventSubject = config.getString(EventBridgeConstant.EVENT_SUBJECT);
        aliyuneventbusname = config.getString(EventBridgeConstant.ALIYUN_EVENT_BUS_NAME);
        accountEndpoint = config.getString(EventBridgeConstant.ACCOUNT_ENDPOINT);
        stsEndpoint = config.getString(EventBridgeConstant.STS_ENDPOINT);
    }

    @Override
    public void stop() {

    }
}
