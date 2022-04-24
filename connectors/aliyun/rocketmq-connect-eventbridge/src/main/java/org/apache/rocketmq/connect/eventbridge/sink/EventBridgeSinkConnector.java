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

    private String accountEndpoint;

    private String eventId;

    private String eventSource;

    private String eventTime;

    private String eventType;

    private String eventSubject;

    private String aliyuneventbusname;

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
        keyValue.put(EventBridgeConstant.ACCOUNT_ENDPOINT, accountEndpoint);
        keyValue.put(EventBridgeConstant.EVENT_ID, eventId);
        keyValue.put(EventBridgeConstant.EVENT_SOURCE, eventSource);
        keyValue.put(EventBridgeConstant.EVENT_TIME, eventTime);
        keyValue.put(EventBridgeConstant.EVENT_TYPE, eventType);
        keyValue.put(EventBridgeConstant.EVENT_SUBJECT, eventSubject);
        keyValue.put(EventBridgeConstant.ALIYUN_EVENT_BUS_NAME, aliyuneventbusname);
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
                || StringUtils.isBlank(config.getString(EventBridgeConstant.EVENT_ID))
                || StringUtils.isBlank(config.getString(EventBridgeConstant.EVENT_SOURCE))
                || StringUtils.isBlank(config.getString(EventBridgeConstant.EVENT_TIME))
                || StringUtils.isBlank(config.getString(EventBridgeConstant.EVENT_TYPE))
                || StringUtils.isBlank(config.getString(EventBridgeConstant.ALIYUN_EVENT_BUS_NAME))) {
            throw new RuntimeException("EventBridge required parameter is null !");
        }
    }

    @Override
    public void init(KeyValue config) {
        accessKeyId = config.getString(EventBridgeConstant.ACCESS_KEY_ID);
        accessKeySecret = config.getString(EventBridgeConstant.ACCESS_KEY_SECRET);
        accountEndpoint = config.getString(EventBridgeConstant.ACCOUNT_ENDPOINT);
        eventId = config.getString(EventBridgeConstant.EVENT_ID);
        eventSource = config.getString(EventBridgeConstant.EVENT_SOURCE);
        eventTime = config.getString(EventBridgeConstant.EVENT_TIME);
        eventType = config.getString(EventBridgeConstant.EVENT_TYPE);
        eventSubject = config.getString(EventBridgeConstant.EVENT_SUBJECT);
        aliyuneventbusname = config.getString(EventBridgeConstant.ALIYUN_EVENT_BUS_NAME);
    }

    @Override
    public void stop() {

    }
}
