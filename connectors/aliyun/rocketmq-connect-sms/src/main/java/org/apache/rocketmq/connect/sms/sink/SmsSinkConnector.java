package org.apache.rocketmq.connect.sms.sink;

import io.openmessaging.KeyValue;
import io.openmessaging.connector.api.component.task.Task;
import io.openmessaging.connector.api.component.task.sink.SinkConnector;
import io.openmessaging.internal.DefaultKeyValue;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.connect.sms.sink.constant.SmsConstant;

import java.util.ArrayList;
import java.util.List;

public class SmsSinkConnector extends SinkConnector {
    private String accessKeyId;

    private String accessKeySecret;

    private String accountEndpoint;

    private String phoneNumbers;

    private String signName;

    private String templateCode;

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
        keyValue.put(SmsConstant.PHONE_NUMBERS, phoneNumbers);
        keyValue.put(SmsConstant.ACCESS_KEY_ID, accessKeyId);
        keyValue.put(SmsConstant.ACCESS_KEY_SECRET, accessKeySecret);
        keyValue.put(SmsConstant.ACCOUNT_ENDPOINT, accountEndpoint);
        keyValue.put(SmsConstant.TEMPLATE_CODE, templateCode);
        keyValue.put(SmsConstant.SIGN_NAME, signName);
        keyValueList.add(keyValue);
        return keyValueList;
    }

    @Override
    public Class<? extends Task> taskClass() {
        return SmsSinkTask.class;
    }

    @Override
    public void validate(KeyValue config) {
        if (StringUtils.isBlank(config.getString(SmsConstant.ACCESS_KEY_ID))
                || StringUtils.isBlank(config.getString(SmsConstant.ACCESS_KEY_SECRET))
                || StringUtils.isBlank(config.getString(SmsConstant.ACCOUNT_ENDPOINT))
                || StringUtils.isBlank(config.getString(SmsConstant.PHONE_NUMBERS))
                || StringUtils.isBlank(config.getString(SmsConstant.SIGN_NAME))
                || StringUtils.isBlank(config.getString(SmsConstant.TEMPLATE_CODE))) {
            throw new RuntimeException("sms required parameter is null !");
        }
    }

    @Override
    public void init(KeyValue config) {
        accessKeyId = config.getString(SmsConstant.ACCESS_KEY_ID);
        accessKeySecret = config.getString(SmsConstant.ACCESS_KEY_SECRET);
        accountEndpoint = config.getString(SmsConstant.ACCOUNT_ENDPOINT);
        phoneNumbers = config.getString(SmsConstant.PHONE_NUMBERS);
        signName = config.getString(SmsConstant.SIGN_NAME);
        templateCode = config.getString(SmsConstant.TEMPLATE_CODE);
    }

    @Override
    public void stop() {

    }
}
