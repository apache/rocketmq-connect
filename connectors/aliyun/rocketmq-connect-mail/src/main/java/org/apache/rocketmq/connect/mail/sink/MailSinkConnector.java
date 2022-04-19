package org.apache.rocketmq.connect.mail.sink;

import io.openmessaging.KeyValue;
import io.openmessaging.connector.api.component.task.Task;
import io.openmessaging.connector.api.component.task.sink.SinkConnector;
import io.openmessaging.internal.DefaultKeyValue;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.connect.mail.sink.constant.MailConstant;

import java.util.ArrayList;
import java.util.List;

public class MailSinkConnector extends SinkConnector {

    private String accessKeyId;

    private String accessKeySecret;

    private String accountEndpoint;

    private String accountName;

    private String addressType;

    private String replyToAddress;

    private String toAddress;

    private String subject;

    private String isHtmlBody;

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
        keyValue.put(MailConstant.ACCESS_KEY_ID, accessKeyId);
        keyValue.put(MailConstant.ACCESS_KEY_SECRET, accessKeySecret);
        keyValue.put(MailConstant.ACCOUNT_ENDPOINT, accountEndpoint);
        keyValue.put(MailConstant.ACCOUNT_NAME, accountName);
        keyValue.put(MailConstant.ADDRESS_TYPE, addressType);
        keyValue.put(MailConstant.REPLY_TO_ADDRESS, replyToAddress);
        keyValue.put(MailConstant.TO_ADDRESS, toAddress);
        keyValue.put(MailConstant.SUBJECT, subject);
        keyValue.put(MailConstant.IS_HTML_BODY, isHtmlBody);
        keyValueList.add(keyValue);
        return keyValueList;
    }

    @Override
    public Class<? extends Task> taskClass() {
        return MailSinkTask.class;
    }

    @Override
    public void validate(KeyValue config) {
        if (StringUtils.isBlank(config.getString(MailConstant.ACCESS_KEY_ID))
                || StringUtils.isBlank(config.getString(MailConstant.ACCESS_KEY_SECRET))
                || StringUtils.isBlank(config.getString(MailConstant.ACCOUNT_ENDPOINT))
                || StringUtils.isBlank(config.getString(MailConstant.ACCOUNT_NAME))
                || StringUtils.isBlank(config.getString(MailConstant.SUBJECT))
                || StringUtils.isBlank(config.getString(MailConstant.TO_ADDRESS))) {
            throw new RuntimeException("mail required parameter is null !");
        }
    }

    @Override
    public void init(KeyValue config) {
        accessKeyId = config.getString(MailConstant.ACCESS_KEY_ID);
        accessKeySecret = config.getString(MailConstant.ACCESS_KEY_SECRET);
        accountEndpoint = config.getString(MailConstant.ACCOUNT_ENDPOINT);
        accountName = config.getString(MailConstant.ACCOUNT_NAME);
        subject = config.getString(MailConstant.SUBJECT);
        addressType = config.getString(MailConstant.ADDRESS_TYPE, "0");
        replyToAddress = config.getString(MailConstant.REPLY_TO_ADDRESS, "true");
        toAddress = config.getString(MailConstant.TO_ADDRESS);
        isHtmlBody = config.getString(MailConstant.IS_HTML_BODY, "true");
    }

    @Override
    public void stop() {

    }
}
