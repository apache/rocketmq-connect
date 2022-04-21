package org.apache.rocketmq.connect.sms.sink;

import com.aliyun.dysmsapi20170525.Client;
import com.aliyun.dysmsapi20170525.models.SendSmsRequest;
import com.aliyun.dysmsapi20170525.models.SendSmsResponse;
import com.aliyun.teaopenapi.models.Config;
import io.openmessaging.KeyValue;
import io.openmessaging.connector.api.component.task.sink.SinkTask;
import io.openmessaging.connector.api.component.task.sink.SinkTaskContext;
import io.openmessaging.connector.api.data.ConnectRecord;
import io.openmessaging.connector.api.errors.ConnectException;
import org.apache.rocketmq.connect.sms.sink.constant.SmsConstant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class SmsSinkTask extends SinkTask {
    private static final Logger log = LoggerFactory.getLogger(SmsSinkTask.class);

    private String accessKeyId;

    private String accessKeySecret;

    private String accountEndpoint;

    private String phoneNumbers;

    private String signName;

    private String templateCode;

    private Client client;

    @Override
    public void put(List<ConnectRecord> sinkRecords) throws ConnectException {
        try {
            sinkRecords.forEach(connectRecord -> {
                SendSmsRequest sendSmsRequest = new SendSmsRequest()
                        .setPhoneNumbers(phoneNumbers)
                        .setSignName(signName)
                        .setTemplateCode(templateCode)
                        .setTemplateParam(connectRecord.getData().toString());
                try {
                    final SendSmsResponse sendSmsResponse = client.sendSms(sendSmsRequest);
                    log.info("sendSms | sendSmsResponse | Code ：{} | Message : {} | BizId : {} | RequestId : {}",
                            sendSmsResponse.getBody().getCode(),
                            sendSmsResponse.getBody().getMessage(),
                            sendSmsResponse.getBody().getBizId(),
                            sendSmsResponse.getBody().getRequestId());
                } catch (Exception e) {
                    log.error("SmsSinkTask | sendSms | error => ", e);
                    throw new RuntimeException(e);
                }
            });
        } catch (Exception e) {
            log.error("SmsSinkTask | put | error => ", e);
            throw new RuntimeException(e);
        }
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
        accessKeyId = config.getString(SmsConstant.ACCESS_KEY_ID);
        accessKeySecret = config.getString(SmsConstant.ACCESS_KEY_SECRET);
        accountEndpoint = config.getString(SmsConstant.ACCOUNT_ENDPOINT);
        phoneNumbers = config.getString(SmsConstant.PHONE_NUMBERS);
        signName = config.getString(SmsConstant.SIGN_NAME);
        templateCode = config.getString(SmsConstant.TEMPLATE_CODE);
    }

    @Override
    public void start(SinkTaskContext sinkTaskContext) {
        Config config = new Config()
                .setAccessKeyId(accessKeyId)
                .setAccessKeySecret(accessKeySecret);
        config.endpoint = accountEndpoint;
        try {
            super.start(sinkTaskContext);
            client = new Client(config);
        } catch (Exception e) {
            log.error("SmsSinkTask | start | error => ", e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public void stop() {
        client = null;
    }
}
