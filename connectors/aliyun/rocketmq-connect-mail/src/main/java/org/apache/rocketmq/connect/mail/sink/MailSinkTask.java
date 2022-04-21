package org.apache.rocketmq.connect.mail.sink;

import com.aliyun.dm20151123.Client;
import com.aliyun.dm20151123.models.SingleSendMailRequest;
import com.aliyun.dm20151123.models.SingleSendMailResponse;
import com.aliyun.teaopenapi.models.Config;
import io.openmessaging.KeyValue;
import io.openmessaging.connector.api.component.task.sink.SinkTask;
import io.openmessaging.connector.api.component.task.sink.SinkTaskContext;
import io.openmessaging.connector.api.data.ConnectRecord;
import io.openmessaging.connector.api.errors.ConnectException;
import org.apache.rocketmq.connect.mail.sink.constant.MailConstant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class MailSinkTask extends SinkTask {
    private static final Logger log = LoggerFactory.getLogger(MailSinkTask.class);

    private String accessKeyId;

    private String accessKeySecret;

    private String accountEndpoint;

    private String accountName;

    private String addressType;

    private String replyToAddress;

    private String toAddress;

    private String subject;

    private String isHtmlBody;

    private Client client;

    @Override
    public void put(List<ConnectRecord> sinkRecords) throws ConnectException {
        sinkRecords.forEach(connectRecord -> {
            SingleSendMailRequest singleSendMailRequest = new SingleSendMailRequest();
            singleSendMailRequest.setAccountName(accountName);
            singleSendMailRequest.setAddressType(Integer.parseInt(addressType));
            singleSendMailRequest.setToAddress(toAddress);
            singleSendMailRequest.setSubject(subject);
            singleSendMailRequest.setReplyToAddress(Boolean.parseBoolean(replyToAddress));
            if (Boolean.parseBoolean(isHtmlBody)) {
                singleSendMailRequest.setHtmlBody(connectRecord.getData().toString());
            } else {
                singleSendMailRequest.setTextBody(connectRecord.getData().toString());
            }
            try {
                final SingleSendMailResponse singleSendMailResponse = client.singleSendMail(singleSendMailRequest);
                log.info("singleSendMail | singleSendMailResponse | envId : {} | requestId : {}",
                        singleSendMailResponse.getBody().getEnvId(),
                        singleSendMailResponse.getBody().getRequestId());
            } catch (Exception e) {
                log.error("MailSinkTask | put | error => ", e);
                throw new RuntimeException(e);
            }
        });
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
    public void start(SinkTaskContext sinkTaskContext) {
        Config config = new Config()
                .setAccessKeyId(accessKeyId)
                .setAccessKeySecret(accessKeySecret);
        config.endpoint = accountEndpoint;
        try {
            super.start(sinkTaskContext);
            client = new Client(config);
        } catch (Exception e) {
            log.error("MailSinkTask | start | error => ", e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public void stop() {
        client = null;
    }
}
