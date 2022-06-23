package org.apache.rocketmq.connect.dingtalk.sink;

import org.apache.rocketmq.connect.dingtalk.sink.common.OkHttpUtils;
import org.apache.rocketmq.connect.dingtalk.sink.constant.DingTalkConstant;
import io.openmessaging.KeyValue;
import io.openmessaging.connector.api.component.task.sink.SinkTask;
import io.openmessaging.connector.api.component.task.sink.SinkTaskContext;
import io.openmessaging.connector.api.data.ConnectRecord;
import io.openmessaging.connector.api.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.crypto.Mac;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;

public class DingTalkSinkTask extends SinkTask {
    private static final Logger log = LoggerFactory.getLogger(DingTalkSinkTask.class);

    private String webHook;

    private String secretKey;

    @Override
    public void put(List<ConnectRecord> sinkRecords) throws ConnectException {
        try {
            sinkRecords.forEach(sinkRecord -> {
                try {
                    String sync = OkHttpUtils.builder()
                            .url(getWebHook())
                            .addHeader(DingTalkConstant.CONTENT_TYPE, DingTalkConstant.APPLICATION_JSON_UTF_8_TYPE)
                            .postForStringBody(sinkRecord.getData())
                            .sync();
                    log.info("DingTalkSinkTask put sync : {}", sync);
                } catch (Exception e) {
                    log.error("DingTalkSinkTask | put | addParam | error => ", e);
                }
            });
        } catch (Exception e) {
            log.error("DingTalkSinkTask | put | error => ", e);
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
        webHook = config.getString(DingTalkConstant.WEB_HOOK);
        secretKey = config.getString(DingTalkConstant.SECRET_KEY);
    }

    @Override
    public void start(SinkTaskContext sinkTaskContext) {
        super.start(sinkTaskContext);
    }

    @Override
    public void stop() {

    }

    private String signByHmacSHA256(String secretKey, long timestamp) throws Exception {
        String strToSign = timestamp + "\n" + secretKey;
        byte[] data = secretKey.getBytes(StandardCharsets.UTF_8);
        SecretKey secret = new SecretKeySpec(data, DingTalkConstant.HMAC_SHA256_CONSTANT);
        Mac mac = Mac.getInstance(DingTalkConstant.HMAC_SHA256_CONSTANT);
        mac.init(secret);
        byte[] bytes = strToSign.getBytes(StandardCharsets.UTF_8);
        return Base64.getEncoder().encodeToString(mac.doFinal(bytes));
    }

    public String getWebHook() throws Exception {
        long timeMillis = System.currentTimeMillis();
        return webHook + "&" + DingTalkConstant.TIMESTAMP_CONSTANT + "=" + timeMillis + "&" + DingTalkConstant.SIGN_CONSTANT + "=" + signByHmacSHA256(secretKey, timeMillis);
    }
}
