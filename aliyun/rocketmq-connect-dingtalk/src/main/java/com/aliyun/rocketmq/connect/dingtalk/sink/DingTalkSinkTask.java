package com.aliyun.rocketmq.connect.dingtalk.sink;

import com.alibaba.fastjson.JSON;
import com.aliyun.rocketmq.connect.dingtalk.sink.common.OkHttpUtils;
import com.aliyun.rocketmq.connect.dingtalk.sink.constant.DingTalkConstant;
import io.openmessaging.KeyValue;
import io.openmessaging.connector.api.component.task.sink.SinkTask;
import io.openmessaging.connector.api.component.task.sink.SinkTaskContext;
import io.openmessaging.connector.api.data.ConnectRecord;
import io.openmessaging.connector.api.errors.ConnectException;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DingTalkSinkTask extends SinkTask {
    private static final Logger log = LoggerFactory.getLogger(DingTalkSinkTask.class);

    private String webHook;

    private String msgType;

    @Override
    public void put(List<ConnectRecord> sinkRecords) throws ConnectException {
        try {
            sinkRecords.forEach(sinkRecord -> {
                Map<String, Object> objectMap = new HashMap<>();
                objectMap.put(DingTalkConstant.CONTENT_CONSTANT, sinkRecord.getData());
                OkHttpUtils.builder()
                        .url(webHook)
                        .addParam(DingTalkConstant.MSG_TYPE_CONSTANT, msgType)
                        .addParam(msgType, JSON.toJSONString(objectMap))
                        .addHeader(DingTalkConstant.CONTENT_TYPE, DingTalkConstant.APPLICATION_JSON_UTF_8_TYPE)
                        .post(true)
                        .sync();
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
        if (StringUtils.isBlank(webHook)) {
            throw new RuntimeException("ding talk required parameter is null !");
        }
    }

    @Override
    public void init(KeyValue config) {
        webHook = config.getString(DingTalkConstant.WEB_HOOK);
        msgType = config.getString(DingTalkConstant.MSG_TYPE_CONSTANT, "text");
    }

    @Override
    public void start(SinkTaskContext sinkTaskContext) {
        super.start(sinkTaskContext);
    }

    @Override
    public void stop() {

    }
}
