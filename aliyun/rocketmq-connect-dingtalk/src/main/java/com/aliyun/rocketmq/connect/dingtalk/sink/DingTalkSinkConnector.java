package com.aliyun.rocketmq.connect.dingtalk.sink;

import com.aliyun.rocketmq.connect.dingtalk.sink.constant.DingTalkConstant;
import io.openmessaging.KeyValue;
import io.openmessaging.connector.api.component.task.Task;
import io.openmessaging.connector.api.component.task.sink.SinkConnector;
import io.openmessaging.internal.DefaultKeyValue;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.List;

public class DingTalkSinkConnector extends SinkConnector {

    private String webHook;

    private String msgType;

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
        keyValue.put(DingTalkConstant.WEB_HOOK, webHook);
        keyValue.put(DingTalkConstant.MSG_TYPE_CONSTANT, msgType);
        taskConfigList.add(keyValue);
        return taskConfigList;
    }

    @Override
    public Class<? extends Task> taskClass() {
        return DingTalkSinkTask.class;
    }

    @Override
    public void validate(KeyValue config) {
        if (StringUtils.isBlank(config.getString(DingTalkConstant.WEB_HOOK))) {
            throw new RuntimeException("ding talk required parameter is null !");
        }
    }

    @Override
    public void init(KeyValue config) {
        webHook = config.getString(DingTalkConstant.WEB_HOOK);
        msgType = config.getString(DingTalkConstant.MSG_TYPE_CONSTANT, "text");
    }

    @Override
    public void stop() {

    }
}
