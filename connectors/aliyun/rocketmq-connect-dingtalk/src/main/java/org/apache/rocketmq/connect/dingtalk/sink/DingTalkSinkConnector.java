package org.apache.rocketmq.connect.dingtalk.sink;

import org.apache.rocketmq.connect.dingtalk.sink.constant.DingTalkConstant;
import io.openmessaging.KeyValue;
import io.openmessaging.connector.api.component.task.Task;
import io.openmessaging.connector.api.component.task.sink.SinkConnector;
import io.openmessaging.internal.DefaultKeyValue;
import org.apache.commons.lang3.StringUtils;

import java.net.URL;
import java.net.URLConnection;
import java.util.ArrayList;
import java.util.List;

public class DingTalkSinkConnector extends SinkConnector {

    private String webHook;

    private String secretKey;

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
        keyValue.put(DingTalkConstant.SECRET_KEY, secretKey);
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
        try {
            URL urlConnect = new URL(config.getString(DingTalkConstant.WEB_HOOK));
            URLConnection urlConnection = urlConnect.openConnection();
            urlConnection.setConnectTimeout(5000);
            urlConnection.connect();
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage());
        }
    }

    @Override
    public void init(KeyValue config) {
        webHook = config.getString(DingTalkConstant.WEB_HOOK);
        secretKey = config.getString(DingTalkConstant.SECRET_KEY);
    }

    @Override
    public void stop() {

    }
}
