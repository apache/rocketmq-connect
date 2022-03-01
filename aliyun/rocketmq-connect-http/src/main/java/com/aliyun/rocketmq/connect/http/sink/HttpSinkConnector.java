package com.aliyun.rocketmq.connect.http.sink;

import com.aliyun.rocketmq.connect.http.sink.constant.HttpConstant;
import io.openmessaging.KeyValue;
import io.openmessaging.connector.api.component.task.Task;
import io.openmessaging.connector.api.component.task.sink.SinkConnector;
import io.openmessaging.internal.DefaultKeyValue;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.List;

public class HttpSinkConnector extends SinkConnector {

    private String url;

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
        keyValue.put(HttpConstant.URL_CONSTANT, url);
        keyValueList.add(keyValue);
        return keyValueList;
    }

    @Override
    public Class<? extends Task> taskClass() {
        return HttpSinkTask.class;
    }

    @Override
    public void validate(KeyValue config) {
        if (StringUtils.isBlank(config.getString(HttpConstant.URL_CONSTANT))) {
            throw new RuntimeException("http required parameter is null !");
        }
    }

    @Override
    public void init(KeyValue config) {
        url = config.getString(HttpConstant.URL_CONSTANT);
    }

    @Override
    public void stop() {

    }
}
