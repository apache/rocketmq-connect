package org.apache.rocketmq.connect.http.sink;

import org.apache.rocketmq.connect.http.sink.constant.HttpConstant;
import io.openmessaging.KeyValue;
import io.openmessaging.connector.api.component.task.Task;
import io.openmessaging.connector.api.component.task.sink.SinkConnector;
import io.openmessaging.internal.DefaultKeyValue;
import org.apache.commons.lang3.StringUtils;

import java.net.URL;
import java.net.URLConnection;
import java.util.ArrayList;
import java.util.List;

public class HttpSinkConnector extends SinkConnector {

    private KeyValue connectConfig;

    @Override
    public List<KeyValue> taskConfigs(int maxTasks) {
        List<KeyValue> keyValueList = new ArrayList<>(11);
        KeyValue keyValue = new DefaultKeyValue();
        for (String key : connectConfig.keySet()) {
            keyValue.put(key, connectConfig.getString(key));
        }
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
        try {
            URL urlConnect = new URL(config.getString(HttpConstant.URL_CONSTANT));
            URLConnection urlConnection = urlConnect.openConnection();
            urlConnection.setConnectTimeout(5000);
            urlConnection.connect();
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage());
        }
    }

    @Override
    public void start(KeyValue config) {
        this.connectConfig = config;
    }

    @Override
    public void stop() {

    }
}
