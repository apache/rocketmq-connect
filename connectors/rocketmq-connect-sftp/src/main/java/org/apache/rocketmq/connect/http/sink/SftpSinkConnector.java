package org.apache.rocketmq.connect.http.sink;

import io.openmessaging.KeyValue;
import io.openmessaging.connector.api.component.task.Task;
import io.openmessaging.connector.api.component.task.sink.SinkConnector;
import io.openmessaging.internal.DefaultKeyValue;
import org.apache.commons.lang3.StringUtils;

import java.net.URL;
import java.net.URLConnection;
import java.util.ArrayList;
import java.util.List;

public class SftpSinkConnector extends SinkConnector {

    private String url;

    @Override
    public List<KeyValue> taskConfigs(int maxTasks) {
        List<KeyValue> keyValueList = new ArrayList<>(11);
        KeyValue keyValue = new DefaultKeyValue();
        keyValue.put(SftpConstant.URL_CONSTANT, url);
        keyValueList.add(keyValue);
        return keyValueList;
    }

    @Override
    public Class<? extends Task> taskClass() {
        return SftpSinkTask.class;
    }

    @Override
    public void validate(KeyValue config) {
        if (StringUtils.isBlank(config.getString(SftpConstant.URL_CONSTANT))) {
            throw new RuntimeException("http required parameter is null !");
        }
        try {
            URL urlConnect = new URL(config.getString(SftpConstant.URL_CONSTANT));
            URLConnection urlConnection = urlConnect.openConnection();
            urlConnection.setConnectTimeout(5000);
            urlConnection.connect();
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage());
        }
    }

    @Override
    public void start(KeyValue keyValue) {

    }

    @Override
    public void stop() {

    }
}
