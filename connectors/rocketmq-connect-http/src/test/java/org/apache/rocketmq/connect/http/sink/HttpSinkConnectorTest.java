package org.apache.rocketmq.connect.http.sink;

import io.openmessaging.KeyValue;
import io.openmessaging.connector.api.data.ConnectRecord;
import io.openmessaging.internal.DefaultKeyValue;
import org.apache.rocketmq.connect.http.sink.constant.HttpConstant;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class HttpSinkConnectorTest {

    private final HttpSinkConnector httpSinkConnector = new HttpSinkConnector();

    @Test
    public void testTaskConfigs() {
        Assert.assertEquals(httpSinkConnector.taskConfigs(1).size(), 1);
    }

    @Test
    public void testPut() {
        HttpSinkTask httpSinkTask = new HttpSinkTask();
        KeyValue keyValue = new DefaultKeyValue();
        keyValue.put(HttpConstant.URL_CONSTANT, "http://127.0.0.1:8081/demo");
        httpSinkTask.init(keyValue);
        List<ConnectRecord> connectRecordList = new ArrayList<>();
        ConnectRecord connectRecord = new ConnectRecord(null ,null, System.currentTimeMillis());
        connectRecord.setData("test");
        connectRecordList.add(connectRecord);
        httpSinkTask.put(connectRecordList);
    }

    @Test(expected = RuntimeException.class)
    public void testValidate() {
        KeyValue keyValue = new DefaultKeyValue();
        // 需要添加测试的http地址
        keyValue.put(HttpConstant.URL_CONSTANT, "http://127.0.0.1");
        httpSinkConnector.validate(keyValue);
    }
}
