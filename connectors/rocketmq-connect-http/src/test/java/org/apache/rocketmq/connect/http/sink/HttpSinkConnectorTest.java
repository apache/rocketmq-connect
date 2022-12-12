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
}
