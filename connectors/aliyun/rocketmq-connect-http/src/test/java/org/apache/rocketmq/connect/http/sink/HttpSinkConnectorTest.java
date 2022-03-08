package org.apache.rocketmq.connect.http.sink;

import org.junit.Assert;
import org.junit.Test;

public class HttpSinkConnectorTest {

    private final HttpSinkConnector httpSinkConnector = new HttpSinkConnector();

    @Test
    public void testTaskConfigs() {
        Assert.assertEquals(httpSinkConnector.taskConfigs(1).size(), 1);
    }

}
