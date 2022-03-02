package com.aliyun.rocketmq.connect.fc.sink;

import org.junit.Assert;
import org.junit.Test;

public class FcSinkConnectorTest {

    private final FcSinkConnector fcSinkConnector = new FcSinkConnector();

    @Test
    public void testTaskConfigs() {
        Assert.assertEquals(fcSinkConnector.taskConfigs(1).size(), 1);
    }
}
