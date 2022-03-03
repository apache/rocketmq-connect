package com.aliyun.rocketmq.connect.dingtalk.sink;

import org.junit.Assert;
import org.junit.Test;

public class DingTalkSinkConnectorTask {

    private DingTalkSinkConnector dingTalkSinkConnector = new DingTalkSinkConnector();

    @Test
    public void taskTaskConfigs() {
        Assert.assertEquals(dingTalkSinkConnector.taskConfigs(1).size(), 1);
    }

}
