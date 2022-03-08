package org.apache.rocketmq.connect.mns.source;

import org.junit.Assert;
import org.junit.Test;

public class MNSSourceConnectorTest {

    private final MNSSourceConnector mnsSourceConnector = new MNSSourceConnector();

    @Test
    public void taskConfigsTest() {
        Assert.assertEquals(mnsSourceConnector.taskConfigs(1).size(), 1);
    }

}
