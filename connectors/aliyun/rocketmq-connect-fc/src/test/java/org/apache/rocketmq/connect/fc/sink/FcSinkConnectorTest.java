package org.apache.rocketmq.connect.fc.sink;

import org.apache.rocketmq.connect.fc.sink.constant.FcConstant;
import io.openmessaging.KeyValue;
import io.openmessaging.internal.DefaultKeyValue;
import org.junit.Assert;
import org.junit.Test;

public class FcSinkConnectorTest {

    private final FcSinkConnector fcSinkConnector = new FcSinkConnector();

    @Test
    public void testTaskConfigs() {
        Assert.assertEquals(fcSinkConnector.taskConfigs(1).size(), 1);
    }

    @Test
    public void testInit() {
        KeyValue keyValue = new DefaultKeyValue();
        keyValue.put(FcConstant.REGION_ID_CONSTANT, FcConstant.REGION_ID_CONSTANT);
        keyValue.put(FcConstant.ACCESS_KEY_ID_CONSTANT, FcConstant.ACCESS_KEY_ID_CONSTANT);
        keyValue.put(FcConstant.ACCESS__KEY_SECRET_CONSTANT, FcConstant.ACCESS__KEY_SECRET_CONSTANT);
        keyValue.put(FcConstant.ACCOUNT_ID_CONSTANT, FcConstant.ACCOUNT_ID_CONSTANT);
        keyValue.put(FcConstant.SERVICE_NAME_CONSTANT, FcConstant.SERVICE_NAME_CONSTANT);
        keyValue.put(FcConstant.FUNCTION_NAME_CONSTANT, FcConstant.FUNCTION_NAME_CONSTANT);
        keyValue.put(FcConstant.INVOCATION_TYPE_CONSTANT, FcConstant.INVOCATION_TYPE_CONSTANT);
        keyValue.put(FcConstant.QUALIFIER_CONSTANT, FcConstant.QUALIFIER_CONSTANT);
        fcSinkConnector.init(keyValue);
        Assert.assertEquals(fcSinkConnector.taskConfigs(1).get(0).getString(FcConstant.REGION_ID_CONSTANT), FcConstant.REGION_ID_CONSTANT);
    }
}
