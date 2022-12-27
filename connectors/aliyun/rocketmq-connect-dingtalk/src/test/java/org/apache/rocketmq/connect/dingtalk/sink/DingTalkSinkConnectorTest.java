package org.apache.rocketmq.connect.dingtalk.sink;

import com.alibaba.fastjson.JSON;
import io.openmessaging.KeyValue;
import io.openmessaging.connector.api.data.ConnectRecord;
import io.openmessaging.internal.DefaultKeyValue;
import org.apache.rocketmq.connect.dingtalk.sink.constant.DingTalkConstant;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DingTalkSinkConnectorTest {

    private DingTalkSinkConnector dingTalkSinkConnector = new DingTalkSinkConnector();

    @Test
    public void testTaskConfigs() {
        Assert.assertEquals(dingTalkSinkConnector.taskConfigs(1).size(), 1);
    }

    @Test
    public void testPut() {
        DingTalkSinkTask dingTalkSinkTask = new DingTalkSinkTask();
        KeyValue keyValue = new DefaultKeyValue();
        // Replace it with your own robot webhook.
        keyValue.put("webHook", "https://oapi.dingtalk.com/robot/send?access_token=7f78aa4734ea9bd245984e47b6764ccb950b4292e4f6f9424dff92909f485f16");
        keyValue.put("secretKey", "SEC8a898c9df7b6415090a8f1341d9eed000c815a89f301f2de87302a1e802dbd69");
        dingTalkSinkTask.start(keyValue);
        Map<String, Object> map = new HashMap<>();
        map.put("msgtype", "text");
        Map<String, String> map1 = new HashMap<>();
        map1.put("content", "小桥流水，哗啦啦");
        map.put("text", map1);
        List<ConnectRecord> connectRecordList = new ArrayList<>();
        ConnectRecord connectRecord = new ConnectRecord(null, null, System.currentTimeMillis());
        connectRecord.setData(JSON.toJSONString(map));
        connectRecordList.add(connectRecord);
        dingTalkSinkTask.put(connectRecordList);
    }

    @Test(expected = RuntimeException.class)
    public void testValidate() {
        KeyValue keyValue = new DefaultKeyValue();
        // 需要添加测试的web_hook地址
        keyValue.put(DingTalkConstant.WEB_HOOK, "http://127.0.0.1");
        dingTalkSinkConnector.validate(keyValue);
    }
}
