package org.apache.rocketmq.connect.eventbridge.sink;

import io.openmessaging.KeyValue;
import io.openmessaging.connector.api.component.task.sink.SinkTaskContext;
import io.openmessaging.connector.api.data.ConnectRecord;
import io.openmessaging.connector.api.data.RecordOffset;
import io.openmessaging.connector.api.data.RecordPartition;
import io.openmessaging.internal.DefaultKeyValue;
import org.apache.rocketmq.connect.eventbridge.sink.constant.EventBridgeConstant;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;


public class EventBridgeSinkTest {

    @Test
    public void testTaskConfigs() {
        EventBridgeSinkConnector eventBridgeSinkConnector = new EventBridgeSinkConnector();
        Assert.assertEquals(eventBridgeSinkConnector.taskConfigs(1).size(), 1);
    }

    @Test
    public void testPut() {
        EventBridgeSinkTask eventBridgeSinkTask = new EventBridgeSinkTask();
        KeyValue keyValue = new DefaultKeyValue();
        keyValue.put(EventBridgeConstant.ACCESS_KEY_ID, "xxxx");
        keyValue.put(EventBridgeConstant.STS_ENDPOINT, "xxxx");
        keyValue.put(EventBridgeConstant.ACCOUNT_ENDPOINT, "xxxx");
        keyValue.put(EventBridgeConstant.ACCESS_KEY_SECRET, "xxxx");
        keyValue.put(EventBridgeConstant.ROLE_ARN, "xxxx");
        keyValue.put(EventBridgeConstant.ROLE_SESSION_NAME, "xxxx");
        keyValue.put(EventBridgeConstant.EVENT_SUBJECT, "xxxx");
        keyValue.put(EventBridgeConstant.ALIYUN_EVENT_BUS_NAME, "xxxx");
        eventBridgeSinkTask.init(keyValue);
        List<ConnectRecord> connectRecordList = new ArrayList<>();
        ConnectRecord connectRecord = new ConnectRecord(null, null, System.currentTimeMillis());
        connectRecord.setData("{\n" +
                "\t\"test\" :  \"test\"\n" +
                "}");
        connectRecord.addExtension(EventBridgeConstant.EVENT_ID, UUID.randomUUID().toString());
        connectRecord.addExtension(EventBridgeConstant.EVENT_SOURCE, "xxxx");
        connectRecord.addExtension(EventBridgeConstant.EVENT_TYPE, "xxxx");
        connectRecord.addExtension(EventBridgeConstant.EVENT_TIME, "2022-04-24 16:12:00");
        connectRecordList.add(connectRecord);
        eventBridgeSinkTask.start(new SinkTaskContext() {
            @Override
            public String getConnectorName() {
                return null;
            }

            @Override
            public String getTaskName() {
                return null;
            }

            @Override
            public void resetOffset(RecordPartition recordPartition, RecordOffset recordOffset) {

            }

            @Override
            public void resetOffset(Map<RecordPartition, RecordOffset> map) {

            }

            @Override
            public void pause(List<RecordPartition> list) {

            }

            @Override
            public void resume(List<RecordPartition> list) {

            }

            @Override
            public Set<RecordPartition> assignment() {
                return null;
            }
        });
        eventBridgeSinkTask.put(connectRecordList);
    }

}
