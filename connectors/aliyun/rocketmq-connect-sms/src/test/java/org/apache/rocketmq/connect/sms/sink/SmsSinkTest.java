package org.apache.rocketmq.connect.sms.sink;

import io.openmessaging.KeyValue;
import io.openmessaging.connector.api.component.task.sink.SinkTaskContext;
import io.openmessaging.connector.api.data.ConnectRecord;
import io.openmessaging.connector.api.data.RecordOffset;
import io.openmessaging.connector.api.data.RecordPartition;
import io.openmessaging.internal.DefaultKeyValue;
import org.apache.rocketmq.connect.sms.sink.constant.SmsConstant;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class SmsSinkTest {

    @Test
    public void testTaskConfigs() {
        SmsSinkConnector smsSinkConnector = new SmsSinkConnector();
        Assert.assertEquals(smsSinkConnector.taskConfigs(1).size(), 1);
    }

    @Test
    public void testPut() {
        SmsSinkTask smsSinkTask = new SmsSinkTask();
        KeyValue keyValue = new DefaultKeyValue();
        keyValue.put(SmsConstant.ACCESS_KEY_ID, "");
        keyValue.put(SmsConstant.ACCESS_KEY_SECRET, "");
        keyValue.put(SmsConstant.ACCOUNT_ENDPOINT, "");
        keyValue.put(SmsConstant.PHONE_NUMBERS, "");
        keyValue.put(SmsConstant.SIGN_NAME, "");
        keyValue.put(SmsConstant.TEMPLATE_CODE, "");
        smsSinkTask.init(keyValue);
        List<ConnectRecord> connectRecordList = new ArrayList<>();
        ConnectRecord connectRecord = new ConnectRecord(null, null, System.currentTimeMillis());
        connectRecord.setData("{\n" +
                "\t'code' : '112233'\n" +
                "}");
        connectRecordList.add(connectRecord);
        smsSinkTask.start(new SinkTaskContext() {
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
            public void resetOffset(Map<RecordPartition, RecordOffset> offsets) {

            }

            @Override
            public void pause(List<RecordPartition> partitions) {

            }

            @Override
            public void resume(List<RecordPartition> partitions) {

            }

            @Override
            public Set<RecordPartition> assignment() {
                return null;
            }
        });
        smsSinkTask.put(connectRecordList);
    }

}
