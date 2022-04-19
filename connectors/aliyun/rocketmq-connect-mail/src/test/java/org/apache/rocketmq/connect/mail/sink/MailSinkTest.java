package org.apache.rocketmq.connect.mail.sink;

import io.openmessaging.KeyValue;
import io.openmessaging.connector.api.component.task.sink.SinkTaskContext;
import io.openmessaging.connector.api.data.ConnectRecord;
import io.openmessaging.connector.api.data.RecordOffset;
import io.openmessaging.connector.api.data.RecordPartition;
import io.openmessaging.internal.DefaultKeyValue;
import org.apache.rocketmq.connect.mail.sink.constant.MailConstant;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class MailSinkTest {

    @Test
    public void testTaskConfigs() {
        MailSinkConnector mailSinkConnector = new MailSinkConnector();
        Assert.assertEquals(mailSinkConnector.taskConfigs(1).size(), 1);
    }

    @Test
    public void testPut() {
        MailSinkTask mailSinkTask = new MailSinkTask();
        KeyValue keyValue = new DefaultKeyValue();
        keyValue.put(MailConstant.ACCESS_KEY_ID, "xxxx");
        keyValue.put(MailConstant.ACCESS_KEY_SECRET, "xxxx");
        keyValue.put(MailConstant.ACCOUNT_ENDPOINT, "xxxx");
        keyValue.put(MailConstant.ACCOUNT_NAME, "xxxx");
        keyValue.put(MailConstant.SUBJECT, "xxxx");
        keyValue.put(MailConstant.TO_ADDRESS, "xxxx");
        keyValue.put(MailConstant.ADDRESS_TYPE, "0");
        keyValue.put(MailConstant.REPLY_TO_ADDRESS, "true");
        keyValue.put(MailConstant.IS_HTML_BODY, "true");
        mailSinkTask.init(keyValue);
        List<ConnectRecord> connectRecordList = new ArrayList<>();
        ConnectRecord connectRecord = new ConnectRecord(null, null, System.currentTimeMillis());
        connectRecord.setData("test mail");
        connectRecordList.add(connectRecord);
        mailSinkTask.start(new SinkTaskContext() {
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
        mailSinkTask.put(connectRecordList);
    }
}
