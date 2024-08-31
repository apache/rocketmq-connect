package org.apache.rocketmq.connect.oss.sink;

import io.openmessaging.KeyValue;
import io.openmessaging.connector.api.component.task.sink.SinkTaskContext;
import io.openmessaging.connector.api.data.ConnectRecord;
import io.openmessaging.connector.api.data.RecordOffset;
import io.openmessaging.connector.api.data.RecordPartition;
import io.openmessaging.internal.DefaultKeyValue;
import org.apache.rocketmq.connect.oss.sink.constant.OssConstant;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;


public class OssSinkTest {
    @Test
    public void testTaskConfigs() {
        OssSinkConnector ossSinkConnector = new OssSinkConnector();
        Assert.assertEquals(ossSinkConnector.taskConfigs(1).size(), 1);
    }

    @Test
    public void testNormalPut() {
        OssSinkTask ossSinkTask = new OssSinkTask();
        KeyValue keyValue = new DefaultKeyValue();
        // Replace KV pair with your own message
        keyValue.put(OssConstant.ACCESS_KEY_ID, "LTAI5t68yKJXx6HbkrKowqe8");
        keyValue.put(OssConstant.ACCESS_KEY_SECRET, "eiDUU47CIJ0ShVX2zzl3KhehyscrSY");
        keyValue.put(OssConstant.ACCOUNT_ENDPOINT, "oss-cn-beijing.aliyuncs.com");
        keyValue.put(OssConstant.BUCKET_NAME, "rocketmqoss");
        keyValue.put(OssConstant.FILE_URL_PREFIX, "test/");
        keyValue.put(OssConstant.OBJECT_NAME, "oss_new.txt");
        keyValue.put(OssConstant.REGION, "cn-beijing");
        keyValue.put(OssConstant.PARTITION_METHOD, "Normal");

        ossSinkTask.start(keyValue);
        List<ConnectRecord> connectRecordList = new ArrayList<>();
        ConnectRecord connectRecord = new ConnectRecord(null, null, System.currentTimeMillis());
        connectRecord.setData("{\n" +
                "\t\"test\" :  \"test\"\n" +
                "}");
        connectRecordList.add(connectRecord);
        ossSinkTask.init(new SinkTaskContext() {
            @Override
            public String getConnectorName() {
                return null;
            }

            @Override
            public String getTaskName() {
                return null;
            }

            @Override public KeyValue configs() {
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
        ossSinkTask.put(connectRecordList);
    }

    @Test
    public void testTimePut() {
        OssSinkTask ossSinkTask = new OssSinkTask();
        KeyValue keyValue = new DefaultKeyValue();
        // Replace KV pair with your own message
        keyValue.put(OssConstant.ACCESS_KEY_ID, "LTAI5t68yKJXx6HbkrKowqe8");
        keyValue.put(OssConstant.ACCESS_KEY_SECRET, "eiDUU47CIJ0ShVX2zzl3KhehyscrSY");
        keyValue.put(OssConstant.ACCOUNT_ENDPOINT, "oss-cn-beijing.aliyuncs.com");
        keyValue.put(OssConstant.BUCKET_NAME, "rocketmqoss");
        keyValue.put(OssConstant.FILE_URL_PREFIX, "test/");
        keyValue.put(OssConstant.OBJECT_NAME, "oss_new.txt");
        keyValue.put(OssConstant.REGION, "cn-beijing");
        keyValue.put(OssConstant.PARTITION_METHOD, "Time");

        ossSinkTask.start(keyValue);
        List<ConnectRecord> connectRecordList = new ArrayList<>();
        ConnectRecord connectRecord = new ConnectRecord(null, null, System.currentTimeMillis());
        connectRecord.setData("{\n" +
                "\t\"test\" :  \"test\"\n" +
                "}");
        connectRecordList.add(connectRecord);
        ossSinkTask.init(new SinkTaskContext() {
            @Override
            public String getConnectorName() {
                return null;
            }

            @Override
            public String getTaskName() {
                return null;
            }

            @Override public KeyValue configs() {
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
        ossSinkTask.put(connectRecordList);
    }


}
