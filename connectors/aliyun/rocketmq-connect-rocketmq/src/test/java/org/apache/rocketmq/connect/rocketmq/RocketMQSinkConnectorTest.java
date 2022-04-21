package org.apache.rocketmq.connect.rocketmq;

import io.openmessaging.KeyValue;
import io.openmessaging.connector.api.component.task.sink.SinkTaskContext;
import io.openmessaging.connector.api.data.ConnectRecord;
import io.openmessaging.connector.api.data.RecordOffset;
import io.openmessaging.connector.api.data.RecordPartition;
import io.openmessaging.internal.DefaultKeyValue;
import org.apache.rocketmq.connect.rocketmq.common.RocketMQConstant;
import org.assertj.core.util.Lists;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class RocketMQSinkConnectorTest {

    @Test
    public void testTaskConfigs() {
        RocketMQSinkConnector rocketMQSinkConnector= new RocketMQSinkConnector();
        Assert.assertEquals(rocketMQSinkConnector.taskConfigs(1).size(), 1);
    }

    @Test
    public void testPut() {
        RocketMQSinkTask rocketMQSinkTask = new RocketMQSinkTask();
        KeyValue keyValue = new DefaultKeyValue();
        keyValue.put(RocketMQConstant.ACCESS_KEY_ID, "xxxx");
        keyValue.put(RocketMQConstant.ACCESS_KEY_SECRET, "xxxx");
        keyValue.put(RocketMQConstant.NAMESRV_ADDR, "xxxx");
        keyValue.put(RocketMQConstant.TOPIC, "xxxx");
        // 有实例Id需要填写实例Id
        keyValue.put(RocketMQConstant.INSTANCE_ID, "xxxx");
        rocketMQSinkTask.init(keyValue);
        rocketMQSinkTask.start(new SinkTaskContext() {
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
        List<ConnectRecord> connectRecords = Lists.newArrayList();
        ConnectRecord connectRecord = new ConnectRecord(new RecordPartition(new HashMap<>()), new RecordOffset(new HashMap<>()), System.currentTimeMillis());
        connectRecord.setData("test message");
        connectRecords.add(connectRecord);
        connectRecord.addExtension(RocketMQConstant.KEY, "value");
        connectRecord.addExtension(RocketMQConstant.TAG, "tag");
        rocketMQSinkTask.put(connectRecords);
    }

    @Test
    public void testValidate() {
        RocketMQSinkTask rocketMQSinkTask = new RocketMQSinkTask();
        KeyValue keyValue = new DefaultKeyValue();
        keyValue.put(RocketMQConstant.ACCESS_KEY_ID, "xxxx");
        keyValue.put(RocketMQConstant.ACCESS_KEY_SECRET, "xxxx");
        keyValue.put(RocketMQConstant.NAMESRV_ADDR, "xxxx");
        keyValue.put(RocketMQConstant.TOPIC, "xxxx");
        // 有实例Id需要填写实例Id
        keyValue.put(RocketMQConstant.INSTANCE_ID, "xxxx");
        rocketMQSinkTask.validate(keyValue);
    }

}
