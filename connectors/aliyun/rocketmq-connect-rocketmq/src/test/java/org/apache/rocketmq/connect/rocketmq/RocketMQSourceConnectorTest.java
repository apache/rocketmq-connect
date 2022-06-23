package org.apache.rocketmq.connect.rocketmq;

import io.openmessaging.KeyValue;
import io.openmessaging.connector.api.component.task.source.SourceTaskContext;
import io.openmessaging.connector.api.data.ConnectRecord;
import io.openmessaging.connector.api.data.RecordOffset;
import io.openmessaging.connector.api.data.RecordPartition;
import io.openmessaging.connector.api.storage.OffsetStorageReader;
import io.openmessaging.internal.DefaultKeyValue;
import org.apache.rocketmq.connect.rocketmq.common.RocketMQConstant;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class RocketMQSourceConnectorTest {

    @Test
    public void testTaskConfigs() {
        RocketMQSourceConnector rocketMQSourceConnector = new RocketMQSourceConnector();
        Assert.assertEquals(rocketMQSourceConnector.taskConfigs(1).size(), 1);
    }

    @Test
    public void testPut() throws InterruptedException {
        RocketMQSourceTask rocketMQSourceTask  = new RocketMQSourceTask();
        KeyValue keyValue = new DefaultKeyValue();
        keyValue.put(RocketMQConstant.ACCESS_KEY_ID, "xxxx");
        keyValue.put(RocketMQConstant.ACCESS_KEY_SECRET, "xxxx");
        keyValue.put(RocketMQConstant.NAMESRV_ADDR, "xxxx");
        keyValue.put(RocketMQConstant.TOPIC, "xxxx");
        // 有实例Id需要填写实例Id
        keyValue.put(RocketMQConstant.INSTANCE_ID, "xxxx");
        keyValue.put(RocketMQConstant.CONSUMER_GROUP, "xxxx");
        rocketMQSourceTask.init(keyValue);
        rocketMQSourceTask.start(new SourceTaskContext() {
            @Override
            public OffsetStorageReader offsetStorageReader() {
                return null;
            }

            @Override
            public String getConnectorName() {
                return null;
            }

            @Override
            public String getTaskName() {
                return null;
            }
        });
        List<ConnectRecord> connectRecords = new ArrayList<>(11);
        ConnectRecord connectRecord = new ConnectRecord(new RecordPartition(new HashMap<>()), new RecordOffset(new HashMap<>()), System.currentTimeMillis());
        connectRecord.addExtension(RocketMQConstant.TAG, "*");
        connectRecords.add(connectRecord);
        rocketMQSourceTask.commit(connectRecords);
        rocketMQSourceTask.poll();
        Thread.sleep(50000);
    }

    @Test
    public void testValidate() {
        RocketMQSourceTask rocketMQSourceTask = new RocketMQSourceTask();
        KeyValue keyValue = new DefaultKeyValue();
        keyValue.put(RocketMQConstant.ACCESS_KEY_ID, "xxxx");
        keyValue.put(RocketMQConstant.ACCESS_KEY_SECRET, "xxxx");
        keyValue.put(RocketMQConstant.NAMESRV_ADDR, "xxxx");
        keyValue.put(RocketMQConstant.TOPIC, "xxxx");
        // 有实例Id需要填写实例Id
        keyValue.put(RocketMQConstant.INSTANCE_ID, "xxxx");
        keyValue.put(RocketMQConstant.CONSUMER_GROUP, "xxxx");
        rocketMQSourceTask.validate(keyValue);
    }
}
