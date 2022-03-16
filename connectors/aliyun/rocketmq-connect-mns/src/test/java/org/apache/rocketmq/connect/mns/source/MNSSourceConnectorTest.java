package org.apache.rocketmq.connect.mns.source;

import io.openmessaging.KeyValue;
import io.openmessaging.connector.api.component.task.source.SourceTaskContext;
import io.openmessaging.connector.api.data.ConnectRecord;
import io.openmessaging.connector.api.storage.OffsetStorageReader;
import io.openmessaging.internal.DefaultKeyValue;
import org.apache.rocketmq.connect.mns.source.constant.MNSConstant;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;


public class MNSSourceConnectorTest {

    private final MNSSourceConnector mnsSourceConnector = new MNSSourceConnector();

    @Test
    public void testTaskConfigs() {
        Assert.assertEquals(mnsSourceConnector.taskConfigs(1).size(), 1);
    }

    @Test
    public void testPut() throws InterruptedException {
        MNSSourceTask mnsSourceTask = new MNSSourceTask();
        KeyValue keyValue = new DefaultKeyValue();
        keyValue.put(MNSConstant.ACCESS_KEY_ID, "xxxx");
        keyValue.put(MNSConstant.ACCESS_KEY_SECRET, "xxxx");
        keyValue.put(MNSConstant.ACCOUNT_ENDPOINT, "xxxx");
        keyValue.put(MNSConstant.ACCOUNT_ID, "xxxx");
        keyValue.put(MNSConstant.QUEUE_NAME, "xxxx");
        mnsSourceTask.init(keyValue);
        mnsSourceTask.start(new SourceTaskContext() {
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
        List<ConnectRecord> poll = mnsSourceTask.poll();
        mnsSourceTask.commit(poll);
    }
}
