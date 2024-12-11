package org.apache.rocketmq.connect.doris.utils;

import io.openmessaging.connector.api.data.RecordOffset;
import io.openmessaging.connector.api.data.RecordPartition;
import java.util.Map;

public class ConnectRecordUtil {
    public static final String TOPIC = "topic";
    public static final String BROKER_NAME = "brokerName";
    public static final String QUEUE_ID = "queueId";
    public static final String QUEUE_OFFSET = "queueOffset";

    public static long getQueueOffset(RecordOffset recordOffset) {
        Map<String, ?> offset = recordOffset.getOffset();
        if (offset.containsKey(QUEUE_OFFSET)) {
            return Long.parseLong((String) offset.get(QUEUE_OFFSET));
        }
        return -1;
    }

    public static String getTopicName(RecordPartition recordPartition) {
        Map<String, ?> partition = recordPartition.getPartition();
        return (String) partition.get(TOPIC);
    }

    public static String getBrokerName(RecordPartition recordPartition) {
        Map<String, ?> partition = recordPartition.getPartition();
        return (String) partition.get(BROKER_NAME);
    }

    public static String getQueueId(RecordPartition recordPartition) {
        Map<String, ?> partition = recordPartition.getPartition();
        return (String) partition.get(QUEUE_ID);
    }

}
