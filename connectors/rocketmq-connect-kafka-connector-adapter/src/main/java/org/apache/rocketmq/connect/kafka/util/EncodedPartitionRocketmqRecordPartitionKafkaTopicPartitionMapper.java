package org.apache.rocketmq.connect.kafka.util;

import io.openmessaging.connector.api.data.RecordPartition;
import org.apache.kafka.common.TopicPartition;

import java.util.HashMap;
import java.util.Map;

public abstract class EncodedPartitionRocketmqRecordPartitionKafkaTopicPartitionMapper
        implements RocketmqRecordPartitionKafkaTopicPartitionMapper {

    private int partitionBits = 16;

    public abstract int getBrokerNameId(String brokerName);
    public abstract String getBrokerName(int id);


    @Override
    public TopicPartition toTopicPartition(RecordPartition recordPartition) {
        int queueId = getQueueId(recordPartition);
        int brokerNameId = getBrokerNameId(String.valueOf(recordPartition.getPartition().get(RecordUtil.BROKER_NAME)));
        int encodedPartition = queueId | (brokerNameId << partitionBits);

        return null;
    }

    private  int getQueueId(RecordPartition recordPartition){
        return Integer.valueOf(
                (String) recordPartition.getPartition().get(RecordUtil.QUEUE_ID)
        );
    }

    @Override
    public RecordPartition toRecordPartition(TopicPartition topicPartition) {

        int encodedPartition = topicPartition.partition();
        int queueId = encodedPartition & (0xffffffff>>>(32-partitionBits));
        int brokerNameId = encodedPartition>>>partitionBits;

        String topic = topicPartition.topic();
        String brokerName = this.getBrokerName(brokerNameId);

        Map<String, String> map = new HashMap<>();
        map.put(RecordUtil.TOPIC, topic);
        map.put(RecordUtil.BROKER_NAME, brokerName);
        map.put(RecordUtil.QUEUE_ID, queueId + "");
        return new RecordPartition(map);
    }



}
