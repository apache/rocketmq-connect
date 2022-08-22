package org.apache.rocketmq.connect.kafka.util;

import io.openmessaging.connector.api.data.RecordPartition;
import org.apache.kafka.common.TopicPartition;

import java.util.Map;

public class BrokerNameToTopicKafkaTopicPartitionMapper implements KafkaTopicPartitionMapper{

    @Override
    public void configure(Map<String, ?> configs) {

    }

    @Override
    public TopicPartition toTopicPartition(RecordPartition recordPartition) {
        return null;
    }

    @Override
    public RecordPartition toRecordPartition(TopicPartition topicPartition) {
        return null;
    }

}
