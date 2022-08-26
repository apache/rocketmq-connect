package org.apache.rocketmq.connect.kafka.util;

import io.openmessaging.connector.api.data.RecordPartition;
import org.apache.kafka.common.TopicPartition;

import java.util.HashMap;
import java.util.Map;

/**
 * rocketmq分区概念是三元组，即MessageQueue(topic,brokerName,queueId)
 *kafka分区概念是二元组，即TopicPartition(topic，partition)
 *org.apache.kafka.connect.sink.SinkTaskContext多个接口需要TopicPartition(topic，partition)映射到
 *MessageQueue(topic,brokerName,queueId)，所以需要转换
 */
public abstract class RocketmqRecordPartitionKafkaTopicPartitionMapper {


    public static RocketmqRecordPartitionKafkaTopicPartitionMapper newKafkaTopicPartitionMapper(Map<String, String> kafkaTaskProps){
        RocketmqRecordPartitionKafkaTopicPartitionMapper kafkaTopicPartitionMapper = new EncodedTopicRocketmqBrokerNameKafkaTopicPartitionMapper();
        kafkaTopicPartitionMapper.configure(new HashMap<>());
        return kafkaTopicPartitionMapper;
    }

    protected String getBrokerName(RecordPartition recordPartition){
        return (String)recordPartition.getPartition().get(RecordUtil.BROKER_NAME);
    }

    protected String getMessageQueueTopic(RecordPartition recordPartition){
        return (String)recordPartition.getPartition().get(RecordUtil.TOPIC);
    }

    protected int getQueueId(RecordPartition recordPartition){
        return Integer.valueOf(
                (String) recordPartition.getPartition().get(RecordUtil.QUEUE_ID)
        );
    }

    /**
     * 配置
     * @param configs
     */
    public abstract void configure(Map<String, String> configs);

    /**
     * 转换为kafka TopicPartition
     * @param recordPartition
     * @return
     */
    public abstract TopicPartition toTopicPartition(RecordPartition recordPartition);

    /**
     * 转换为openmessaging RecordPartition 也就是 rocketmq的MessageQueue
     * @param topicPartition
     * @return
     */
    public abstract RecordPartition toRecordPartition(TopicPartition topicPartition);


}
