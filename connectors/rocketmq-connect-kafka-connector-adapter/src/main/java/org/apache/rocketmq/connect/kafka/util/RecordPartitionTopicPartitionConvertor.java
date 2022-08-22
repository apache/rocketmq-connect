package org.apache.rocketmq.connect.kafka.util;

import io.openmessaging.connector.api.data.RecordPartition;
import org.apache.kafka.common.TopicPartition;

import java.util.Map;

/**
 * rocketmq分区概念是三元组，即MessageQueue(topic,brokerName,queueId)
 *kafka分区概念是二元组，即TopicPartition(topic，partition)
 *org.apache.kafka.connect.sink.SinkTaskContext多个接口需要TopicPartition(topic，partition)映射到
 *MessageQueue(topic,brokerName,queueId)，所以需要转换
 */
public interface RecordPartitionTopicPartitionConvertor {

    /**
     * 配置
     * @param configs
     */
    void configure(Map<String, ?> configs);

    /**
     * 转换为kafka TopicPartition
     * @param recordPartition
     * @return
     */
    TopicPartition toTopicPartition(RecordPartition recordPartition);

    /**
     * 转换为openmessaging RecordPartition 也就是 rocketmq的MessageQueue
     * @param topicPartition
     * @return
     */
    RecordPartition toRecordPartition(TopicPartition topicPartition);


}
