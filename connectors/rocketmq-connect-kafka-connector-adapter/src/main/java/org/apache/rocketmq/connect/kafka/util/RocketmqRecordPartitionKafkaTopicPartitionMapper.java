package org.apache.rocketmq.connect.kafka.util;

import io.openmessaging.connector.api.data.RecordPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.rocketmq.connect.kafka.config.ConfigDefine;

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
        String mapper = kafkaTaskProps.getOrDefault(ConfigDefine.ROCKETMQ_RECORDPARTITION_KAFKATOPICPARTITION_MAPPER, "encodedTopic");
        RocketmqRecordPartitionKafkaTopicPartitionMapper kafkaTopicPartitionMapper;
        if(mapper.equalsIgnoreCase("encodedTopic")){
            kafkaTopicPartitionMapper = new EncodedTopicRocketmqBrokerNameKafkaTopicPartitionMapper();
        } else if(mapper.equalsIgnoreCase("assignEncodedPartition")){
            kafkaTopicPartitionMapper = new AssignEncodedPartitionRocketmqRecordPartitionKafkaTopicPartitionMapper();
        } else if(mapper.equalsIgnoreCase("regexEncodedPartition")){
            kafkaTopicPartitionMapper = new RegexEncodedPartitionRocketmqRecordPartitionKafkaTopicPartitionMapper();
        } else {
            throw new ConnectException("unknown rocketmq.recordPartition.kafkaTopicPartition.mapper config:"+mapper);
        }
        Map<String, String> mapperConfig = new HashMap<>();
        String prefix = mapper+".";
        for(Map.Entry<String, String> kv: kafkaTaskProps.entrySet()){
            if(kv.getKey().startsWith(prefix)){
                mapperConfig.put(kv.getKey().substring(prefix.length()), kv.getValue());
            }
        }
        kafkaTopicPartitionMapper.configure(mapperConfig);
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
