package org.apache.rocketmq.connect.kafka.util;

import io.openmessaging.connector.api.data.RecordPartition;
import org.apache.kafka.common.TopicPartition;

import java.util.HashMap;
import java.util.Map;

/**
 * 编码到Topic
 */
public class EncodedTopicRocketmqBrokerNameKafkaTopicPartitionMapper
        implements RocketmqRecordPartitionKafkaTopicPartitionMapper {

    private String SEPARATOR_CONFIG = "separator";

    private String DEFAULT_SEP = "@#@";;

    private String separator = DEFAULT_SEP;

    @Override
    public void configure(Map<String, String> configs) {
        this.separator = configs.getOrDefault(SEPARATOR_CONFIG, DEFAULT_SEP);
    }

    @Override
    public TopicPartition toTopicPartition(RecordPartition recordPartition) {
        String topicAndBrokerName = getTopicAndBrokerName(recordPartition);
        int partition = getPartition(recordPartition);
        return new TopicPartition(topicAndBrokerName, partition);
    }

    private  String getTopicAndBrokerName(RecordPartition recordPartition) {
        return new StringBuilder()
                .append(recordPartition.getPartition().get(RecordUtil.TOPIC))
                .append(this.separator)
                .append(recordPartition.getPartition().get(RecordUtil.BROKER_NAME))
                .toString();
    }

    private  int getPartition(RecordPartition recordPartition){
        return Integer.valueOf(
                (String) recordPartition.getPartition().get(RecordUtil.QUEUE_ID)
        );
    }

    @Override
    public RecordPartition toRecordPartition(TopicPartition topicPartition) {
        Map<String, String> map = getPartitionMap(topicPartition.topic());
        map.put(RecordUtil.QUEUE_ID, topicPartition.partition() + "");
        return new RecordPartition(map);
    }

    private  Map<String, String>  getPartitionMap(String topicAndBrokerName) {
        String[] split = topicAndBrokerName.split(this.separator);
        Map<String, String> map = new HashMap<>();
        map.put(RecordUtil.TOPIC, split[0]);
        map.put(RecordUtil.BROKER_NAME, split[1]);
        return map;
    }

}
