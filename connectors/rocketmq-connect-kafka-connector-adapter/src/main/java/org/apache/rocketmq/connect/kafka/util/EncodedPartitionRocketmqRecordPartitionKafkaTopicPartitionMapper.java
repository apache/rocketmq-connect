package org.apache.rocketmq.connect.kafka.util;

import io.openmessaging.connector.api.data.RecordPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.ConnectException;

import java.util.HashMap;
import java.util.Map;

public abstract class EncodedPartitionRocketmqRecordPartitionKafkaTopicPartitionMapper
        extends RocketmqRecordPartitionKafkaTopicPartitionMapper {

    private String partitionBits_config = "partitionBits";

    private int partitionBits;

    private int maxPartition;
    private int maxBrokerNameId;


    protected abstract Integer getBrokerNameId(String brokerName);
    protected abstract String getBrokerNameById(int id);


    @Override
    public void configure(Map<String, String> configs) {
        this.partitionBits = Integer.valueOf(configs.getOrDefault(partitionBits_config, "16"));
        this.maxPartition = 2^16;
        this.maxBrokerNameId = 2^(32-16);
    }

    protected void checkBrokerNameId(int brokerNameId){
        if(brokerNameId < 0 || brokerNameId>this.maxBrokerNameId){
            throw new ConnectException("brokerNameId:"+brokerNameId+" must >=0 and <="+this.maxBrokerNameId);
        }
    }



    @Override
    public TopicPartition toTopicPartition(RecordPartition recordPartition) {
        int queueId = getQueueId(recordPartition);
        Integer brokerNameId = getBrokerNameId(getBrokerName(recordPartition));
        int encodedPartition = queueId | (brokerNameId << partitionBits);
        return new TopicPartition(getMessageQueueTopic(recordPartition), encodedPartition);
    }

    @Override
    public RecordPartition toRecordPartition(TopicPartition topicPartition) {

        int encodedPartition = topicPartition.partition();
        int queueId = encodedPartition & (0xffffffff>>>(32-partitionBits));
        int brokerNameId = encodedPartition>>>partitionBits;

        String topic = topicPartition.topic();
        String brokerName = this.getBrokerNameById(brokerNameId);
        if(brokerName == null || brokerName.trim().isEmpty()){
            throw new ConnectException("can not get brokerName from  id:"+brokerNameId);
        }

        Map<String, String> map = new HashMap<>();
        map.put(RecordUtil.TOPIC, topic);
        map.put(RecordUtil.BROKER_NAME, brokerName);
        map.put(RecordUtil.QUEUE_ID, queueId + "");
        return new RecordPartition(map);
    }
}
