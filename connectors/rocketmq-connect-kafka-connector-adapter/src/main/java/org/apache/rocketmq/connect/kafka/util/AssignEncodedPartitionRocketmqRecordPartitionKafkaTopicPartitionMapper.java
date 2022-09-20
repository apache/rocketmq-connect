package org.apache.rocketmq.connect.kafka.util;

import org.apache.kafka.connect.errors.ConnectException;

import java.util.HashMap;
import java.util.Map;

public class AssignEncodedPartitionRocketmqRecordPartitionKafkaTopicPartitionMapper
        extends EncodedPartitionRocketmqRecordPartitionKafkaTopicPartitionMapper{


    private String assignments_config = "assignments";

    private Map<String, Integer> brokerName2Ids = new HashMap<>();
    private Map<Integer, String> id2BrokerName = new HashMap<>();


    @Override
    public void configure(Map<String, String> configs) {
        super.configure(configs);
        String assignments = configs.get(assignments_config);
        if(assignments == null || assignments.trim().isEmpty()){
            throw new ConnectException("miss assignments config");
        }
        for(String brokerNameAndId:assignments.split(",")){
            String[] brokerNameAndIds = brokerNameAndId.trim().split(":");
            String brokerName = brokerNameAndIds[0].trim();
            Integer id = Integer.valueOf(brokerNameAndIds[1].trim());
            checkBrokerNameId(id);
            if(brokerName2Ids.put(brokerName, id) != null){
                throw new ConnectException("error config，repeat brokerName："+brokerName);
            }
            if(id2BrokerName.put(id, brokerName) != null){
                throw new ConnectException("error config，repeat brokerNameId："+id);
            }
        }
    }

    @Override
    protected Integer getBrokerNameId(String brokerName) {
        return brokerName2Ids.get(brokerName);
    }

    @Override
    protected String getBrokerNameById(int id) {
        return id2BrokerName.get(id);
    }

}
