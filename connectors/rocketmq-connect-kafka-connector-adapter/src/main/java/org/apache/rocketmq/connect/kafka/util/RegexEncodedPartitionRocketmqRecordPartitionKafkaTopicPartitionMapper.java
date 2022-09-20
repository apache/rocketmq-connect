package org.apache.rocketmq.connect.kafka.util;

import org.apache.kafka.connect.errors.ConnectException;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class RegexEncodedPartitionRocketmqRecordPartitionKafkaTopicPartitionMapper
        extends EncodedPartitionRocketmqRecordPartitionKafkaTopicPartitionMapper{


    private String pattern_config = "pattern";
    private Pattern pattern;

    private Map<Integer, String> id2BrokerName = new HashMap<>();

    @Override
    public void configure(Map<String, String> configs) {
        super.configure(configs);
        String pattern = configs.get(pattern_config);
        if(pattern == null || pattern.trim().isEmpty()){
            throw new ConnectException("miss pattern config");
        }

        this.pattern = Pattern.compile(pattern);

    }

    @Override
    protected Integer getBrokerNameId(String brokerName) {
        Matcher matcher = pattern.matcher(brokerName);
        if(matcher.find()){
            try {
                Integer id =  Integer.valueOf(matcher.group());
                if(id2BrokerName.put(id, brokerName) != null){
                    throw new ConnectException("error config，repeat brokerNameId："+id);
                }
                return id;
            } catch (NumberFormatException e){
                throw new ConnectException("can not get brokerNameId from brokerName for regex:"+brokerName, e);
            }
        } else {
            throw new ConnectException("can not get brokerNameId from brokerName for find:"+brokerName);
        }
    }

    @Override
    protected String getBrokerNameById(int id) {
        return this.id2BrokerName.get(id);
    }
}
