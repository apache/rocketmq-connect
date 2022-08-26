package org.apache.rocketmq.connect.kafka.util;

import io.openmessaging.connector.api.data.ConnectRecord;
import io.openmessaging.connector.api.data.RecordOffset;
import io.openmessaging.connector.api.data.RecordPartition;
import io.openmessaging.connector.api.data.SchemaBuilder;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.storage.Converter;
import org.apache.kafka.connect.storage.HeaderConverter;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

public class RecordUtil {

    public static final String BROKER_NAME = "brokerName";
    public static final String QUEUE_ID = "queueId";
    public static final String TOPIC = "topic";
    public static final String QUEUE_OFFSET = "queueOffset";


    private static final String TOPIC_SEP = "@#@";


    public static final String KAFKA_MSG_KEY = "kafka_key";
    public static final String KAFKA_CONNECT_RECORD_TOPIC_KEY = "kafka_connect_record_topic";
    public static final String KAFKA_CONNECT_RECORD_PARTITION_KEY = "kafka_connect_record_partition";
    public static final String KAFKA_CONNECT_RECORD_HEADER_KEY_PREFIX = "kafka_connect_record_header_";

    public static String getTopicAndBrokerName(RecordPartition recordPartition) {
        return new StringBuilder()
                .append(recordPartition.getPartition().get(TOPIC))
                .append(TOPIC_SEP)
                .append(recordPartition.getPartition().get(BROKER_NAME))
                .toString();
    }

    public static Map<String, String>  getPartitionMap(String topicAndBrokerName) {
        String[] split = topicAndBrokerName.split(TOPIC_SEP);
        Map<String, String> map = new HashMap<>();
        map.put(TOPIC, split[0]);
        map.put(BROKER_NAME, split[1]);

        return map;
    }

    public static  long getOffset(RecordOffset recordOffset){
        return Long.valueOf(
                (String) recordOffset.getOffset().get(QUEUE_OFFSET)
        );
    }

    public static  int getPartition(RecordPartition recordPartition){
        return Integer.valueOf(
                (String) recordPartition.getPartition().get(QUEUE_ID)
        );
    }

    public static TopicPartition recordPartitionToTopicPartition(RecordPartition recordPartition){
        String topicAndBrokerName = getTopicAndBrokerName(recordPartition);
        int partition = getPartition(recordPartition);
        return new TopicPartition(topicAndBrokerName, partition);
    }

    public static RecordPartition topicPartitionToRecordPartition(TopicPartition topicPartition){
        Map<String, String> map = RecordUtil.getPartitionMap(topicPartition.topic());
        map.put(RecordUtil.QUEUE_ID, topicPartition.partition() + "");
        return new RecordPartition(map);
    }


    public static ConnectRecord toConnectRecord(SourceRecord sourceRecord, Converter keyConverter, Converter valueConverter,
                                         HeaderConverter headerConverter){
        RecordPartition recordPartition = new RecordPartition(new HashMap<>(sourceRecord.sourcePartition()));
        RecordOffset recordOffset = new RecordOffset(new HashMap<>(sourceRecord.sourceOffset()));
        Long timestamp = sourceRecord.timestamp();

        byte[] value = valueConverter.fromConnectData(
                sourceRecord.topic(), sourceRecord.valueSchema(), sourceRecord.value()
        );

        ConnectRecord connectRecord = new ConnectRecord(
                recordPartition, recordOffset, timestamp,
                SchemaBuilder.string().build(), new String(value, StandardCharsets.UTF_8)
        );

        if(sourceRecord.key() != null) {
            byte[] key = keyConverter.fromConnectData
                    (sourceRecord.topic(), sourceRecord.keySchema(), sourceRecord.key()
                    );
            connectRecord.addExtension(RecordUtil.KAFKA_MSG_KEY, new String(key, StandardCharsets.UTF_8));
        }

        for(Header header: sourceRecord.headers()){
            byte[] headerValue = headerConverter.fromConnectHeader(
                    sourceRecord.topic(), header.key(), header.schema(), header.value()
            );
            connectRecord.addExtension(RecordUtil.KAFKA_CONNECT_RECORD_HEADER_KEY_PREFIX+header.key(), new String(headerValue, StandardCharsets.UTF_8));
        }

        if(sourceRecord.topic() != null){
            connectRecord.addExtension(RecordUtil.KAFKA_CONNECT_RECORD_TOPIC_KEY, sourceRecord.topic());
        }
        if(sourceRecord.kafkaPartition() != null){
            connectRecord.addExtension(RecordUtil.KAFKA_CONNECT_RECORD_PARTITION_KEY, sourceRecord.kafkaPartition().toString());
        }
        return connectRecord;
    }


    public static SourceRecord toSourceRecord(ConnectRecord connectRecord, Converter keyConverter, Converter valueConverter,
                                                HeaderConverter headerConverter){
        Map<String, ?> sourcePartition = new HashMap<>(connectRecord.getPosition().getPartition().getPartition());
        Map<String, ?> sourceOffset = new HashMap<>(connectRecord.getPosition().getOffset().getOffset());
        String topic = connectRecord.getExtension(RecordUtil.KAFKA_CONNECT_RECORD_TOPIC_KEY);
        String partitionStr = connectRecord.getExtension(RecordUtil.KAFKA_CONNECT_RECORD_PARTITION_KEY);
        Integer partition = null;
        if(partitionStr != null){
            partition = Integer.valueOf(partitionStr);
        }
        String keyStr = connectRecord.getExtension(RecordUtil.KAFKA_MSG_KEY);
        Schema keySchema = null;
        Object key = null;
        if(keyStr != null){
            SchemaAndValue keySchemaAndValue = keyConverter.toConnectData(topic, keyStr.getBytes(StandardCharsets.UTF_8));
            keySchema = keySchemaAndValue.schema();
            key = keySchemaAndValue.value();
        }

        ConnectHeaders headers = new ConnectHeaders();
        for(String extKey: connectRecord.getExtensions().keySet()){
            if(!extKey.startsWith(RecordUtil.KAFKA_CONNECT_RECORD_HEADER_KEY_PREFIX)){
                continue;
            }
            String header = extKey.substring(RecordUtil.KAFKA_CONNECT_RECORD_HEADER_KEY_PREFIX.length());
            SchemaAndValue headerSchemaAndValue =  headerConverter
                    .toConnectHeader(topic, header, connectRecord.getExtension(extKey).getBytes(StandardCharsets.UTF_8));

            headers.add(header, headerSchemaAndValue);
        }

        SchemaAndValue valueSchemaAndValue = keyConverter.toConnectData(topic, ((String)connectRecord.getData()).getBytes(StandardCharsets.UTF_8));
        SourceRecord sourceRecord = new SourceRecord(
                sourcePartition, sourceOffset, topic, partition,
                keySchema, key, valueSchemaAndValue.schema(), valueSchemaAndValue.value(),
                connectRecord.getTimestamp(),headers
                );
        return sourceRecord;
    }

}
