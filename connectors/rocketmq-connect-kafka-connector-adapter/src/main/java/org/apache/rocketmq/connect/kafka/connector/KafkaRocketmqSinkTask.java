package org.apache.rocketmq.connect.kafka.connector;

import io.openmessaging.KeyValue;
import io.openmessaging.connector.api.component.task.sink.SinkTask;
import io.openmessaging.connector.api.data.ConnectRecord;
import io.openmessaging.connector.api.data.RecordOffset;
import io.openmessaging.connector.api.data.RecordPartition;
import io.openmessaging.connector.api.errors.ConnectException;
import io.openmessaging.connector.api.errors.RetriableException;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.runtime.TaskConfig;
import org.apache.kafka.connect.runtime.isolation.Plugins;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.rocketmq.connect.kafka.config.ConfigDefine;
import org.apache.rocketmq.connect.kafka.util.ConfigUtil;
import org.apache.rocketmq.connect.kafka.util.KafkaPluginsUtil;
import org.apache.rocketmq.connect.kafka.util.RecordUtil;
import org.apache.rocketmq.connect.kafka.util.RocketmqRecordPartitionKafkaTopicPartitionMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.*;


public class KafkaRocketmqSinkTask extends SinkTask {
    private static final Logger log = LoggerFactory.getLogger(KafkaRocketmqSinkTask.class);

    private org.apache.kafka.connect.sink.SinkTask kafkaSinkTask;
    private final KafkaRocketmqTask parentTask = new KafkaRocketmqTask();
    private RocketmqRecordPartitionKafkaTopicPartitionMapper kafkaTopicPartitionMapper;

    @Override
    public void put(List<ConnectRecord> sinkRecords) throws ConnectException {
        Collection<SinkRecord> records = new ArrayList<>(sinkRecords.size());
        for(ConnectRecord sinkRecord: sinkRecords){
            String topic = (String)sinkRecord.getPosition().getPartition().getPartition().get(RecordUtil.TOPIC);
            SchemaAndValue valueSchemaAndValue = this.parentTask.getValueConverter().toConnectData(topic, ((String)sinkRecord.getData()).getBytes(StandardCharsets.UTF_8));
            String key = sinkRecord.getExtension(RecordUtil.KAFKA_MSG_KEY);
            SchemaAndValue keySchemaAndValue = null;
            if(key != null) {
                keySchemaAndValue = this.parentTask.getKeyConverter().toConnectData(topic, key.getBytes(StandardCharsets.UTF_8));
            }
            TopicPartition topicPartition = this.kafkaTopicPartitionMapper.toTopicPartition(sinkRecord.getPosition().getPartition());
            SinkRecord record = new SinkRecord(
                    topicPartition.topic(), topicPartition.partition(),
                    keySchemaAndValue==null?null:keySchemaAndValue.schema(),
                    keySchemaAndValue==null?null:keySchemaAndValue.value(),
                    valueSchemaAndValue.schema(), valueSchemaAndValue.value(),
                    RecordUtil.getOffset(sinkRecord.getPosition().getOffset()),
                    sinkRecord.getTimestamp(), TimestampType.NO_TIMESTAMP_TYPE,
                    getHeaders(sinkRecord.getExtensions(),  topic)
                    );
            records.add(record);
        }
        try {
            this.kafkaSinkTask.put(records);
        } catch (org.apache.kafka.connect.errors.RetriableException e){
            throw new RetriableException(e);
        }
    }

    private ConnectHeaders getHeaders(KeyValue extensions, String topic){
        ConnectHeaders headers = new ConnectHeaders();
        for(String headerKey: extensions.keySet()){
            if(RecordUtil.KAFKA_MSG_KEY.equals(headerKey)){
                continue;
            }
            SchemaAndValue headerSchemaAndValue = parentTask.getHeaderConverter()
                    .toConnectHeader(topic, headerKey, extensions.getString(headerKey).getBytes());
            headers.add(headerKey, headerSchemaAndValue);
        }
        return headers;
    }



    @Override
    public void start(KeyValue config) {
        Map<String, String> kafkaTaskProps = ConfigUtil.getKafkaConnectorConfigs(config);
        log.info("kafka connector task config is {}", kafkaTaskProps);
        Plugins kafkaPlugins =  KafkaPluginsUtil.getPlugins(Collections.singletonMap(KafkaPluginsUtil.PLUGIN_PATH, kafkaTaskProps.get(ConfigDefine.PLUGIN_PATH)));
        String connectorClass = kafkaTaskProps.get(ConfigDefine.CONNECTOR_CLASS);
        ClassLoader connectorLoader = kafkaPlugins.delegatingLoader().connectorLoader(connectorClass);
        this.parentTask.setClassLoader(Plugins.compareAndSwapLoaders(connectorLoader));
        try {
            TaskConfig taskConfig = new TaskConfig(kafkaTaskProps);
            Class<? extends Task> taskClass = taskConfig.getClass(ConfigDefine.TASK_CLASS).asSubclass(Task.class);
            this.kafkaSinkTask = (org.apache.kafka.connect.sink.SinkTask)kafkaPlugins.newTask(taskClass);
            this.parentTask.initConverter(kafkaPlugins, kafkaTaskProps, this.sinkTaskContext.getConnectorName(), this.sinkTaskContext.getTaskName());
            this.kafkaTopicPartitionMapper = RocketmqRecordPartitionKafkaTopicPartitionMapper.newKafkaTopicPartitionMapper(kafkaTaskProps);
            this.kafkaSinkTask.initialize(new RocketmqKafkaSinkTaskContext(sinkTaskContext, this.kafkaTopicPartitionMapper));
            this.kafkaSinkTask.start(kafkaTaskProps);
        } catch (Throwable e){
            this.parentTask.recoverClassLoader();
            throw e;
        }
    }

    @Override
    public void stop() {
        try {
            this.kafkaSinkTask.stop();
        } finally {
            this.parentTask.recoverClassLoader();
        }
    }



    @Override
    public void flush(Map<RecordPartition, RecordOffset> currentOffsets) throws ConnectException {

        if(this.kafkaSinkTask == null){
            log.warn("the task is not start, currentOffsets:{}", currentOffsets);
            return;
        }

        Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>(currentOffsets.size());

        for(Map.Entry<RecordPartition, RecordOffset> po: currentOffsets.entrySet()){
            TopicPartition tp = this.kafkaTopicPartitionMapper.toTopicPartition(po.getKey());
            OffsetAndMetadata offsetAndMetadata = new OffsetAndMetadata(RecordUtil.getOffset(po.getValue()));
            offsets.put(tp, offsetAndMetadata);
        }
        this.kafkaSinkTask.flush(offsets);
    }
}
