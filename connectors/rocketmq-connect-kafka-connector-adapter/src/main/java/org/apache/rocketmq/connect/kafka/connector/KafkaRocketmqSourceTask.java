package org.apache.rocketmq.connect.kafka.connector;

import io.openmessaging.connector.api.data.*;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.runtime.TaskConfig;
import org.apache.rocketmq.connect.kafka.util.KafkaPluginsUtil;
import org.apache.rocketmq.connect.kafka.util.RecordUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.openmessaging.KeyValue;
import io.openmessaging.connector.api.component.task.source.SourceTask;
import org.apache.kafka.connect.runtime.isolation.Plugins;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.rocketmq.connect.kafka.config.ConfigDefine;
import org.apache.rocketmq.connect.kafka.util.ConfigUtil;

import java.util.*;

public class KafkaRocketmqSourceTask extends SourceTask {
    private static final Logger log = LoggerFactory.getLogger(KafkaRocketmqSourceTask.class);

    private org.apache.kafka.connect.source.SourceTask kafkaSourceTask;
    private final KafkaRocketmqTask parentTask = new KafkaRocketmqTask();


    @Override
    public List<ConnectRecord> poll() throws InterruptedException {
        List<SourceRecord>  sourceRecords =  this.kafkaSourceTask.poll();

        if(sourceRecords == null){
            return  null;
        }

        List<ConnectRecord> connectRecords = new ArrayList<>(sourceRecords.size());
        for(SourceRecord sourceRecord: sourceRecords){
            connectRecords.add(RecordUtil.toConnectRecord(sourceRecord,
                    this.parentTask.getKeyConverter(), this.parentTask.getValueConverter(),
                    this.parentTask.getHeaderConverter()));
        }
        return connectRecords;
    }

    @Override
    public void start(KeyValue config) {
        Map<String, String> kafkaTaskProps = ConfigUtil.getKafkaConnectorConfigs(config);
        log.info("kafka connector task config is {}", kafkaTaskProps);
        Plugins kafkaPlugins = KafkaPluginsUtil.getPlugins(Collections.singletonMap(KafkaPluginsUtil.PLUGIN_PATH, kafkaTaskProps.get(ConfigDefine.PLUGIN_PATH)));
        String connectorClass = kafkaTaskProps.get(ConfigDefine.CONNECTOR_CLASS);
        ClassLoader connectorLoader = kafkaPlugins.delegatingLoader().connectorLoader(connectorClass);
        this.parentTask.setClassLoader(Plugins.compareAndSwapLoaders(connectorLoader));
        try {

            TaskConfig taskConfig = new TaskConfig(kafkaTaskProps);
            Class<? extends Task> taskClass = taskConfig.getClass(ConfigDefine.TASK_CLASS).asSubclass(Task.class);
            this.kafkaSourceTask = (org.apache.kafka.connect.source.SourceTask)kafkaPlugins.newTask(taskClass);
            this.parentTask.initConverter(kafkaPlugins, kafkaTaskProps, this.sourceTaskContext.getConnectorName(), this.sourceTaskContext.getTaskName());
            this.kafkaSourceTask.initialize(new RocketmqKafkaSourceTaskContext(sourceTaskContext));
            this.kafkaSourceTask.start(kafkaTaskProps);
        } catch (Throwable e){
            this.parentTask.recoverClassLoader();
            throw e;
        }
    }


    @Override
    public void stop() {
        try {
            this.kafkaSourceTask.stop();
        } finally {
            this.parentTask.recoverClassLoader();
        }
    }




    @Override
    public void commit(ConnectRecord record, Map<String, String> metadata) {
        if(this.kafkaSourceTask == null){
            log.warn("the task is not start, metadata:{}", metadata);
            return;
        }
        
        try {
            long baseOffset = Long.valueOf(metadata.get(RecordUtil.QUEUE_OFFSET));
            TopicPartition topicPartition = new TopicPartition(metadata.get(RecordUtil.TOPIC), Integer.valueOf(metadata.get(RecordUtil.QUEUE_ID)));
            RecordMetadata recordMetadata = new RecordMetadata(
                    topicPartition, baseOffset, 0,
                    System.currentTimeMillis(), 0,0
            );
            this.kafkaSourceTask.commitRecord(
                    RecordUtil.toSourceRecord(record, this.parentTask.getKeyConverter(), this.parentTask.getValueConverter(), this.parentTask.getHeaderConverter()),
                    recordMetadata
            );
        } catch (InterruptedException e){
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public void commit() {
        if(this.kafkaSourceTask == null){
            log.warn("the task is not start");
            return;
        }

        try {
            this.kafkaSourceTask.commit();
        } catch (InterruptedException e){
            Thread.currentThread().interrupt();
        }
    }
}
