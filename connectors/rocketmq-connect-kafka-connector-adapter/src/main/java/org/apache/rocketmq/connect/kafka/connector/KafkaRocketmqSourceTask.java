package org.apache.rocketmq.connect.kafka.connector;

import io.openmessaging.connector.api.data.*;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.runtime.TaskConfig;
import org.apache.kafka.connect.runtime.WorkerConfig;
import org.apache.kafka.connect.storage.Converter;
import org.apache.kafka.connect.storage.HeaderConverter;
import org.apache.kafka.connect.transforms.util.SimpleConfig;
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

    private ClassLoader classLoader;

    private Converter keyConverter;
    private Converter valueConverter;
    private HeaderConverter headerConverter;


    @Override
    public List<ConnectRecord> poll() throws InterruptedException {

        List<SourceRecord>  sourceRecords =  this.kafkaSourceTask.poll();

        if(sourceRecords == null){
            return  null;
        }

        List<ConnectRecord> connectRecords = new ArrayList<>(sourceRecords.size());
        for(SourceRecord sourceRecord: sourceRecords){
            connectRecords.add(RecordUtil.toConnectRecord(sourceRecord,
                    this.keyConverter, this.valueConverter, this.headerConverter));
        }
        return connectRecords;
    }

    @Override
    public void start(KeyValue config) {
        Map<String, String> kafkaTaskProps = ConfigUtil.keyValueConfigToMap(config);
        log.info("kafka connector task config is {}", kafkaTaskProps);
        Plugins kafkaPlugins = KafkaPluginsUtil.getPlugins(Collections.singletonMap(KafkaPluginsUtil.PLUGIN_PATH, kafkaTaskProps.get(ConfigDefine.PLUGIN_PATH)));
        String connectorClass = kafkaTaskProps.get(ConfigDefine.CONNECTOR_CLASS);
        ClassLoader connectorLoader = kafkaPlugins.delegatingLoader().connectorLoader(connectorClass);
        this.classLoader = Plugins.compareAndSwapLoaders(connectorLoader);
        try {

            TaskConfig taskConfig = new TaskConfig(kafkaTaskProps);
            Class<? extends Task> taskClass = taskConfig.getClass(ConfigDefine.TASK_CLASS).asSubclass(Task.class);
            this.kafkaSourceTask = (org.apache.kafka.connect.source.SourceTask)kafkaPlugins.newTask(taskClass);

            initConverter(kafkaPlugins, kafkaTaskProps);

            this.kafkaSourceTask.initialize(new RocketmqKafkaSourceTaskContext(sourceTaskContext));
            this.kafkaSourceTask.start(kafkaTaskProps);
        } catch (Throwable e){
            recoverClassLoader();
            throw e;
        }
    }

    private void initConverter(Plugins plugins, Map<String, String> taskProps){

        ConfigDef converterConfigDef = new ConfigDef()
                .define(ConfigDefine.KEY_CONVERTER, ConfigDef.Type.CLASS, null, ConfigDef.Importance.LOW, "")
                .define(ConfigDefine.VALUE_CONVERTER, ConfigDef.Type.CLASS, null, ConfigDef.Importance.LOW, "")
                .define(ConfigDefine.HEADER_CONVERTER, ConfigDef.Type.CLASS, null, ConfigDef.Importance.LOW, "");

        Map<String, String> connProps = new HashMap<>();
        if(taskProps.containsKey(ConfigDefine.KEY_CONVERTER)){
            connProps.put(ConfigDefine.KEY_CONVERTER, taskProps.get(ConfigDefine.KEY_CONVERTER));
        }
        if(taskProps.containsKey(ConfigDefine.VALUE_CONVERTER)){
            connProps.put(ConfigDefine.VALUE_CONVERTER, taskProps.get(ConfigDefine.VALUE_CONVERTER));
        }
        if(taskProps.containsKey(ConfigDefine.HEADER_CONVERTER)){
            connProps.put(ConfigDefine.HEADER_CONVERTER, taskProps.get(ConfigDefine.HEADER_CONVERTER));
        }
        SimpleConfig connConfig = new SimpleConfig(converterConfigDef, connProps);

        Map<String, String> workerProps = new HashMap<>();
        workerProps.put(ConfigDefine.KEY_CONVERTER, "org.apache.kafka.connect.json.JsonConverter");
        workerProps.put(ConfigDefine.VALUE_CONVERTER, "org.apache.kafka.connect.json.JsonConverter");
        workerProps.put(ConfigDefine.HEADER_CONVERTER, "org.apache.kafka.connect.storage.SimpleHeaderConverter");
        SimpleConfig workerConfig = new SimpleConfig(converterConfigDef, workerProps);

        keyConverter = plugins.newConverter(connConfig, ConfigDefine.KEY_CONVERTER, Plugins.ClassLoaderUsage
                .CURRENT_CLASSLOADER);
        valueConverter = plugins.newConverter(connConfig, ConfigDefine.VALUE_CONVERTER, Plugins.ClassLoaderUsage.CURRENT_CLASSLOADER);
        headerConverter = plugins.newHeaderConverter(connConfig, ConfigDefine.HEADER_CONVERTER,
                Plugins.ClassLoaderUsage.CURRENT_CLASSLOADER);

        if (keyConverter == null) {
            keyConverter = plugins.newConverter(workerConfig, WorkerConfig.KEY_CONVERTER_CLASS_CONFIG, Plugins.ClassLoaderUsage.PLUGINS);
            log.info("Set up the key converter {} for task {} using the worker config", keyConverter.getClass(), sourceTaskContext.getTaskName());
        } else {
            log.info("Set up the key converter {} for task {} using the connector config", keyConverter.getClass(), sourceTaskContext.getTaskName());
        }
        if (valueConverter == null) {
            valueConverter = plugins.newConverter(workerConfig, WorkerConfig.VALUE_CONVERTER_CLASS_CONFIG, Plugins.ClassLoaderUsage.PLUGINS);
            log.info("Set up the value converter {} for task {} using the worker config", valueConverter.getClass(), sourceTaskContext.getTaskName());
        } else {
            log.info("Set up the value converter {} for task {} using the connector config", valueConverter.getClass(), sourceTaskContext.getTaskName());
        }
        if (headerConverter == null) {
            headerConverter = plugins.newHeaderConverter(workerConfig, WorkerConfig.HEADER_CONVERTER_CLASS_CONFIG, Plugins.ClassLoaderUsage
                    .PLUGINS);
            log.info("Set up the header converter {} for task {} using the worker config", headerConverter.getClass(), sourceTaskContext.getTaskName());
        } else {
            log.info("Set up the header converter {} for task {} using the connector config", headerConverter.getClass(), sourceTaskContext.getTaskName());
        }
    }

    @Override
    public void stop() {
        try {
            this.kafkaSourceTask.stop();
        } finally {
            recoverClassLoader();
        }
    }

    private void recoverClassLoader(){
        if(this.classLoader != null){
            Plugins.compareAndSwapLoaders(this.classLoader);
            this.classLoader = null;
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
                    RecordUtil.toSourceRecord(record, this.keyConverter, this.valueConverter, this.headerConverter),
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
