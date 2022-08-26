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
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.runtime.TaskConfig;
import org.apache.kafka.connect.runtime.WorkerConfig;
import org.apache.kafka.connect.runtime.isolation.Plugins;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.storage.Converter;
import org.apache.kafka.connect.storage.HeaderConverter;

import org.apache.kafka.connect.transforms.util.SimpleConfig;
import org.apache.rocketmq.connect.kafka.config.ConfigDefine;
import org.apache.rocketmq.connect.kafka.util.ConfigUtil;
import org.apache.rocketmq.connect.kafka.util.KafkaPluginsUtil;
import org.apache.rocketmq.connect.kafka.util.RecordUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.*;


public class KafkaRocketmqSinkTask extends SinkTask {
    private static final Logger log = LoggerFactory.getLogger(KafkaRocketmqSinkTask.class);

    private org.apache.kafka.connect.sink.SinkTask kafkaSinkTask;
    private ClassLoader classLoader;

    private Converter keyConverter;
    private Converter valueConverter;
    private HeaderConverter headerConverter;

    @Override
    public void put(List<ConnectRecord> sinkRecords) throws ConnectException {
        Collection<SinkRecord> records = new ArrayList<>(sinkRecords.size());
        for(ConnectRecord sinkRecord: sinkRecords){
            String topic = (String)sinkRecord.getPosition().getPartition().getPartition().get(RecordUtil.TOPIC);
            SchemaAndValue valueSchemaAndValue = valueConverter.toConnectData(topic, ((String)sinkRecord.getData()).getBytes(StandardCharsets.UTF_8));
            String key = sinkRecord.getExtension(RecordUtil.KAFKA_MSG_KEY);
            SchemaAndValue keySchemaAndValue = null;
            if(key != null) {
                keySchemaAndValue = keyConverter.toConnectData(topic, key.getBytes(StandardCharsets.UTF_8));
            }

            SinkRecord record = new SinkRecord(
                    RecordUtil.getTopicAndBrokerName(sinkRecord.getPosition().getPartition()),
                    RecordUtil.getPartition(sinkRecord.getPosition().getPartition()),
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
            SchemaAndValue headerSchemaAndValue = headerConverter
                    .toConnectHeader(topic, headerKey, extensions.getString(headerKey).getBytes());
            headers.add(headerKey, headerSchemaAndValue);
        }
        return headers;
    }



    @Override
    public void start(KeyValue config) {
        Map<String, String> kafkaTaskProps = ConfigUtil.keyValueConfigToMap(config);
        log.info("kafka connector task config is {}", kafkaTaskProps);
        Plugins kafkaPlugins =  KafkaPluginsUtil.getPlugins(Collections.singletonMap(KafkaPluginsUtil.PLUGIN_PATH, kafkaTaskProps.get(ConfigDefine.PLUGIN_PATH)));
        String connectorClass = kafkaTaskProps.get(ConfigDefine.CONNECTOR_CLASS);
        ClassLoader connectorLoader = kafkaPlugins.delegatingLoader().connectorLoader(connectorClass);
        this.classLoader = Plugins.compareAndSwapLoaders(connectorLoader);
        try {
            TaskConfig taskConfig = new TaskConfig(kafkaTaskProps);
            Class<? extends Task> taskClass = taskConfig.getClass(ConfigDefine.TASK_CLASS).asSubclass(Task.class);
            this.kafkaSinkTask = (org.apache.kafka.connect.sink.SinkTask)kafkaPlugins.newTask(taskClass);
            initConverter(kafkaPlugins, kafkaTaskProps);
            this.kafkaSinkTask.initialize(new RocketmqKafkaSinkTaskContext(sinkTaskContext));
            this.kafkaSinkTask.start(kafkaTaskProps);
        } catch (Throwable e){
            recoverClassLoader();
            throw e;
        }
    }

    @Override
    public void stop() {
        try {
            this.kafkaSinkTask.stop();
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
            log.info("Set up the key converter {} for task {} using the worker config", keyConverter.getClass(), sinkTaskContext.getTaskName());
        } else {
            log.info("Set up the key converter {} for task {} using the connector config", keyConverter.getClass(), sinkTaskContext.getTaskName());
        }
        if (valueConverter == null) {
            valueConverter = plugins.newConverter(workerConfig, WorkerConfig.VALUE_CONVERTER_CLASS_CONFIG, Plugins.ClassLoaderUsage.PLUGINS);
            log.info("Set up the value converter {} for task {} using the worker config", valueConverter.getClass(), sinkTaskContext.getTaskName());
        } else {
            log.info("Set up the value converter {} for task {} using the connector config", valueConverter.getClass(), sinkTaskContext.getTaskName());
        }
        if (headerConverter == null) {
            headerConverter = plugins.newHeaderConverter(workerConfig, WorkerConfig.HEADER_CONVERTER_CLASS_CONFIG, Plugins.ClassLoaderUsage
                    .PLUGINS);
            log.info("Set up the header converter {} for task {} using the worker config", headerConverter.getClass(), sinkTaskContext.getTaskName());
        } else {
            log.info("Set up the header converter {} for task {} using the connector config", headerConverter.getClass(), sinkTaskContext.getTaskName());
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
            TopicPartition tp = RecordUtil.recordPartitionToTopicPartition(po.getKey());
            OffsetAndMetadata offsetAndMetadata = new OffsetAndMetadata(RecordUtil.getOffset(po.getValue()));
            offsets.put(tp, offsetAndMetadata);
        }
        this.kafkaSinkTask.flush(offsets);
    }
}
