package org.apache.rocketmq.connect.kafka.connector;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.runtime.ConnectorConfig;
import org.apache.kafka.connect.runtime.WorkerConfig;
import org.apache.kafka.connect.runtime.isolation.Plugins;
import org.apache.kafka.connect.storage.Converter;
import org.apache.kafka.connect.storage.HeaderConverter;
import org.apache.kafka.connect.transforms.util.SimpleConfig;
import org.apache.rocketmq.connect.kafka.config.ConfigDefine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class KafkaRocketmqTask {
    private static final Logger log = LoggerFactory.getLogger(KafkaRocketmqTask.class);

    private ClassLoader classLoader;
    private Converter keyConverter;
    private Converter valueConverter;
    private HeaderConverter headerConverter;

    public ClassLoader getClassLoader() {
        return classLoader;
    }

    public void setClassLoader(ClassLoader classLoader) {
        this.classLoader = classLoader;
    }

    public Converter getKeyConverter() {
        return keyConverter;
    }

    public Converter getValueConverter() {
        return valueConverter;
    }

    public HeaderConverter getHeaderConverter() {
        return headerConverter;
    }

    public void recoverClassLoader(){
        if(this.classLoader != null){
            Plugins.compareAndSwapLoaders(this.classLoader);
            this.classLoader = null;
        }
    }

    public void initConverter(Plugins plugins, Map<String, String> taskProps, String connectorName, String taskName){
        Map<String, String> connProps = new HashMap<>(taskProps);
        connProps.put(ConnectorConfig.NAME_CONFIG, connectorName);
        final ConnectorConfig connConfig = new ConnectorConfig(plugins, connProps);
        Map<String, String> workerProps = new HashMap<>();
        workerProps.put(ConfigDefine.KEY_CONVERTER, "org.apache.kafka.connect.json.JsonConverter");
        workerProps.put(ConfigDefine.VALUE_CONVERTER, "org.apache.kafka.connect.json.JsonConverter");
        workerProps.put(ConfigDefine.HEADER_CONVERTER, "org.apache.kafka.connect.storage.SimpleHeaderConverter");
        ConfigDef converterConfigDef = new ConfigDef()
                .define(ConfigDefine.KEY_CONVERTER, ConfigDef.Type.CLASS, null, ConfigDef.Importance.LOW, "")
                .define(ConfigDefine.VALUE_CONVERTER, ConfigDef.Type.CLASS, null, ConfigDef.Importance.LOW, "")
                .define(ConfigDefine.HEADER_CONVERTER, ConfigDef.Type.CLASS, null, ConfigDef.Importance.LOW, "");
        SimpleConfig workerConfig = new SimpleConfig(converterConfigDef, workerProps);

        keyConverter = plugins.newConverter(connConfig, ConfigDefine.KEY_CONVERTER, Plugins.ClassLoaderUsage
                .CURRENT_CLASSLOADER);
        valueConverter = plugins.newConverter(connConfig, ConfigDefine.VALUE_CONVERTER, Plugins.ClassLoaderUsage.CURRENT_CLASSLOADER);
        headerConverter = plugins.newHeaderConverter(connConfig, ConfigDefine.HEADER_CONVERTER,
                Plugins.ClassLoaderUsage.CURRENT_CLASSLOADER);

        if (keyConverter == null) {
            keyConverter = plugins.newConverter(workerConfig, WorkerConfig.KEY_CONVERTER_CLASS_CONFIG, Plugins.ClassLoaderUsage.PLUGINS);
            log.info("Set up the key converter {} for task {} using the worker config", keyConverter.getClass(), taskName);
        } else {
            log.info("Set up the key converter {} for task {} using the connector config", keyConverter.getClass(), taskName);
        }
        if (valueConverter == null) {
            valueConverter = plugins.newConverter(workerConfig, WorkerConfig.VALUE_CONVERTER_CLASS_CONFIG, Plugins.ClassLoaderUsage.PLUGINS);
            log.info("Set up the value converter {} for task {} using the worker config", valueConverter.getClass(), taskName);
        } else {
            log.info("Set up the value converter {} for task {} using the connector config", valueConverter.getClass(), taskName);
        }
        if (headerConverter == null) {
            headerConverter = plugins.newHeaderConverter(workerConfig, WorkerConfig.HEADER_CONVERTER_CLASS_CONFIG, Plugins.ClassLoaderUsage
                    .PLUGINS);
            log.info("Set up the header converter {} for task {} using the worker config", headerConverter.getClass(), taskName);
        } else {
            log.info("Set up the header converter {} for task {} using the connector config", headerConverter.getClass(), taskName);
        }
    }

}
