package org.apache.rocketmq.connect.debezium;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.transforms.Transformation;

import java.util.Map;

public class SchemaRenameTransformation implements Transformation {
    @Override
    public ConnectRecord apply(ConnectRecord connectRecord) {
        return null;
    }

    @Override
    public ConfigDef config() {
        return null;
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> map) {

    }
}
