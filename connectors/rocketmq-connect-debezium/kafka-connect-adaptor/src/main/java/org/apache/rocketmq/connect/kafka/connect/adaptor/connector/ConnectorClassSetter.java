package org.apache.rocketmq.connect.kafka.connect.adaptor.connector;

import io.openmessaging.KeyValue;
import org.apache.kafka.connect.runtime.ConnectorConfig;

/**
 * connector class setter
 */
public interface ConnectorClassSetter {
    /**
     * set connector class
     * @param config
     */
     default void setConnectorClass(KeyValue config) {
         config.put(ConnectorConfig.CONNECTOR_CLASS_CONFIG, getConnectorClass());
     }

    /**
     * get connector class
     */
    String getConnectorClass();
}
