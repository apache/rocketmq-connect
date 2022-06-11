package org.apache.rocketmq.connect.kafka.connect.adaptor.connector;

import io.openmessaging.KeyValue;

/**
 * kafka source connector
 */
public abstract class KafkaSinkAdaptorConnector extends AbstractKafkaSinkConnector {

    /**
     * Start the component
     * @param config component context
     */
    @Override
    public void start(KeyValue config) {
        super.start(config);
        sinkConnector.validate(taskConfig);
        sinkConnector.initialize(new KafkaConnectorContext(connectorContext));
        sinkConnector.start(taskConfig);
    }

    /**
     * Stop the component.
     */
    @Override
    public void stop() {
        if (sinkConnector != null){
           sinkConnector.stop();
        }
        super.stop();
    }
}
