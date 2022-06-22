package org.apache.rocketmq.connect.kafka.connect.adaptor.connector;

import io.openmessaging.KeyValue;

/**
 * kafka source connector
 */
public abstract class KafkaSourceAdaptorConnector extends AbstractKafkaSourceConnector {

    /**
     * Start the component
     * @param config component context
     */
    @Override
    public void start(KeyValue config) {
        super.start(config);
        sourceConnector.validate(taskConfig);
        sourceConnector.initialize(new KafkaConnectorContext(connectorContext));
        sourceConnector.start(taskConfig);
    }

    /**
     * Stop the component.
     */
    @Override
    public void stop() {
        if (sourceConnector != null){
            sourceConnector.stop();
        }
        super.stop();
    }
}
