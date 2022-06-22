package org.apache.rocketmq.connect.kafka.connect.adaptor.connector;

import org.apache.kafka.connect.connector.ConnectorContext;

/**
 * kafka connect context
 */
public class KafkaConnectorContext implements ConnectorContext {
    io.openmessaging.connector.api.component.connector.ConnectorContext context;
    public KafkaConnectorContext(io.openmessaging.connector.api.component.connector.ConnectorContext context){
        this.context = context;
    }

    @Override
    public void requestTaskReconfiguration() {
        context.requestTaskReconfiguration();
    }

    @Override
    public void raiseError(Exception e) {
        context.raiseError(e);
    }
}
