package org.apache.rocketmq.connect.kafka.connector;

import io.openmessaging.connector.api.component.connector.ConnectorContext;

public class RocketmqKafkaConnectorContext implements org.apache.kafka.connect.connector.ConnectorContext{
    protected ConnectorContext rocketMqConnectorContext;

    public RocketmqKafkaConnectorContext(ConnectorContext rocketMqConnectorContext) {
        this.rocketMqConnectorContext = rocketMqConnectorContext;
    }

    @Override
    public void requestTaskReconfiguration() {
        this.rocketMqConnectorContext.requestTaskReconfiguration();
    }

    @Override
    public void raiseError(Exception e) {
        this.rocketMqConnectorContext.raiseError(e);
    }
}
