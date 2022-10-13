package com.apache.rocketmq.schema.common;

import org.apache.rocketmq.schema.registry.client.SchemaRegistryClient;
import org.apache.rocketmq.schema.registry.client.SchemaRegistryClientFactory;

public abstract class BaseRegistrySchema {
    protected final SchemaRegistryClient schemaRegistryClient;
    protected final String schemaRegistryUrl = "http://localhost:8080";
    public BaseRegistrySchema(){
        this.schemaRegistryClient = SchemaRegistryClientFactory.newClient(schemaRegistryUrl,null);
    }
}
