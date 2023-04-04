package com.apache.rocketmq.schema.common;

import org.apache.rocketmq.schema.registry.client.exceptions.RestClientException;
import org.apache.rocketmq.schema.registry.common.dto.RegisterSchemaRequest;
import org.apache.rocketmq.schema.registry.common.dto.RegisterSchemaResponse;
import org.apache.rocketmq.schema.registry.common.model.SchemaType;
import org.junit.Test;

import java.io.IOException;

/**
 * registry schema
 */
public class SameSchemaNameTest extends BaseRegistrySchema{

    String idl = "{\"type\":\"object\",\"properties\":{\"name\":{\"type\":\"string\"},\"age\":{\"type\":\"int\"}}";
    String subject = "test-topic";
    String schemaNameOne = "test-topic-10";
    String schemaNameTwo = "test-topic-11";


    /**
     *  subject 不在唯一范围内
     * @throws RestClientException
     * @throws IOException
     */
    @Test
    public void testSameIdlRegistry() throws RestClientException, IOException {
//        RegisterSchemaRequest registerSchemaRequest = RegisterSchemaRequest.builder().schemaIdl(idl).schemaType(SchemaType.JSON).build();
//        RegisterSchemaResponse registerSchemaOne = schemaRegistryClient.registerSchema(subject,schemaNameOne, registerSchemaRequest);
//        RegisterSchemaResponse registerSchemaTwo = schemaRegistryClient.registerSchema(subject,schemaNameTwo, registerSchemaRequest);
        // schema 不是唯一
    }
}
