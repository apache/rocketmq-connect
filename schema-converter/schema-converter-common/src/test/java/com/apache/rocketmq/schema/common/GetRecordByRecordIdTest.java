package com.apache.rocketmq.schema.common;

import org.apache.rocketmq.schema.registry.client.exceptions.RestClientException;
import org.apache.rocketmq.schema.registry.common.dto.GetSchemaResponse;
import org.apache.rocketmq.schema.registry.common.dto.RegisterSchemaRequest;
import org.apache.rocketmq.schema.registry.common.dto.RegisterSchemaResponse;
import org.apache.rocketmq.schema.registry.common.model.SchemaType;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

/**
 * registry schema
 *
 * 注册时对同一个 【Tenant + SchemaName】 做幂等校验，
 * 那为什么要允许 schemaName 为空
 *
 */
public class GetRecordByRecordIdTest extends BaseRegistrySchema{

    String idl = "{\"type\":\"object\",\"properties\":{\"name\":{\"type\":\"string\"},\"age\":{\"type\":\"int\"}}";
    String subject = "test-topic-2";

    String schemaNameOne = "test-topic-5";


    /**
     * 无法拿到数据
     * @throws RestClientException
     * @throws IOException
     */
    @Test
    public void testGetSchemaByRecordId() throws RestClientException, IOException {
        RegisterSchemaRequest registerSchemaRequest = RegisterSchemaRequest.builder().schemaIdl(idl).schemaType(SchemaType.JSON).build();
        RegisterSchemaResponse registerSchemaOne = schemaRegistryClient.registerSchema("test", "namespace", subject,schemaNameOne, registerSchemaRequest);

        long recordId = registerSchemaOne.getRecordId();
        GetSchemaResponse response = schemaRegistryClient.getSchemaByRecordId("test", "namespace", subject, recordId);

        Assert.assertNotNull(response);

        // schema 不是唯一
    }
}
