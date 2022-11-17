package com.apache.rocketmq.schema.common;

import org.apache.rocketmq.schema.registry.client.exceptions.RestClientException;
import org.apache.rocketmq.schema.registry.common.dto.GetSchemaResponse;
import org.apache.rocketmq.schema.registry.common.dto.RegisterSchemaRequest;
import org.apache.rocketmq.schema.registry.common.dto.RegisterSchemaResponse;
import org.apache.rocketmq.schema.registry.common.dto.SchemaRecordDto;
import org.apache.rocketmq.schema.registry.common.model.SchemaType;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

/**
 * registry schema
 *
 * 注册时对同一个 【Tenant + SchemaName】 做幂等校验，
 * 那为什么要允许 schemaName 为空
 *
 */
public class GetRecordByRecordIdTest extends BaseRegistrySchema{

    String idl = "{\"type\":\"object\",\"properties\":{\"name\":{\"type\":\"string\"},\"age\":{\"type\":\"int\"}}";
    String subject = "test-topic-100";
    String schemaNameOne = "test-topic-102";

    /**
     * 无法拿到数据
     *
     *   public static final String NAMESPACE = "org.apache.rocketmq.connect.avro";
     *
     * @throws RestClientException
     * @throws IOException
     */
    @Test
    public void testGetSchemaByRecordIdError() throws RestClientException, IOException {

//        String subject = "test-topic-12";
//        long recordId = 103465635513630721L;
//        String cluster = "Connect";
//        String namespace = "org.apache.rocketmq.connect.avro";
//
//        List<SchemaRecordDto> schemaRecordDtos = schemaRegistryClient.getSchemaListBySubject(cluster, namespace, subject);
//        for (SchemaRecordDto schemaRecordDto : schemaRecordDtos) {
//            if (schemaRecordDto.getRecordId() == recordId) {
//                System.out.println(schemaRecordDto);
//            }
//        }
//        GetSchemaResponse response = schemaRegistryClient.getSchemaByRecordId(cluster, namespace,subject, recordId);
//        Assert.assertNotNull(response);
        // schema 不是唯一
    }


    @Test
    public void testGetSchemaByRecordId() throws RestClientException, IOException {
//        RegisterSchemaRequest registerSchemaRequest = RegisterSchemaRequest.builder().schemaIdl(idl).schemaType(SchemaType.AVRO).build();
//        RegisterSchemaResponse registerSchemaResponse = schemaRegistryClient.registerSchema("test", "namespace", subject,schemaNameOne, registerSchemaRequest);
//        long recordId = registerSchemaResponse.getRecordId();
//
//        List<SchemaRecordDto> schemaRecordDtos  = schemaRegistryClient.getSchemaListBySubject("test", "namespace", subject);
//
//        System.out.println(recordId);
//        GetSchemaResponse response = schemaRegistryClient.getSchemaByRecordId( "test", "namespace", subject, recordId);
//        System.out.println(response.getRecordId());
    }
}
