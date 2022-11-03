package com.apache.rocketmq.schema.avro.test;

import io.openmessaging.connector.api.data.Schema;
import io.openmessaging.connector.api.data.SchemaAndValue;
import io.openmessaging.connector.api.data.SchemaBuilder;
import io.openmessaging.connector.api.data.Struct;
import org.apache.rocketmq.schema.avro.AvroConverter;
import org.apache.rocketmq.schema.avro.AvroConverterConfig;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class AvroConverterTest {
    private AvroConverter avroConverter = new AvroConverter();

//    private String topic = "test-topic-08";
    private String topic = "test-topic-13";

    private String SCHEMA_REGISTRY_URL = "http://localhost:8080";

    @Before
    public void init(){
        Map<String, Object> schemaConfigs = new HashMap<>();
        schemaConfigs.put(AvroConverterConfig.IS_KEY, false);
        schemaConfigs.put(AvroConverterConfig.AUTO_REGISTER_SCHEMAS, true);
        schemaConfigs.put(AvroConverterConfig.SCHEMA_REGISTRY_URL, SCHEMA_REGISTRY_URL);
        avroConverter.configure(schemaConfigs);
    }

    /**
     * From connect record test
     */
    @Test
    public void fromConverterTest(){
//        Schema schema = SchemaBuilder.struct().name("test")
//                .field("test_int", SchemaBuilder.int32().build())
//                .field("test_bool", SchemaBuilder.bool().build())
//                .field("test_str", SchemaBuilder.string().build())
//                .field("test_str_01", SchemaBuilder.string().build())
//                .field("test_str_02", SchemaBuilder.string().build())
//                .field("test_str_03", SchemaBuilder.string().build())
//                .field("test_str_04", SchemaBuilder.string().build())
//                .build();
//        Struct struct =  new Struct(schema);
//        struct.put("test_int", new Integer(1000000));
//        struct.put("test_bool", true);
//        struct.put("test_str", "test-str");
//        struct.put("test_str_01", "test_str_01");
//        struct.put("test_str_02", "test_str_02");
//        struct.put("test_str_03", "test_str_03");
//        struct.put("test_str_04", "test_str_03");
//        byte[] value = avroConverter.fromConnectData(topic, schema, struct);
//
//        SchemaAndValue schemaAndValue = avroConverter.toConnectData(topic, value);
//        Assert.assertEquals(schema, schemaAndValue.schema());
//        Assert.assertEquals(struct, schemaAndValue.value());
    }

}
