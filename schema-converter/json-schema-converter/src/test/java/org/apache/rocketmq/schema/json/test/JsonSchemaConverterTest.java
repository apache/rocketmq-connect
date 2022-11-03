package org.apache.rocketmq.schema.json.test;


import io.openmessaging.connector.api.data.Schema;
import io.openmessaging.connector.api.data.SchemaAndValue;
import io.openmessaging.connector.api.data.SchemaBuilder;
import io.openmessaging.connector.api.data.Struct;
import org.apache.rocketmq.schema.json.JsonSchemaConverter;
import org.apache.rocketmq.schema.json.JsonSchemaConverterConfig;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

/**
 * json schema converter test
 */
public class JsonSchemaConverterTest {

    private JsonSchemaConverter jsonSchemaConverter;

    private String topic = "json-test-topic-082";

    @Before
    public void init(){
//        jsonSchemaConverter = new JsonSchemaConverter();
//        Map<String, Object> schemaConfigs = new HashMap<>();
//        schemaConfigs.put(JsonSchemaConverterConfig.IS_KEY, false);
//        schemaConfigs.put(JsonSchemaConverterConfig.AUTO_REGISTER_SCHEMAS, true);
//        schemaConfigs.put(JsonSchemaConverterConfig.SCHEMA_REGISTRY_URL, "http://localhost:8080");
//        schemaConfigs.put(JsonSchemaConverterConfig.VALIDATE_ENABLED, true);
//        jsonSchemaConverter.configure(schemaConfigs);
    }

    /**
     * From connect record test
     */
    @Test
    public void fromConnectRecordTest(){
//        Schema schema = SchemaBuilder.struct().required()
//                .field("test-int", SchemaBuilder.int32().required().build())
//                .field("test-bool", SchemaBuilder.bool().required().build())
//                .field("test-str", SchemaBuilder.string().required().build())
//                .field("test-str-01", SchemaBuilder.string().required().build())
//                .build();
//        Struct struct =  new Struct(schema);
//        struct.put("test-int", new Integer(1000000));
//        struct.put("test-bool", true);
//        struct.put("test-str", "test-str");
//        struct.put("test-str-01", "test-str-01");
//        byte[] convertData = jsonSchemaConverter.fromConnectData(topic, schema, struct);
//        SchemaAndValue schemaAndValue = jsonSchemaConverter.toConnectData(topic, convertData);
//        Assert.assertEquals(schema, schemaAndValue.schema());
//        Assert.assertEquals(struct, schemaAndValue.value());
    }

}
