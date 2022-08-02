package org.apache.rocketmq.connect.transforms.util;

import io.openmessaging.connector.api.data.Schema;
import io.openmessaging.connector.api.data.SchemaBuilder;

/**
 * schema util
 */
public class SchemaUtil {
    public static Schema INT8_SCHEMA = SchemaBuilder.int8().build();
    public static Schema INT16_SCHEMA = SchemaBuilder.int16().build();
    public static Schema INT32_SCHEMA = SchemaBuilder.int32().build();
    public static Schema INT64_SCHEMA = SchemaBuilder.int64().build();
    public static Schema FLOAT32_SCHEMA = SchemaBuilder.float32().build();
    public static Schema FLOAT64_SCHEMA = SchemaBuilder.float64().build();
    public static Schema BOOLEAN_SCHEMA = SchemaBuilder.bool().build();
    public static Schema STRING_SCHEMA = SchemaBuilder.string().build();
    public static Schema BYTES_SCHEMA = SchemaBuilder.bytes().build();

    public static Schema OPTIONAL_INT8_SCHEMA = SchemaBuilder.int8().optional().build();
    public static Schema OPTIONAL_INT16_SCHEMA = SchemaBuilder.int16().optional().build();
    public static Schema OPTIONAL_INT32_SCHEMA = SchemaBuilder.int32().optional().build();
    public static Schema OPTIONAL_INT64_SCHEMA = SchemaBuilder.int64().optional().build();
    public static Schema OPTIONAL_FLOAT32_SCHEMA = SchemaBuilder.float32().optional().build();
    public static Schema OPTIONAL_FLOAT64_SCHEMA = SchemaBuilder.float64().optional().build();
    public static Schema OPTIONAL_BOOLEAN_SCHEMA = SchemaBuilder.bool().optional().build();
    public static Schema OPTIONAL_STRING_SCHEMA = SchemaBuilder.string().optional().build();
    public static Schema OPTIONAL_BYTES_SCHEMA = SchemaBuilder.bytes().optional().build();
}
