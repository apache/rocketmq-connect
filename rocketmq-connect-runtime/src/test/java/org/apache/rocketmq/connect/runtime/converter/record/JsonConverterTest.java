/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rocketmq.connect.runtime.converter.record;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.util.TypeUtils;
import com.google.common.collect.Maps;
import io.openmessaging.connector.api.data.Schema;
import io.openmessaging.connector.api.data.SchemaAndValue;
import io.openmessaging.connector.api.data.SchemaBuilder;
import io.openmessaging.connector.api.data.Struct;
import io.openmessaging.connector.api.data.logical.Date;
import io.openmessaging.connector.api.data.logical.Decimal;
import io.openmessaging.connector.api.data.logical.Time;
import io.openmessaging.connector.api.data.logical.Timestamp;
import io.openmessaging.connector.api.errors.ConnectException;
import org.apache.rocketmq.connect.runtime.converter.record.json.DecimalFormat;
import org.apache.rocketmq.connect.runtime.converter.record.json.JsonConverter;
import org.apache.rocketmq.connect.runtime.converter.record.json.JsonConverterConfig;
import org.apache.rocketmq.connect.runtime.converter.record.json.JsonSchema;
import org.junit.Before;
import org.junit.Test;

import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.fail;

public class JsonConverterTest {
    private static final String TOPIC = "topic";
    private final JSONObject objectMapper = new JSONObject();
    private final JsonConverter converter = new JsonConverter();

    @Before
    public void setUp() {
        Map<String, Object> config = Maps.newConcurrentMap();
        config.put(JsonConverterConfig.SCHEMAS_ENABLE_CONFIG, true);
        converter.configure(config);
    }

    // Schema metadata

    @Test
    public void testConnectSchemaMetadataTranslation() {
        // this validates the non-type fields are translated and handled properly
        SchemaAndValue schemaAndValue = converter.toConnectData(TOPIC, "{ \"schema\": { \"type\": \"boolean\",\"optional\": \"true\" }, \"payload\": true }".getBytes());
        SchemaAndValue newSchemaAndValue = new SchemaAndValue(SchemaBuilder.bool().build(), true);
        assertEquals(newSchemaAndValue, schemaAndValue);

        SchemaAndValue schemaAndValue01 = converter.toConnectData(TOPIC, "{ \"schema\": { \"type\": \"boolean\", \"optional\": true }, \"payload\": null }".getBytes());
        SchemaAndValue newSchemaAndValue01 = new SchemaAndValue(SchemaBuilder.bool().optional().build(), null);
        assertEquals(schemaAndValue01, newSchemaAndValue01);

        SchemaAndValue schemaAndValue02 = converter.toConnectData(TOPIC, "{ \"schema\": { \"type\": \"boolean\", \"optional\": true, \"default\": true}, \"payload\": true }".getBytes());
        SchemaAndValue newSchemaAndValue02 = new SchemaAndValue(SchemaBuilder.bool().defaultValue(true).build(), true);
        assertEquals(schemaAndValue02, newSchemaAndValue02);


        SchemaAndValue schemaAndValue03 = converter.toConnectData(TOPIC, "{ \"schema\": { \"type\": \"boolean\", \"optional\": true, \"name\": \"bool\", \"version\": 2, \"doc\": \"the documentation\", \"parameters\": { \"foo\": \"bar\" }}, \"payload\": true }".getBytes());
        SchemaAndValue newSchemaAndValue03 = new SchemaAndValue(SchemaBuilder.bool().name("bool").version(2).doc("the documentation").parameter("foo", "bar").build(), true);

        assertEquals(newSchemaAndValue03, schemaAndValue03);
    }

    // Schema types

    @Test
    public void booleanToConnect() {
        assertEquals(
                new SchemaAndValue(SchemaBuilder.bool().build(), true),
                converter.toConnectData(TOPIC, "{ \"schema\": { \"type\": \"boolean\",\"optional\": true }, \"payload\": true }".getBytes())
        );
        assertEquals(
                new SchemaAndValue(SchemaBuilder.bool().build(), false),
                converter.toConnectData(TOPIC, "{ \"schema\": { \"type\": \"boolean\" ,\"optional\": false}, \"payload\": false }".getBytes())
        );
    }

    @Test
    public void byteToConnect() {
        assertEquals(
                new SchemaAndValue(SchemaBuilder.int8().build(), (byte) 12),
                converter.toConnectData(TOPIC, "{ \"schema\": { \"type\": \"int8\",\"optional\": true }, \"payload\": 12 }".getBytes())
        );
    }

    @Test
    public void shortToConnect() {
        assertEquals(
                new SchemaAndValue(SchemaBuilder.int16().build(), (short) 12),
                converter.toConnectData(TOPIC, "{ \"schema\": { \"type\": \"int16\" ,\"optional\": true}, \"payload\": 12 }".getBytes())
        );
    }

    @Test
    public void intToConnect() {
        assertEquals(
                new SchemaAndValue(SchemaBuilder.int32().build(), 12),
                converter.toConnectData(TOPIC, "{ \"schema\": { \"type\": \"int32\" ,\"optional\": true}, \"payload\": 12 }".getBytes())
        );
    }

    @Test
    public void longToConnect() {
        assertEquals(
                new SchemaAndValue(SchemaBuilder.int64().build(), 12L),
                converter.toConnectData(TOPIC, "{ \"schema\": { \"type\": \"int64\" ,\"optional\": true }, \"payload\": 12 }".getBytes())
        );
        assertEquals(
                new SchemaAndValue(SchemaBuilder.int64().build(), 4398046511104L),
                converter.toConnectData(TOPIC, "{ \"schema\": { \"type\": \"int64\" ,\"optional\": true }, \"payload\": 4398046511104 }".getBytes())
        );
    }

    @Test(expected = ClassCastException.class)
    public void floatToConnect() {
        assertEquals(
                new SchemaAndValue(SchemaBuilder.float32().build(), 12.34f),
                converter.toConnectData(TOPIC, "{ \"schema\": { \"type\": \"float\" ,\"optional\": true }, \"payload\": 12.34 }".getBytes())
        );
    }

    @Test
    public void doubleToConnect() {
        assertEquals(
                new SchemaAndValue(SchemaBuilder.float64().build(), 12.34),
                converter.toConnectData(TOPIC, "{ \"schema\": { \"type\": \"double\",\"optional\": true }, \"payload\": 12.34 }".getBytes())
        );
    }


    @Test
    public void bytesToConnect() throws UnsupportedEncodingException {
        ByteBuffer reference = ByteBuffer.wrap("test-string".getBytes("UTF-8"));
        String msg = "{ \"schema\": { \"type\": \"bytes\",\"optional\": true }, \"payload\": \"dGVzdC1zdHJpbmc=\" }";
        SchemaAndValue schemaAndValue = converter.toConnectData(TOPIC, msg.getBytes());
        ByteBuffer converted = ByteBuffer.wrap((byte[]) schemaAndValue.value());
        assertEquals(reference, converted);
    }

    @Test
    public void stringToConnect() {
        assertEquals(
                new SchemaAndValue(SchemaBuilder.string().build(), "foo-bar-baz"),
                converter.toConnectData(TOPIC, "{ \"schema\": { \"type\": \"string\",\"optional\": true }, \"payload\": \"foo-bar-baz\" }".getBytes()
                )
        );
    }

    @Test
    public void arrayToConnect() {
        byte[] arrayJson = "{ \"schema\": { \"type\": \"array\",  \"items\": { \"type\" : \"int32\",\"optional\": true } ,\"optional\": true}, \"payload\": [1, 2, 3] }".getBytes();
        assertEquals(
                new SchemaAndValue(
                        SchemaBuilder.array(SchemaBuilder.int32().build()).build(),
                        Arrays.asList(1, 2, 3)
                ),
                converter.toConnectData(TOPIC, arrayJson)
        );
    }

    @Test
    public void mapToConnectStringKeys() {
        byte[] mapJson = "{ \"schema\": { \"type\": \"map\", \"keys\": { \"type\" : \"string\",\"optional\": true}, \"values\": { \"type\" : \"int32\" ,\"optional\": true } ,\"optional\": true}, \"payload\": { \"key1\": 12, \"key2\": 15} }".getBytes();
        Map<String, Integer> expected = new HashMap<>();
        expected.put("key1", 12);
        expected.put("key2", 15);
        assertEquals(
                new SchemaAndValue(
                        SchemaBuilder.map(SchemaBuilder.string().build(), SchemaBuilder.int32().build()).build(), expected
                ),
                converter.toConnectData(TOPIC, mapJson)
        );
    }

    @Test
    public void mapToConnectNonStringKeys() {
        byte[] mapJson = "{ \"schema\": { \"type\": \"map\", \"keys\": { \"type\" : \"int32\" }, \"values\": { \"type\" : \"int32\" } }, \"payload\": [ [1, 12], [2, 15] ] }".getBytes();
        Map<Integer, Integer> expected = new HashMap<>();
        expected.put(1, 12);
        expected.put(2, 15);
        assertEquals(
                new SchemaAndValue(
                        SchemaBuilder.map(SchemaBuilder.int32().build(), SchemaBuilder.int32().build()).build(),
                        expected
                ),
                converter.toConnectData(TOPIC, mapJson)
        );
    }

    @Test
    public void structToConnect() {
        byte[] structJson = "{ \"schema\": { \"type\": \"struct\", \"fields\": [{ \"field\": \"field1\", \"type\": \"boolean\" }, { \"field\": \"field2\", \"type\": \"string\" }] }, \"payload\": { \"field1\": true, \"field2\": \"string\" } }".getBytes();
        Schema expectedSchema =
                SchemaBuilder
                        .struct()
                        .field("field1", SchemaBuilder.bool().build())
                        .field("field2", SchemaBuilder.string().build()).build();
        Struct expected = new Struct(expectedSchema)
                .put("field1", true)
                .put("field2", "string");
        SchemaAndValue converted = converter.toConnectData(TOPIC, structJson);
        assertEquals(new SchemaAndValue(expectedSchema, expected).toString(), converted.toString());
    }

    @Test
    public void structWithOptionalFieldToConnect() {
        byte[] structJson = "{ \"schema\": { \"type\": \"struct\", \"fields\": [{ \"field\":\"optional\", \"type\": \"string\", \"optional\": true }, {  \"field\": \"required\", \"type\": \"string\", \"optional\": true }] }, \"payload\": { \"required\": \"required\" } }".getBytes();
        Schema expectedSchema = SchemaBuilder
                .struct().optional()
                .field("optional", SchemaBuilder.string().build())
                .field("required", SchemaBuilder.string().optional().build()).build();
        expectedSchema.setOptional(true);
        Struct expected = new Struct(expectedSchema).put("required", "required");
        SchemaAndValue converted = converter.toConnectData(TOPIC, structJson);
        assertEquals(
                new SchemaAndValue(expectedSchema, expected),
                converted
        );
    }

    @Test
    public void nullToConnect() {
        // When schemas are enabled, trying to decode a tombstone should be an empty envelope
        // the behavior is the same as when the json is "{ "schema": null, "payload": null }"
        // to keep compatibility with the record
        SchemaAndValue converted = converter.toConnectData(TOPIC, null);
        assertEquals(SchemaAndValue.NULL, converted);
    }

    @Test
    public void nullSchemaPrimitiveToConnect() {
        SchemaAndValue converted = converter.toConnectData(TOPIC, "{ \"schema\": null, \"payload\": null }".getBytes());
        assertEquals(SchemaAndValue.NULL, converted);

        converted = converter.toConnectData(TOPIC, "{ \"schema\": null, \"payload\": true }".getBytes());
        assertEquals(new SchemaAndValue(null, true), converted);

        // Integers: Connect has more data types, and JSON unfortunately mixes all number types. We try to preserve
        // info as best we can, so we always use the largest integer and floating point numbers we can and have Jackson
        // determine if it's an integer or not
        converted = converter.toConnectData(TOPIC, "{ \"schema\": null, \"payload\": 12 }".getBytes());
        assertEquals(new SchemaAndValue(null, 12), converted);

        converted = converter.toConnectData(TOPIC, "{ \"schema\": null, \"payload\": 12.24 }".getBytes());
        assertEquals(new SchemaAndValue(null, 12.24), converted);

        converted = converter.toConnectData(TOPIC, "{ \"schema\": null, \"payload\": \"a string\" }".getBytes());
        assertEquals(new SchemaAndValue(null, "a string"), converted);

        converted = converter.toConnectData(TOPIC, "{ \"schema\": null, \"payload\": [1, \"2\", 3] }".getBytes());
        assertEquals(new SchemaAndValue(null, Arrays.asList(1L, "2", 3L)).toString(), converted.toString());

        converted = converter.toConnectData(TOPIC, "{ \"schema\": null, \"payload\": { \"field1\": 1, \"field2\": 2} }".getBytes());
        Map<String, Long> obj = new HashMap<>();
        obj.put("field1", 1L);
        obj.put("field2", 2L);
        assertEquals(new SchemaAndValue(null, obj).toString(), converted.toString());
    }

    @Test
    public void decimalToConnect() {
        Schema schema = Decimal.schema(2);
        BigDecimal reference = new BigDecimal(new BigInteger("-390"), 2);
        String msg = "{ \"schema\": { \"type\": \"bytes\", \"name\": \"io.openmessaging.connector.api.data.logical.Decimal\", \"version\": 1, \"parameters\": { \"scale\": \"2\" } }, \"payload\": \"1.56\" }";
        SchemaAndValue schemaAndValue = converter.toConnectData(TOPIC, msg.getBytes());
        BigDecimal converted = (BigDecimal) schemaAndValue.value();
        assertEquals(schema, schemaAndValue.schema());
        assertEquals(reference, converted);
    }

    @Test
    public void decimalToConnectOptional() {
        Schema schema = Decimal.builder(2).optional().build();
        String msg = "{ \"schema\": { \"type\": \"bytes\", \"name\": \"io.openmessaging.connector.api.data.logical.Decimal\", \"version\": 1, \"optional\": true, \"parameters\": { \"scale\": \"2\" } }, \"payload\": null }";
        SchemaAndValue schemaAndValue = converter.toConnectData(TOPIC, msg.getBytes());
        assertEquals(schema, schemaAndValue.schema());
        assertNull(schemaAndValue.value());
    }

    @Test
    public void decimalToConnectWithDefaultValue() {
        BigDecimal reference = new BigDecimal(new BigInteger("-390"), 2);
        Schema schema = Decimal.builder(2).defaultValue(reference).build();
        String msg = "{ \"schema\": { \"type\": \"bytes\", \"name\": \"io.openmessaging.connector.api.data.logical.Decimal\", \"version\": 1, \"default\": \"1.56\", \"parameters\": { \"scale\": \"2\" } }, \"payload\": null }";
        SchemaAndValue schemaAndValue = converter.toConnectData(TOPIC, msg.getBytes());
        assertEquals(schema, schemaAndValue.schema());
        assertEquals(reference, schemaAndValue.value());
    }

    @Test
    public void decimalToConnectOptionalWithDefaultValue() {
        BigDecimal reference = new BigDecimal(new BigInteger("-390"), 2);
        Schema schema = Decimal.builder(2).optional().defaultValue(reference).build();
        String msg = "{ \"schema\": { \"type\": \"bytes\", \"name\": \"io.openmessaging.connector.api.data.logical.Decimal\", \"version\": 1, \"optional\": true, \"default\": \"1.56\", \"parameters\": { \"scale\": \"2\" } }, \"payload\": null }";
        SchemaAndValue schemaAndValue = converter.toConnectData(TOPIC, msg.getBytes());
        assertEquals(schema, schemaAndValue.schema());
        assertEquals(reference, schemaAndValue.value());
    }

    @Test
    public void numericDecimalToConnect() {
        BigDecimal reference = new BigDecimal(new BigInteger("156"), 2);
        Schema schema = Decimal.schema(2);
        String msg = "{ \"schema\": { \"type\": \"bytes\", \"name\": \"io.openmessaging.connector.api.data.logical.Decimal\", \"version\": 1, \"parameters\": { \"scale\": \"2\" } }, \"payload\": 1.56 }";
        SchemaAndValue schemaAndValue = converter.toConnectData(TOPIC, msg.getBytes());
        assertEquals(schema, schemaAndValue.schema());
        assertEquals(reference, schemaAndValue.value());
    }

    @Test
    public void numericDecimalWithTrailingZerosToConnect() {
        BigDecimal reference = new BigDecimal(new BigInteger("15600"), 4);
        Schema schema = Decimal.schema(4);
        String msg = "{ \"schema\": { \"type\": \"bytes\", \"name\": \"io.openmessaging.connector.api.data.logical.Decimal\", \"version\": 1, \"parameters\": { \"scale\": \"4\" } }, \"payload\": 1.5600 }";
        SchemaAndValue schemaAndValue = converter.toConnectData(TOPIC, msg.getBytes());
        assertEquals(schema, schemaAndValue.schema());
        assertEquals(reference, schemaAndValue.value());
    }

    @Test
    public void highPrecisionNumericDecimalToConnect() {
        // this number is too big to be kept in a float64!
        BigDecimal reference = new BigDecimal("1.23456789123456789");
        Schema schema = Decimal.schema(17);
        String msg = "{ \"schema\": { \"type\": \"bytes\", \"name\": \"io.openmessaging.connector.api.data.logical.Decimal\", \"version\": 1, \"parameters\": { \"scale\": \"17\" } }, \"payload\": 1.23456789123456789 }";
        SchemaAndValue schemaAndValue = converter.toConnectData(TOPIC, msg.getBytes());
        assertEquals(schema, schemaAndValue.schema());
        assertEquals(reference, schemaAndValue.value());
    }

    @Test
    public void dateToConnect() {
        Schema schema = Date.SCHEMA;
        GregorianCalendar calendar = new GregorianCalendar(1970, Calendar.JANUARY, 1, 0, 0, 0);
        calendar.setTimeZone(TimeZone.getTimeZone("UTC"));
        calendar.add(Calendar.DATE, 10000);
        java.util.Date reference = calendar.getTime();
        String msg = "{ \"schema\": { \"type\": \"int32\", \"name\": \"io.openmessaging.connector.api.data.logical.Date\", \"version\": 1 }, \"payload\": 10000 }";
        SchemaAndValue schemaAndValue = converter.toConnectData(TOPIC, msg.getBytes());
        java.util.Date converted = (java.util.Date) schemaAndValue.value();
        assertEquals(schema, schemaAndValue.schema());
        assertEquals(reference, converted);
    }

    @Test
    public void dateToConnectOptional() {
        Schema schema = Date.builder().optional().build();
        String msg = "{ \"schema\": { \"type\": \"int32\", \"name\": \"io.openmessaging.connector.api.data.logical.Date\", \"version\": 1, \"optional\": true }, \"payload\": null }";
        SchemaAndValue schemaAndValue = converter.toConnectData(TOPIC, msg.getBytes());
        assertEquals(schema, schemaAndValue.schema());
        assertNull(schemaAndValue.value());
    }

    @Test
    public void dateToConnectWithDefaultValue() {
        java.util.Date reference = new java.util.Date(0);
        Schema schema = Date.builder().defaultValue(reference).build();
        String msg = "{ \"schema\": { \"type\": \"int32\", \"name\": \"io.openmessaging.connector.api.data.logical.Date\", \"version\": 1, \"default\": 0 }, \"payload\": null }";
        SchemaAndValue schemaAndValue = converter.toConnectData(TOPIC, msg.getBytes());
        assertEquals(schema, schemaAndValue.schema());
        assertEquals(reference, schemaAndValue.value());
    }

    @Test
    public void dateToConnectOptionalWithDefaultValue() {
        java.util.Date reference = new java.util.Date(0);
        Schema schema = Date.builder().optional().defaultValue(reference).build();
        String msg = "{ \"schema\": { \"type\": \"int32\", \"name\": \"io.openmessaging.connector.api.data.logical.Date\", \"version\": 1, \"optional\": true, \"default\": 0 }, \"payload\": null }";
        SchemaAndValue schemaAndValue = converter.toConnectData(TOPIC, msg.getBytes());
        assertEquals(schema, schemaAndValue.schema());
        assertEquals(reference, schemaAndValue.value());
    }

    @Test
    public void timeToConnect() {
        Schema schema = Time.SCHEMA;
        GregorianCalendar calendar = new GregorianCalendar(1970, Calendar.JANUARY, 1, 0, 0, 0);
        calendar.setTimeZone(TimeZone.getTimeZone("UTC"));
        calendar.add(Calendar.MILLISECOND, 14400000);
        java.util.Date reference = calendar.getTime();
        String msg = "{ \"schema\": { \"type\": \"int32\", \"name\": \"io.openmessaging.connector.api.data.logical.Time\", \"version\": 1 }, \"payload\": 14400000 }";
        SchemaAndValue schemaAndValue = converter.toConnectData(TOPIC, msg.getBytes());
        java.util.Date converted = (java.util.Date) schemaAndValue.value();
        assertEquals(schema, schemaAndValue.schema());
        assertEquals(reference, converted);
    }

    @Test
    public void timeToConnectOptional() {
        Schema schema = Time.builder().optional().build();
        String msg = "{ \"schema\": { \"type\": \"int32\", \"name\": \"io.openmessaging.connector.api.data.logical.Time\", \"version\": 1, \"optional\": true }, \"payload\": null }";
        SchemaAndValue schemaAndValue = converter.toConnectData(TOPIC, msg.getBytes());
        assertEquals(schema, schemaAndValue.schema());
        assertNull(schemaAndValue.value());
    }

    @Test
    public void timeToConnectWithDefaultValue() {
        java.util.Date reference = new java.util.Date(0);
        Schema schema = Time.builder().defaultValue(reference).build();
        String msg = "{ \"schema\": { \"type\": \"int32\", \"name\": \"io.openmessaging.connector.api.data.logical.Time\", \"version\": 1, \"default\": 0 }, \"payload\": null }";
        SchemaAndValue schemaAndValue = converter.toConnectData(TOPIC, msg.getBytes());
        assertEquals(schema, schemaAndValue.schema());
        assertEquals(reference, schemaAndValue.value());
    }

    @Test
    public void timeToConnectOptionalWithDefaultValue() {
        java.util.Date reference = new java.util.Date(0);
        Schema schema = Time.builder().optional().defaultValue(reference).build();
        String msg = "{ \"schema\": { \"type\": \"int32\", \"name\": \"io.openmessaging.connector.api.data.logical.Time\", \"version\": 1, \"optional\": true, \"default\": 0 }, \"payload\": null }";
        SchemaAndValue schemaAndValue = converter.toConnectData(TOPIC, msg.getBytes());
        assertEquals(schema, schemaAndValue.schema());
        assertEquals(reference, schemaAndValue.value());
    }

    @Test
    public void timestampToConnect() {
        Schema schema = Timestamp.SCHEMA;
        GregorianCalendar calendar = new GregorianCalendar(1970, Calendar.JANUARY, 1, 0, 0, 0);
        calendar.setTimeZone(TimeZone.getTimeZone("UTC"));
        calendar.add(Calendar.MILLISECOND, 2000000000);
        calendar.add(Calendar.MILLISECOND, 2000000000);
        java.util.Date reference = calendar.getTime();
        String msg = "{ \"schema\": { \"type\": \"int64\", \"name\": \"io.openmessaging.connector.api.data.logical.Timestamp\", \"version\": 1 }, \"payload\": 4000000000 }";
        SchemaAndValue schemaAndValue = converter.toConnectData(TOPIC, msg.getBytes());
        // java.util.Date converted = (java.util.Date) schemaAndValue.value();
        java.util.Date converted = (java.util.Date) schemaAndValue.value();
        assertEquals(schema, schemaAndValue.schema());
        assertEquals(reference, converted);
    }

    @Test
    public void timestampToConnectOptional() {
        Schema schema = Timestamp.builder().optional().build();
        String msg = "{ \"schema\": { \"type\": \"int64\", \"name\": \"io.openmessaging.connector.api.data.logical.Timestamp\", \"version\": 1, \"optional\": true }, \"payload\": null }";
        SchemaAndValue schemaAndValue = converter.toConnectData(TOPIC, msg.getBytes());
        assertEquals(schema, schemaAndValue.schema());
        assertNull(schemaAndValue.value());
    }

    @Test
    public void timestampToConnectWithDefaultValue() {
        Schema schema = Timestamp.builder().defaultValue(new java.util.Date(42)).build();
        String msg = "{ \"schema\": { \"type\": \"int64\", \"name\": \"io.openmessaging.connector.api.data.logical.Timestamp\", \"version\": 1, \"default\": 42 }, \"payload\": null }";
        SchemaAndValue schemaAndValue = converter.toConnectData(TOPIC, msg.getBytes());
        assertEquals(schema, schemaAndValue.schema());
        assertEquals(new java.util.Date(42), schemaAndValue.value());
    }

    @Test
    public void timestampToConnectOptionalWithDefaultValue() {
        Schema schema = Timestamp.builder().optional().defaultValue(new java.util.Date(42)).build();
        String msg = "{ \"schema\": { \"type\": \"int64\", \"name\": \"io.openmessaging.connector.api.data.logical.Timestamp\", \"version\": 1,  \"optional\": true, \"default\": 42 }, \"payload\": null }";
        SchemaAndValue schemaAndValue = converter.toConnectData(TOPIC, msg.getBytes());
        assertEquals(schema, schemaAndValue.schema());
        assertEquals(new java.util.Date(42), schemaAndValue.value());
    }

    // Schema metadata

    @Test
    public void testJsonSchemaMetadataTranslation() {
        JSONObject converted = parse(converter.fromConnectData(TOPIC, SchemaBuilder.bool().build(), true));
        assertEquals(parse("{ \"type\": \"boolean\", \"optional\": true }"), converted.get(JsonSchema.ENVELOPE_SCHEMA_FIELD_NAME));
        assertEquals(true, converted.get(JsonSchema.ENVELOPE_PAYLOAD_FIELD_NAME));

        converted = parse(converter.fromConnectData(TOPIC, SchemaBuilder.bool().build(), null));
        assertEquals(parse("{ \"type\": \"boolean\", \"optional\": true }"), converted.get(JsonSchema.ENVELOPE_SCHEMA_FIELD_NAME));


        converted = parse(converter.fromConnectData(TOPIC, SchemaBuilder.bool().required().defaultValue(true).build(), true));
        assertEquals(parse("{ \"type\": \"boolean\", \"optional\": false, \"default\": true }"), converted.get(JsonSchema.ENVELOPE_SCHEMA_FIELD_NAME));
        assertEquals(true, converted.get(JsonSchema.ENVELOPE_PAYLOAD_FIELD_NAME));

        converted = parse(converter.fromConnectData(TOPIC, SchemaBuilder.bool().required().name("bool").version(3).doc("the documentation").parameter("foo", "bar").build(), true));
        assertEquals(
                parse("{ \"type\": \"boolean\", \"optional\": false, \"name\": \"bool\", \"version\": 3, \"doc\": \"the documentation\", \"parameters\": { \"foo\": \"bar\" }}"),
                converted.get(JsonSchema.ENVELOPE_SCHEMA_FIELD_NAME)
        );
        assertEquals(true, converted.get(JsonSchema.ENVELOPE_PAYLOAD_FIELD_NAME));
    }


    // Schema types

    @Test
    public void booleanToJson() {
        JSONObject converted = parse(converter.fromConnectData(TOPIC, SchemaBuilder.bool().required().build(), true));
        assertEquals(parse("{ \"type\": \"boolean\", \"optional\": false }"), converted.get(JsonSchema.ENVELOPE_SCHEMA_FIELD_NAME));
        assertEquals(true, converted.get(JsonSchema.ENVELOPE_PAYLOAD_FIELD_NAME));
    }

    @Test
    public void byteToJson() {
        JSONObject converted = parse(converter.fromConnectData(TOPIC, SchemaBuilder.int8().required().build(), (byte) 12));
        assertEquals(parse("{ \"type\": \"int8\", \"optional\": false }"), converted.get(JsonSchema.ENVELOPE_SCHEMA_FIELD_NAME));
        assertEquals(12, converted.get(JsonSchema.ENVELOPE_PAYLOAD_FIELD_NAME));
    }

    @Test
    public void shortToJson() {
        JSONObject converted = parse(converter.fromConnectData(TOPIC, SchemaBuilder.int16().required().build(), (short) 12));
        assertEquals(parse("{ \"type\": \"int16\", \"optional\": false }"), converted.get(JsonSchema.ENVELOPE_SCHEMA_FIELD_NAME));
        assertEquals(12, converted.get(JsonSchema.ENVELOPE_PAYLOAD_FIELD_NAME));
    }

    @Test
    public void intToJson() {
        JSONObject converted = parse(converter.fromConnectData(TOPIC, SchemaBuilder.int32().build(), 12));
        assertEquals(parse("{ \"type\": \"int32\", \"optional\": true }"), converted.get(JsonSchema.ENVELOPE_SCHEMA_FIELD_NAME));
        assertEquals(12, converted.get(JsonSchema.ENVELOPE_PAYLOAD_FIELD_NAME));
    }

    @Test
    public void longToJson() {
        JSONObject converted = parse(converter.fromConnectData(TOPIC, SchemaBuilder.int64().required().build(), 4398046511104L));
        assertEquals(parse("{ \"type\": \"int64\", \"optional\": false }"), converted.get(JsonSchema.ENVELOPE_SCHEMA_FIELD_NAME));
        assertEquals(4398046511104L, converted.get(JsonSchema.ENVELOPE_PAYLOAD_FIELD_NAME));
    }

    @Test
    public void floatToJson() {
        JSONObject converted = parse(converter.fromConnectData(TOPIC, SchemaBuilder.float32().required().build(), 12.34f));
        assertEquals(parse("{ \"type\": \"float\", \"optional\": false }"), converted.get(JsonSchema.ENVELOPE_SCHEMA_FIELD_NAME));
        assert 12.34f == converted.getFloat(JsonSchema.ENVELOPE_PAYLOAD_FIELD_NAME);
    }

    @Test
    public void doubleToJson() {
        JSONObject converted = parse(converter.fromConnectData(TOPIC, SchemaBuilder.float64().required().build(), 12.34));
        assertEquals(parse("{ \"type\": \"double\", \"optional\": false }"), converted.get(JsonSchema.ENVELOPE_SCHEMA_FIELD_NAME));
        assert 12.34 == converted.getDouble(JsonSchema.ENVELOPE_PAYLOAD_FIELD_NAME);
    }


    @Test
    public void bytesToJson() {
        JSONObject converted = parse(converter.fromConnectData(TOPIC, SchemaBuilder.bytes().required().build(), "test-string".getBytes()));
        assertEquals(parse("{ \"type\": \"bytes\", \"optional\": false }"), converted.get(JsonSchema.ENVELOPE_SCHEMA_FIELD_NAME));
        byte[] bytes = TypeUtils.castToBytes(converted.get(JsonSchema.ENVELOPE_PAYLOAD_FIELD_NAME).toString());
        assertEquals(
                ByteBuffer.wrap("test-string".getBytes()),
                ByteBuffer.wrap(bytes)
        );
    }

    @Test
    public void stringToJson() {
        JSONObject converted = parse(converter.fromConnectData(TOPIC, SchemaBuilder.string().required().build(), "test-string"));
        assertEquals(parse("{ \"type\": \"string\", \"optional\": false }"), converted.get(JsonSchema.ENVELOPE_SCHEMA_FIELD_NAME));
        assertEquals("test-string", converted.get(JsonSchema.ENVELOPE_PAYLOAD_FIELD_NAME));
    }

    @Test
    public void arrayToJson() {
        Schema int32Array = SchemaBuilder.array(SchemaBuilder.int32().required().build()).required().build();
        JSONObject converted = parse(converter.fromConnectData(TOPIC, int32Array, Arrays.asList(1, 2, 3)));
        assertEquals(parse("{ \"type\": \"array\", \"items\": { \"type\": \"int32\", \"optional\": false }, \"optional\": false }"),
                converted.get(JsonSchema.ENVELOPE_SCHEMA_FIELD_NAME));
        assertEquals(Arrays.asList(1, 2, 3),
                converted.get(JsonSchema.ENVELOPE_PAYLOAD_FIELD_NAME));
    }

    @Test
    public void mapToJsonStringKeys() {
        Schema stringIntMap = SchemaBuilder.map(SchemaBuilder.string().required().build(), SchemaBuilder.int32().required().build()).required().build();
        Map<String, Integer> input = new HashMap<>();
        input.put("key1", 12);
        input.put("key2", 15);
        JSONObject converted = parse(converter.fromConnectData(TOPIC, stringIntMap, input));
        assertEquals(parse("{ \"type\": \"map\", \"keys\": { \"type\" : \"string\", \"optional\": false }, \"values\": { \"type\" : \"int32\", \"optional\": false }, \"optional\": false }"),
                converted.get(JsonSchema.ENVELOPE_SCHEMA_FIELD_NAME));
    }

    @Test
    public void mapToJsonNonStringKeys() {
        Schema intIntMap = SchemaBuilder.map(SchemaBuilder.int32().required().build(), SchemaBuilder.int32().required().build()).required().build();
        Map<Integer, Integer> input = new HashMap<>();
        input.put(1, 12);
        input.put(2, 15);
        JSONObject converted = parse(converter.fromConnectData(TOPIC, intIntMap, input));
        assertEquals(parse("{ \"type\": \"map\", \"keys\": { \"type\" : \"int32\", \"optional\": false }, \"values\": { \"type\" : \"int32\", \"optional\": false }, \"optional\": false }"),
                converted.get(JsonSchema.ENVELOPE_SCHEMA_FIELD_NAME));

        JSONArray payload = (JSONArray) converted.get(JsonSchema.ENVELOPE_PAYLOAD_FIELD_NAME);
        assertEquals(2, payload.size());
        Set<JSONArray> payloadEntries = new HashSet<>();
        for (Object elem : payload.toArray()) {
            JSONArray array = (JSONArray) elem;
            payloadEntries.add(array);
        }
        JSONArray values01 = new JSONArray();
        values01.add(1);
        values01.add(12);
        JSONArray values02 = new JSONArray();
        values02.add(2);
        values02.add(15);
        assertEquals(new HashSet<>(Arrays.asList(values01, values02)), payloadEntries);
    }

    @Test
    public void structToJson() {
        Schema schema = SchemaBuilder.struct()
                .field("field1", SchemaBuilder.bool().required().build())
                .field("field2", SchemaBuilder.string().required().build())
                .field("field3", SchemaBuilder.string().required().build())
                .field("field4", SchemaBuilder.bool().required().build())
                .required()
                .build();
        Struct input = new Struct(schema).put("field1", true).put("field2", "string2").put("field3", "string3").put("field4", false);
        JSONObject converted = (JSONObject) parseObject(converter.fromConnectData(TOPIC, schema, input));
        JSONObject object = new JSONObject();
        object.put("field1", true);
        object.put("field2", "string2");
        object.put("field3", "string3");
        object.put("field4", false);
        assertEquals(
                object,
                converted.get(JsonSchema.ENVELOPE_PAYLOAD_FIELD_NAME)
        );
    }

    @Test
    public void structSchemaIdentical() {
        Schema schema = SchemaBuilder.struct()
                .field("field1", SchemaBuilder.bool().build())
                .field("field2", SchemaBuilder.string().build())
                .field("field3", SchemaBuilder.string().build())
                .field("field4", SchemaBuilder.bool().build())
                .required()
                .build();
        Schema inputSchema = SchemaBuilder.struct()
                .field("field1", SchemaBuilder.bool().build())
                .field("field2", SchemaBuilder.string().build())
                .field("field3", SchemaBuilder.string().build())
                .field("field4", SchemaBuilder.bool().build())
                .required()
                .build();
        Struct input = new Struct(inputSchema).put("field1", true).put("field2", "string2").put("field3", "string3").put("field4", false);
        assertStructSchemaEqual(schema, input);
    }


    @Test
    public void decimalToJson() {
        JSONObject converted = parse(converter.fromConnectData(TOPIC, Decimal.schema(2), new BigDecimal(new BigInteger("156"), 2)));
        assertEquals(parse("{ \"type\": \"bytes\", \"optional\": true, \"name\": \"io.openmessaging.connector.api.data.logical.Decimal\", \"version\": 1, \"parameters\": { \"scale\": \"2\" } }"),
                converted.get(JsonSchema.ENVELOPE_SCHEMA_FIELD_NAME));
        byte[] bytes = TypeUtils.castToBytes(converted.get(JsonSchema.ENVELOPE_PAYLOAD_FIELD_NAME).toString());
        assertArrayEquals(new byte[]{0, -100}, bytes);
    }

    @Test
    public void decimalToNumericJson() {
        converter.configure(Collections.singletonMap(JsonConverterConfig.DECIMAL_FORMAT_CONFIG, DecimalFormat.NUMERIC.name()));
        JSONObject converted = parse(converter.fromConnectData(TOPIC, Decimal.schema(2), new BigDecimal(new BigInteger("156"), 2)));
        assertEquals(parse("{ \"type\": \"bytes\", \"optional\": true, \"name\": \"io.openmessaging.connector.api.data.logical.Decimal\", \"version\": 1, \"parameters\": { \"scale\": \"2\" } }"),
                converted.get(JsonSchema.ENVELOPE_SCHEMA_FIELD_NAME));
        assertEquals(new BigDecimal("1.56"), converted.get(JsonSchema.ENVELOPE_PAYLOAD_FIELD_NAME));
    }


    @Test
    public void decimalToJsonWithoutSchema() {
        assertThrows(
                "expected data exception when serializing BigDecimal without schema",
                ConnectException.class,
                () -> converter.fromConnectData(TOPIC, null, new BigDecimal(new BigInteger("156"), 2)));
    }

    @Test
    public void dateToJson() {
        GregorianCalendar calendar = new GregorianCalendar(1970, Calendar.JANUARY, 1, 0, 0, 0);
        calendar.setTimeZone(TimeZone.getTimeZone("UTC"));
        calendar.add(Calendar.DATE, 10000);
        java.util.Date date = calendar.getTime();

        JSONObject converted = parse(converter.fromConnectData(TOPIC, Date.SCHEMA, date));
        assertEquals(parse("{ \"type\": \"int32\", \"optional\": true, \"name\": \"io.openmessaging.connector.api.data.logical.Date\", \"version\": 1 }"),
                converted.get(JsonSchema.ENVELOPE_SCHEMA_FIELD_NAME));
        Object payload = converted.get(JsonSchema.ENVELOPE_PAYLOAD_FIELD_NAME);
        assertEquals(10000, payload);
    }

    @Test
    public void timeToJson() {
        GregorianCalendar calendar = new GregorianCalendar(1970, Calendar.JANUARY, 1, 0, 0, 0);
        calendar.setTimeZone(TimeZone.getTimeZone("UTC"));
        calendar.add(Calendar.MILLISECOND, 14400000);
        java.util.Date date = calendar.getTime();

        JSONObject converted = parse(converter.fromConnectData(TOPIC, Time.SCHEMA, date));
        assertEquals(parse("{ \"type\": \"int32\", \"optional\": true, \"name\": \"io.openmessaging.connector.api.data.logical.Time\", \"version\": 1 }"),
                converted.get(JsonSchema.ENVELOPE_SCHEMA_FIELD_NAME));
        Object payload = converted.get(JsonSchema.ENVELOPE_PAYLOAD_FIELD_NAME);
        assertEquals(14400000, payload);
    }

    @Test
    public void timestampToJson() {
        GregorianCalendar calendar = new GregorianCalendar(1970, Calendar.JANUARY, 1, 0, 0, 0);
        calendar.setTimeZone(TimeZone.getTimeZone("UTC"));
        calendar.add(Calendar.MILLISECOND, 2000000000);
        calendar.add(Calendar.MILLISECOND, 2000000000);
        java.util.Date date = calendar.getTime();

        JSONObject converted = parse(converter.fromConnectData(TOPIC, Timestamp.SCHEMA, date));
        assertEquals(parse("{ \"type\": \"int64\", \"optional\": true, \"name\": \"io.openmessaging.connector.api.data.logical.Timestamp\", \"version\": 1 }"),
                converted.get(JsonSchema.ENVELOPE_SCHEMA_FIELD_NAME));
        Object payload = converted.get(JsonSchema.ENVELOPE_PAYLOAD_FIELD_NAME);
        assertEquals(4000000000L, payload);
    }


    @Test
    public void nullSchemaAndPrimitiveToJson() {
        // This still needs to do conversion of data, null schema means "anything goes"
        JSONObject converted = parse(converter.fromConnectData(TOPIC, null, true));
        assertEquals(true, converted.get(JsonSchema.ENVELOPE_PAYLOAD_FIELD_NAME));
    }

    @Test
    public void nullSchemaAndArrayToJson() {
        // This still needs to do conversion of data, null schema means "anything goes". Make sure we mix and match
        // types to verify conversion still works.
        JSONObject converted = parse(converter.fromConnectData(TOPIC, null, Arrays.asList(1, "string", true)));
        JSONArray array = new JSONArray();
        array.add(1);
        array.add("string");
        array.add(true);
        assertEquals(
                array,
                converted.get(JsonSchema.ENVELOPE_PAYLOAD_FIELD_NAME)
        );
    }

    @Test
    public void nullSchemaAndMapToJson() {
        // This still needs to do conversion of data, null schema means "anything goes". Make sure we mix and match
        // types to verify conversion still works.
        Map<String, Object> input = new HashMap<>();
        input.put("key1", 12);
        input.put("key2", "string");
        input.put("key3", true);
        JSONObject converted = parse(converter.fromConnectData(TOPIC, null, input));
        assertEquals("{\"key1\":12,\"key2\":\"string\",\"key3\":true}",
                converted.get(JsonSchema.ENVELOPE_PAYLOAD_FIELD_NAME).toString());
    }

    @Test
    public void nullSchemaAndMapNonStringKeysToJson() {
        // This still needs to do conversion of data, null schema means "anything goes". Make sure we mix and match
        // types to verify conversion still works.
        Map<Object, Object> input = new HashMap<>();
        input.put("string", 12);
        input.put(52, "string");
        input.put(false, true);
        JSONObject converted = parse(converter.fromConnectData(TOPIC, null, input));
        JSONArray payload = (JSONArray) converted.get(JsonSchema.ENVELOPE_PAYLOAD_FIELD_NAME);
        assertEquals(3, payload.size());
        Set<JSONArray> payloadEntries = new HashSet<>();
        for (Object elem : payload.toArray())
            payloadEntries.add((JSONArray) elem);
    }

    @Test
    public void nullSchemaAndNullValueToJson() {
        // This characterizes the production of tombstone messages when Json schemas is enabled
        Map<String, Boolean> props = Collections.singletonMap("schemas.enable", true);
        converter.configure(props);
        byte[] converted = converter.fromConnectData(TOPIC, null, null);
        assertNull(converted);
    }

    @Test
    public void nullValueToJson() {
        // This characterizes the production of tombstone messages when Json schemas is not enabled
        Map<String, Boolean> props = Collections.singletonMap("schemas.enable", false);
        converter.configure(props);
        byte[] converted = converter.fromConnectData(TOPIC, null, null);
        assertNull(converted);
    }

    @Test
    public void mismatchSchemaJson() {
        // If we have mismatching schema info, we should properly convert to a DataException
        converter.fromConnectData(TOPIC, SchemaBuilder.float64().build(), true);
    }

    @Test
    public void noSchemaToConnect() {
        Map<String, Boolean> props = Collections.singletonMap("schemas.enable", false);
        converter.configure(props);
        assertEquals(new SchemaAndValue(null, true), converter.toConnectData(TOPIC, "true".getBytes()));
    }

    @Test
    public void noSchemaToJson() {
        Map<String, Boolean> props = Collections.singletonMap("schemas.enable", false);
        converter.configure(props);
        byte[] result = converter.fromConnectData(TOPIC, null, true);
        Object converted = parseObject(result);
        assertEquals(true, converted);
    }

    private JSONObject parse(byte[] json) {
        try {
            String objStr = new String(json, StandardCharsets.UTF_8);
            return JSON.parseObject(objStr);
        } catch (Exception e) {
            fail("IOException during JSON parse: " + e.getMessage());
            throw new RuntimeException("failed");
        }
    }

    private Object parseObject(byte[] json) {
        try {
            String objStr = new String(json, StandardCharsets.UTF_8);
            Object data = JSON.parse(objStr);
            if (data instanceof JSONObject) {
                return (JSONObject) data;
            }
            return data;
        } catch (Exception e) {
            fail("IOException during JSON parse: " + e.getMessage());
            throw new RuntimeException("failed");
        }
    }


    private JSONObject parse(String json) {
        try {
            return JSON.parseObject(json);
        } catch (Exception e) {
            fail("IOException during JSON parse: " + e.getMessage());
            throw new RuntimeException("failed");
        }
    }


    private void assertStructSchemaEqual(Schema schema, Struct struct) {
        converter.fromConnectData(TOPIC, schema, struct);
        assertEquals(schema, struct.getSchema());
    }

    @Test
    public void converterData(){
        JsonConverter jsonConverter = new JsonConverter();
        jsonConverter.configure(new HashMap<>());
        Map<Object,Object> data = Maps.newConcurrentMap();
        data.put("data","data" );
        data.put("data01","data02");
        byte[] serData = jsonConverter.fromConnectData("test", null,Arrays.asList("test", null, null));
        SchemaAndValue deData = jsonConverter.toConnectData("test", serData);
        List<Object> value = (List<Object>)deData.value();
    }
}
