/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.rocketmq.schema.avro.util;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericContainer;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.io.JsonEncoder;
import org.apache.avro.reflect.ReflectData;
import org.apache.rocketmq.schema.avro.AvroSchema;
import org.apache.rocketmq.schema.registry.client.exceptions.SerializationException;
import org.apache.rocketmq.schema.registry.client.rest.JacksonMapper;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class AvroSchemaUtils {

    private static final EncoderFactory ENCODER_FACTORY = EncoderFactory.get();
    private static final DecoderFactory DECODER_FACTORY = DecoderFactory.get();
    private static final ObjectMapper JSON_MAPPER = JacksonMapper.INSTANCE;

    private static final Map<String, Schema> PRIMITIVE_SCHEMAS;

    static {
        PRIMITIVE_SCHEMAS = new HashMap<>();
        PRIMITIVE_SCHEMAS.put("Null", createPrimitiveSchema("null"));
        PRIMITIVE_SCHEMAS.put("Boolean", createPrimitiveSchema("boolean"));
        PRIMITIVE_SCHEMAS.put("Integer", createPrimitiveSchema("int"));
        PRIMITIVE_SCHEMAS.put("Long", createPrimitiveSchema("long"));
        PRIMITIVE_SCHEMAS.put("Float", createPrimitiveSchema("float"));
        PRIMITIVE_SCHEMAS.put("Double", createPrimitiveSchema("double"));
        PRIMITIVE_SCHEMAS.put("String", createPrimitiveSchema("string"));
        PRIMITIVE_SCHEMAS.put("Bytes", createPrimitiveSchema("bytes"));
    }

    private static Schema createPrimitiveSchema(String type) {
        String schemaString = String.format("{\"type\" : \"%s\"}", type);
        return new AvroSchema(schemaString).rawSchema();
    }

    public static AvroSchema copyOf(AvroSchema schema) {
        return schema.copy();
    }

    public static Map<String, Schema> getPrimitiveSchemas() {
        return Collections.unmodifiableMap(PRIMITIVE_SCHEMAS);
    }

    public static Schema getSchema(Object object) {
        return getSchema(object, false, false, false);
    }

    public static Schema getSchema(Object object, boolean useReflection,
                                   boolean reflectionAllowNull, boolean removeJavaProperties) {
        if (object == null) {
            return PRIMITIVE_SCHEMAS.get("Null");
        } else if (object instanceof Boolean) {
            return PRIMITIVE_SCHEMAS.get("Boolean");
        } else if (object instanceof Integer) {
            return PRIMITIVE_SCHEMAS.get("Integer");
        } else if (object instanceof Long) {
            return PRIMITIVE_SCHEMAS.get("Long");
        } else if (object instanceof Float) {
            return PRIMITIVE_SCHEMAS.get("Float");
        } else if (object instanceof Double) {
            return PRIMITIVE_SCHEMAS.get("Double");
        } else if (object instanceof CharSequence) {
            return PRIMITIVE_SCHEMAS.get("String");
        } else if (object instanceof byte[] || object instanceof ByteBuffer) {
            return PRIMITIVE_SCHEMAS.get("Bytes");
        } else if (useReflection) {
            Schema schema = reflectionAllowNull ? ReflectData.AllowNull.get().getSchema(object.getClass())
                    : ReflectData.get().getSchema(object.getClass());
            if (schema == null) {
                throw new SerializationException("Schema is null for object of class " + object.getClass()
                        .getCanonicalName());
            } else {
                return schema;
            }
        } else if (object instanceof GenericContainer) {
            Schema schema = ((GenericContainer) object).getSchema();
            if (removeJavaProperties) {
                schema = removeJavaProperties(schema);
            }
            return schema;
        } else if (object instanceof Map) {
            // This case is unusual -- the schema isn't available directly anywhere, instead we have to
            // take get the value schema out of one of the entries and then construct the full schema.
            Map mapValue = (Map) object;
            if (mapValue.isEmpty()) {
                // In this case the value schema doesn't matter since there is no content anyway. This
                // only works because we know in this case that we are only using this for conversion and
                // no data will be added to the map.
                return Schema.createMap(PRIMITIVE_SCHEMAS.get("Null"));
            }
            Schema valueSchema = getSchema(mapValue.values().iterator().next());
            return Schema.createMap(valueSchema);
        } else {
            throw new IllegalArgumentException(
                    "Unsupported Avro type. Supported types are null, Boolean, Integer, Long, "
                            + "Float, Double, String, byte[] and IndexedRecord");
        }
    }

    private static Schema removeJavaProperties(Schema schema) {
        try {
            JsonNode node = JSON_MAPPER.readTree(schema.toString());
            removeProperty(node, "avro.java.string");
            AvroSchema avroSchema = new AvroSchema(node.toString());
            return avroSchema.rawSchema();
        } catch (IOException e) {
            throw new SerializationException("Could not parse schema: " + schema);
        }
    }

    private static void removeProperty(JsonNode node, String propertyName) {
        if (node.isObject()) {
            ObjectNode objectNode = (ObjectNode) node;
            objectNode.remove(propertyName);
            Iterator<JsonNode> elements = objectNode.elements();
            while (elements.hasNext()) {
                removeProperty(elements.next(), propertyName);
            }
        } else if (node.isArray()) {
            ArrayNode arrayNode = (ArrayNode) node;
            Iterator<JsonNode> elements = arrayNode.elements();
            while (elements.hasNext()) {
                removeProperty(elements.next(), propertyName);
            }
        }
    }

    public static Object toObject(JsonNode value, AvroSchema schema) throws IOException {
        try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            Schema rawSchema = schema.rawSchema();
            JSON_MAPPER.writeValue(out, value);
            DatumReader<Object> reader = new GenericDatumReader<Object>(rawSchema);
            Object object = reader.read(null,
                    DECODER_FACTORY.jsonDecoder(rawSchema, new ByteArrayInputStream(out.toByteArray()))
            );
            return object;
        }
    }

    public static Object toObject(String value, AvroSchema schema) throws IOException {
        Schema rawSchema = schema.rawSchema();
        DatumReader<Object> reader = new GenericDatumReader<Object>(rawSchema);
        Object object = reader.read(null,
                DECODER_FACTORY.jsonDecoder(rawSchema, value));
        return object;
    }

    public static byte[] toJson(Object value) throws IOException {
        if (value == null) {
            return null;
        }
        try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            toJson(value, out);
            return out.toByteArray();
        }
    }

    public static void toJson(Object value, OutputStream out) throws IOException {
        Schema schema = getSchema(value);
        JsonEncoder encoder = ENCODER_FACTORY.jsonEncoder(schema, out);
        DatumWriter<Object> writer = new GenericDatumWriter<>(schema);
        // Some types require wrapping/conversion
        Object wrappedValue = value;
        if (value instanceof byte[]) {
            wrappedValue = ByteBuffer.wrap((byte[]) value);
        }
        writer.write(wrappedValue, encoder);
        encoder.flush();
    }
}
