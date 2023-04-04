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
package org.apache.rocketmq.connect.runtime.converter.record.json;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.util.TypeUtils;
import io.openmessaging.connector.api.data.Field;
import io.openmessaging.connector.api.data.FieldType;
import io.openmessaging.connector.api.data.RecordConverter;
import io.openmessaging.connector.api.data.Schema;
import io.openmessaging.connector.api.data.SchemaAndValue;
import io.openmessaging.connector.api.data.SchemaBuilder;
import io.openmessaging.connector.api.data.Struct;
import io.openmessaging.connector.api.data.logical.Date;
import io.openmessaging.connector.api.data.logical.Decimal;
import io.openmessaging.connector.api.data.logical.Time;
import io.openmessaging.connector.api.data.logical.Timestamp;
import io.openmessaging.connector.api.errors.ConnectException;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.connect.runtime.common.LoggerName;
import org.apache.rocketmq.connect.runtime.common.cache.Cache;
import org.apache.rocketmq.connect.runtime.common.cache.LRUCache;
import org.apache.rocketmq.connect.runtime.serialization.JsonDeserializer;
import org.apache.rocketmq.connect.runtime.serialization.JsonSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

/**
 * json converter for fastjson
 */
public class JsonConverter implements RecordConverter {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.ROCKETMQ_RUNTIME);

    private static final Map<FieldType, JsonToConnectTypeConverter> TO_CONNECT_CONVERTERS = new EnumMap<>(FieldType.class);

    {
        TO_CONNECT_CONVERTERS.put(FieldType.BOOLEAN, new JsonToConnectTypeConverter<Boolean>() {
            @Override
            public Boolean convert(Schema schema, Object value) {
                return Boolean.valueOf(value.toString());
            }
        });
        TO_CONNECT_CONVERTERS.put(FieldType.INT8, new JsonToConnectTypeConverter<Byte>() {
            @Override
            public Byte convert(Schema schema, Object value) {
                return Byte.valueOf(value.toString());
            }
        });
        TO_CONNECT_CONVERTERS.put(FieldType.INT16, new JsonToConnectTypeConverter<Short>() {
            @Override
            public Short convert(Schema schema, Object value) {
                return Short.valueOf(value.toString());
            }
        });
        TO_CONNECT_CONVERTERS.put(FieldType.INT32, new JsonToConnectTypeConverter<Integer>() {
            @Override
            public Integer convert(Schema schema, Object value) {
                return Integer.valueOf(value.toString());
            }
        });
        TO_CONNECT_CONVERTERS.put(FieldType.INT64, new JsonToConnectTypeConverter<Long>() {
            @Override
            public Long convert(Schema schema, Object value) {
                return Long.valueOf(value.toString());
            }
        });
        TO_CONNECT_CONVERTERS.put(FieldType.FLOAT32, new JsonToConnectTypeConverter<Float>() {
            @Override
            public Float convert(Schema schema, Object value) {
                return (Float) value;
            }
        });
        TO_CONNECT_CONVERTERS.put(FieldType.FLOAT64, new JsonToConnectTypeConverter<Double>() {
            @Override
            public Double convert(Schema schema, Object value) {
                if (value instanceof BigDecimal) {
                    return Double.parseDouble(value.toString());
                }
                return (Double) value;
            }
        });
        TO_CONNECT_CONVERTERS.put(FieldType.BYTES, new JsonToConnectTypeConverter<byte[]>() {
            @Override
            public byte[] convert(Schema schema, Object value) {
                if (value instanceof String) {
                    return TypeUtils.castToBytes(value.toString());
                }
                return (byte[]) value;
            }
        });
        TO_CONNECT_CONVERTERS.put(FieldType.STRING, new JsonToConnectTypeConverter<String>() {
            @Override
            public String convert(Schema schema, Object value) {
                return String.valueOf(value);
            }
        });
        TO_CONNECT_CONVERTERS.put(FieldType.ARRAY, new JsonToConnectTypeConverter<List<Object>>() {
            @Override
            public List<Object> convert(Schema schema, Object value) {
                Schema elemSchema = schema == null ? null : schema.getValueSchema();
                List<Object> values = (List<Object>) value;
                List<Object> result = new ArrayList<>();
                for (Object elem : values) {
                    result.add(convertToConnect(elemSchema, elem));
                }
                return result;
            }
        });
        TO_CONNECT_CONVERTERS.put(FieldType.MAP, new JsonToConnectTypeConverter<Map<Object, Object>>() {
            @Override
            public Map<Object, Object> convert(Schema schema, Object value) {
                Schema keySchema = schema == null ? null : schema.getKeySchema();
                Schema valueSchema = schema == null ? null : schema.getValueSchema();
                Map<Object, Object> result = new HashMap<>();
                if (schema == null || keySchema.getFieldType() == FieldType.STRING) {
                    Map<String, Object> fieldIt = (Map<String, Object>) value;
                    fieldIt.forEach((k, v) -> {
                        result.put(k, convertToConnect(valueSchema, v));
                    });
                } else {
                    if (!(value instanceof JSONArray)) {
                        throw new ConnectException("Maps with non-string fields should be encoded as JSON array of tuples, but found " + JSON.toJSONString(value));
                    }
                    JSONArray array = (JSONArray) value;
                    for (Object entry : array.stream().toArray()) {
                        if (!(value instanceof JSONArray)) {
                            throw new ConnectException("Found invalid map entry instead of array tuple: " + JSON.toJSONString(entry));
                        }
                        JSONArray entryArray = (JSONArray) entry;
                        if (entryArray.toArray().length != 2) {
                            throw new ConnectException("Found invalid map entry, expected length 2 but found :" + entryArray.toArray().length);
                        }
                        result.put(
                                convertToConnect(keySchema, entryArray.toArray()[0]),
                                convertToConnect(valueSchema, entryArray.toArray()[1]));
                    }
                }
                return result;
            }
        });
        TO_CONNECT_CONVERTERS.put(FieldType.STRUCT, new JsonToConnectTypeConverter() {
            @Override
            public Object convert(Schema schema, Object value) {
                Struct result = new Struct(schema);
                JSONObject obj = (JSONObject) value;
                for (Field field : schema.getFields()) {
                    result.put(field, convertToConnect(field.getSchema(), obj.get(field.getName())));
                }
                return result;
            }
        });
    }

    // names specified in the field
    private static final HashMap<String, LogicalTypeConverter> LOGICAL_CONVERTERS = new HashMap<>();

    static {
        LOGICAL_CONVERTERS.put(Decimal.LOGICAL_NAME, new LogicalTypeConverter() {
            @Override
            public Object toJson(final Schema schema, final Object value, final JsonConverterConfig config) {
                if (!(value instanceof BigDecimal)) {
                    throw new ConnectException("Invalid type for Decimal, expected BigDecimal but was " + value.getClass());
                }
                final BigDecimal decimal = (BigDecimal) value;
                switch (config.decimalFormat()) {
                    case NUMERIC:
                        return decimal;
                    case BASE64:
                        return Decimal.fromLogical(schema, decimal);
                    default:
                        throw new ConnectException("Unexpected " + JsonConverterConfig.DECIMAL_FORMAT_CONFIG + ": " + config.decimalFormat());
                }
            }

            @Override
            public Object toConnect(final Schema schema, final Object value) {
                if (value instanceof BigDecimal) {
                    return (BigDecimal) value;
                }
                if (value instanceof byte[]) {
                    try {
                        return Decimal.toLogical(schema, (byte[]) value);
                    } catch (Exception e) {
                        throw new ConnectException("Invalid bytes for Decimal field", e);
                    }
                }
                if (value instanceof String) {
                    try {
                        return Decimal.toLogical(schema, TypeUtils.castToBytes((String) value));
                    } catch (Exception e) {
                        throw new ConnectException("Invalid bytes for Decimal field", e);
                    }
                }
                throw new ConnectException("Invalid type for Decimal, underlying representation should be numeric or bytes but was " + value);
            }
        });

        LOGICAL_CONVERTERS.put(Date.LOGICAL_NAME, new LogicalTypeConverter() {
            @Override
            public Object toJson(final Schema schema, final Object value, final JsonConverterConfig config) {
                if (!(value instanceof java.util.Date)) {
                    throw new ConnectException("Invalid type for Date, expected Date but was " + value.getClass());
                }
                return Date.fromLogical(schema, (java.util.Date) value);
            }

            @Override
            public Object toConnect(final Schema schema, final Object value) {
                if (!(value instanceof Integer)) {
                    throw new ConnectException("Invalid type for Date, underlying representation should be integer but was " + value);
                }
                return Date.toLogical(schema, (Integer) value);
            }
        });

        LOGICAL_CONVERTERS.put(Time.LOGICAL_NAME, new LogicalTypeConverter() {
            @Override
            public Object toJson(final Schema schema, final Object value, final JsonConverterConfig config) {
                if (!(value instanceof java.util.Date)) {
                    throw new ConnectException("Invalid type for Time, expected Date but was " + value.getClass());
                }
                return Time.fromLogical(schema, (java.util.Date) value);
            }

            @Override
            public Object toConnect(final Schema schema, final Object value) {
                if (!(value instanceof Integer)) {
                    throw new ConnectException("Invalid type for Time, underlying representation should be integer but was " + value);
                }
                return Time.toLogical(schema, ((Integer) value).intValue());
            }
        });

        LOGICAL_CONVERTERS.put(Timestamp.LOGICAL_NAME, new LogicalTypeConverter() {
            @Override
            public Object toJson(final Schema schema, final Object value, final JsonConverterConfig config) {
                if (!(value instanceof java.util.Date)) {
                    throw new ConnectException("Invalid type for Timestamp, expected Date but was " + value.getClass());
                }
                return Timestamp.fromLogical(schema, (java.util.Date) value);
            }

            @Override
            public Object toConnect(final Schema schema, final Object value) {
                if (value instanceof Number) {
                    return Timestamp.toLogical(schema, Long.valueOf(value.toString()));
                }
                throw new ConnectException("Invalid type for Timestamp, underlying representation should be integral but was " + value);
            }
        });
    }

    private JsonDeserializer deserializer = new JsonDeserializer();
    private JsonSerializer serializer = new JsonSerializer();
    public JsonConverterConfig converterConfig;
    private Cache<Schema, JSONObject> fromConnectSchemaCache;
    private Cache<JSONObject, Schema> toConnectSchemaCache;

    /**
     * Configure this class.
     *
     * @param configs configs in key/value pairs
     */
    @Override
    public void configure(Map<String, ?> configs) {
        converterConfig = new JsonConverterConfig(configs);
        fromConnectSchemaCache = new LRUCache<>(converterConfig.cacheSize());
        toConnectSchemaCache = new LRUCache<>(converterConfig.cacheSize());
    }

    /**
     * Convert a rocketmq Connect data object to a native object for serialization.
     *
     * @param topic the topic associated with the data
     * @param schema the schema for the value
     * @param value the value to convert
     * @return the serialized value
     */
    @Override
    public byte[] fromConnectData(String topic, Schema schema, Object value) {
        if (schema == null && value == null) {
            return null;
        }
        Object jsonValue = converterConfig.schemasEnabled() ? convertToJsonWithEnvelope(schema, value) : convertToJsonWithoutEnvelope(schema, value);
        try {
            return serializer.serialize(topic, jsonValue);
        } catch (Exception e) {
            throw new ConnectException("Converting Kafka Connect data to byte[] failed due to serialization error: ", e);
        }
    }

    /**
     * Convert a native object to a Rocketmq Connect data object.
     *
     * @param topic the topic associated with the data
     * @param value the value to convert
     * @return an object containing the {@link Schema} and the converted value
     */
    @Override
    public SchemaAndValue toConnectData(String topic, byte[] value) {
        // This handles a tombstone message
        if (value == null) {
            return SchemaAndValue.NULL;
        }
        Object jsonValue;
        try {
            jsonValue = deserializer.deserialize(topic, value);
        } catch (Exception e) {
            throw new ConnectException("Converting byte[] to Kafka Connect data failed due to serialization error: ", e);
        }
        JSONObject newJsonValue;
        if (!converterConfig.schemasEnabled()) {
            // schema disabled
            JSONObject envelope = new JSONObject();
            envelope.put(JsonSchema.ENVELOPE_SCHEMA_FIELD_NAME, null);
            envelope.put(JsonSchema.ENVELOPE_PAYLOAD_FIELD_NAME, jsonValue);
            newJsonValue = envelope;
        } else {
            // schema enabled
            newJsonValue = (JSONObject) jsonValue;
        }
        Object jsonSchema = newJsonValue.get(JsonSchema.ENVELOPE_SCHEMA_FIELD_NAME);
        Schema schema = asConnectSchema(jsonSchema == null ? null : (JSONObject) jsonSchema);
        return new SchemaAndValue(
                schema,
                convertToConnect(schema, newJsonValue.get(JsonSchema.ENVELOPE_PAYLOAD_FIELD_NAME))
        );
    }

    /**
     * convert to json with envelope
     *
     * @param schema
     * @param value
     * @return
     */
    private JSONObject convertToJsonWithEnvelope(Schema schema, Object value) {
        return new JsonSchema.Envelope(
                asJsonSchema(schema),
                convertToJson(schema, value)
        ).toJsonNode();
    }

    /**
     * convert to json without envelop
     *
     * @param schema
     * @param value
     * @return
     */
    private Object convertToJsonWithoutEnvelope(Schema schema, Object value) {
        return convertToJson(schema, value);
    }

    private interface JsonToConnectTypeConverter<Output> {
        Output convert(Schema schema, Object value);
    }

    private interface LogicalTypeConverter {
        Object toJson(Schema schema, Object value, JsonConverterConfig config);

        Object toConnect(Schema schema, Object value);
    }

    /**
     * convert ConnectRecord schema to json schema
     *
     * @param schema
     * @return
     */
    public JSONObject asJsonSchema(Schema schema) {
        if (schema == null) {
            return null;
        }
        // from cached
        JSONObject cached = fromConnectSchemaCache.get(schema);
        if (cached != null) {
            return cached.clone();
        }

        JSONObject jsonSchema;
        // convert field type name
        switch (schema.getFieldType()) {
            case BOOLEAN:
                jsonSchema = JsonSchema.BOOLEAN_SCHEMA();
                break;
            case BYTES:
                jsonSchema = JsonSchema.BYTES_SCHEMA();
                break;
            case FLOAT64:
                jsonSchema = JsonSchema.DOUBLE_SCHEMA();
                break;
            case FLOAT32:
                jsonSchema = JsonSchema.FLOAT_SCHEMA();
                break;
            case INT8:
                jsonSchema = JsonSchema.INT8_SCHEMA();
                break;
            case INT16:
                jsonSchema = JsonSchema.INT16_SCHEMA();
                break;
            case INT32:
                jsonSchema = JsonSchema.INT32_SCHEMA();
                break;
            case INT64:
                jsonSchema = JsonSchema.INT64_SCHEMA();
                break;
            case STRING:
                jsonSchema = JsonSchema.STRING_SCHEMA();
                break;
            case ARRAY:
                jsonSchema = new JSONObject();
                jsonSchema.put(JsonSchema.SCHEMA_TYPE_FIELD_NAME, JsonSchema.ARRAY_TYPE_NAME);
                jsonSchema.put(JsonSchema.ARRAY_ITEMS_FIELD_NAME, asJsonSchema(schema.getValueSchema()));
                break;
            case MAP:
                jsonSchema = new JSONObject();
                jsonSchema.put(JsonSchema.SCHEMA_TYPE_FIELD_NAME, JsonSchema.MAP_TYPE_NAME);
                jsonSchema.put(JsonSchema.MAP_KEY_FIELD_NAME, asJsonSchema(schema.getKeySchema()));
                jsonSchema.put(JsonSchema.MAP_VALUE_FIELD_NAME, asJsonSchema(schema.getValueSchema()));
                break;
            case STRUCT:
                jsonSchema = new JSONObject(new ConcurrentHashMap<>());
                jsonSchema.put(JsonSchema.SCHEMA_TYPE_FIELD_NAME, JsonSchema.STRUCT_TYPE_NAME);
                // field list
                JSONArray fields = new JSONArray();
                for (Field field : schema.getFields()) {
                    String fieldName = field.getName();
                    JSONObject fieldJsonSchema = asJsonSchema(field.getSchema());
                    fieldJsonSchema.put(JsonSchema.STRUCT_FIELD_NAME_FIELD_NAME, fieldName);
                    fields.add(fieldJsonSchema);
                }
                jsonSchema.put(JsonSchema.STRUCT_FIELDS_FIELD_NAME, fields);
                break;
            default:
                throw new ConnectException("Couldn't translate unsupported schema type " + schema + ".");
        }

        // optional
        jsonSchema.put(JsonSchema.SCHEMA_OPTIONAL_FIELD_NAME, schema.isOptional());

        // name
        if (schema.getName() != null) {
            jsonSchema.put(JsonSchema.SCHEMA_NAME_FIELD_NAME, schema.getName());
        }
        // version
        if (schema.getVersion() != null) {
            jsonSchema.put(JsonSchema.SCHEMA_VERSION_FIELD_NAME, schema.getVersion());
        }

        // doc
        if (schema.getDoc() != null) {
            jsonSchema.put(JsonSchema.SCHEMA_DOC_FIELD_NAME, schema.getDoc());
        }

        // parameters
        if (schema.getParameters() != null) {
            JSONObject jsonSchemaParams = new JSONObject();
            for (Map.Entry<String, String> prop : schema.getParameters().entrySet()) {
                jsonSchemaParams.put(prop.getKey(), prop.getValue());
            }
            jsonSchema.put(JsonSchema.SCHEMA_PARAMETERS_FIELD_NAME, jsonSchemaParams);
        }

        // default value
        if (schema.getDefaultValue() != null) {
            jsonSchema.put(JsonSchema.SCHEMA_DEFAULT_FIELD_NAME, convertToJson(schema, schema.getDefaultValue()));
        }
        // add cache
        fromConnectSchemaCache.put(schema, jsonSchema);
        return jsonSchema;
    }

    /**
     * Convert this object, in the org.apache.kafka.connect.data format, into a JSON object, returning both the schema
     * and the converted object.
     */
    private Object convertToJson(Schema schema, Object value) {
        if (value == null) {
            if (schema == null) {
                return null;
            }
            if (schema.getDefaultValue() != null) {
                return convertToJson(schema, schema.getDefaultValue());
            }
            if (schema.isOptional()) {
                return null;
            }
            throw new ConnectException("Conversion error: null value for field that is required and has no default value");
        }

        if (schema != null && schema.getName() != null) {
            LogicalTypeConverter logicalConverter = LOGICAL_CONVERTERS.get(schema.getName());
            if (logicalConverter != null) {
                if (value == null) {
                    return null;
                } else {
                    return logicalConverter.toJson(schema, value, converterConfig);
                }
            }
        }

        try {
            final FieldType schemaType;
            if (schema == null) {
                schemaType = Schema.schemaType(value.getClass());
                if (schemaType == null) {
                    throw new ConnectException("Java class " + value.getClass() + " does not have corresponding schema type.");
                }
            } else {
                schemaType = schema.getFieldType();
            }

            switch (schemaType) {
                case INT8:
                case INT16:
                case INT32:
                case INT64:
                case FLOAT32:
                case FLOAT64:
                case BOOLEAN:
                case STRING:
                    return value;
                case BYTES:
                    if (value instanceof byte[]) {
                        return (byte[]) value;
                    } else if (value instanceof ByteBuffer) {
                        return ((ByteBuffer) value).array();
                    } else {
                        throw new ConnectException("Invalid type for bytes type: " + value.getClass());
                    }
                case ARRAY: {
                    Collection collection = (Collection) value;
                    List list = new ArrayList();
                    for (Object elem : collection) {
                        Schema valueSchema = schema == null ? null : schema.getValueSchema();
                        Object fieldValue = convertToJson(valueSchema, elem);
                        list.add(fieldValue);
                    }
                    return list;
                }
                case MAP: {
                    Map<?, ?> map = (Map<?, ?>) value;
                    boolean objectMode;
                    if (schema == null) {
                        objectMode = true;
                        for (Map.Entry<?, ?> entry : map.entrySet()) {
                            if (!(entry.getKey() instanceof String)) {
                                objectMode = false;
                                break;
                            }
                        }
                    } else {
                        objectMode = schema.getKeySchema().getFieldType() == FieldType.STRING;
                    }

                    JSONArray resultArray = new JSONArray();
                    Map<String, Object> resultMap = new HashMap<>();
                    for (Map.Entry<?, ?> entry : map.entrySet()) {
                        Schema keySchema = schema == null ? null : schema.getKeySchema();
                        Schema valueSchema = schema == null ? null : schema.getValueSchema();
                        Object mapKey = convertToJson(keySchema, entry.getKey());
                        Object mapValue = convertToJson(valueSchema, entry.getValue());
                        if (objectMode) {
                            resultMap.put((String) mapKey, mapValue);
                        } else {
                            JSONArray entryArray = new JSONArray();
                            entryArray.add(0, mapKey);
                            entryArray.add(1, mapValue);
                            resultArray.add(entryArray);
                        }
                    }
                    return objectMode ? resultMap : resultArray;
                }
                case STRUCT: {
                    Struct struct = (Struct) value;
                    if (!struct.schema().equals(schema)) {
                        throw new ConnectException("Mismatching schema.");
                    }
                    JSONObject obj = new JSONObject(new LinkedHashMap());
                    for (Field field : struct.schema().getFields()) {
                        obj.put(field.getName(), convertToJson(field.getSchema(), struct.get(field)));
                    }
                    return obj;
                }
            }
            throw new ConnectException("Couldn't convert " + value + " to JSON.");
        } catch (ClassCastException e) {
            String schemaTypeStr = (schema != null) ? schema.getFieldType().toString() : "unknown schema";
            throw new ConnectException("Invalid type for " + schemaTypeStr + ": " + value.getClass());
        }
    }

    /**
     * convert json to schema if not empty
     *
     * @param jsonSchema
     * @return
     */
    public Schema asConnectSchema(JSONObject jsonSchema) {
        // schema null
        if (jsonSchema == null) {
            return null;
        }
        Schema cached = toConnectSchemaCache.get(jsonSchema);
        if (cached != null) {
            return cached;
        }

        String schemaType = String.valueOf(jsonSchema.get(JsonSchema.SCHEMA_TYPE_FIELD_NAME));
        if (StringUtils.isEmpty(schemaType)) {
            throw new ConnectException("Schema must contain 'type' field");
        }
        final SchemaBuilder builder;
        switch (schemaType) {
            case JsonSchema.BOOLEAN_TYPE_NAME:
                builder = SchemaBuilder.bool();
                break;
            case JsonSchema.INT8_TYPE_NAME:
                builder = SchemaBuilder.int8();
                break;
            case JsonSchema.INT16_TYPE_NAME:
                builder = SchemaBuilder.int16();
                break;
            case JsonSchema.INT32_TYPE_NAME:
                builder = SchemaBuilder.int32();
                break;
            case JsonSchema.INT64_TYPE_NAME:
                builder = SchemaBuilder.int64();
                break;
            case JsonSchema.FLOAT_TYPE_NAME:
                builder = SchemaBuilder.float32();
                break;
            case JsonSchema.DOUBLE_TYPE_NAME:
                builder = SchemaBuilder.float64();
                break;
            case JsonSchema.BYTES_TYPE_NAME:
                builder = SchemaBuilder.bytes();
                break;
            case JsonSchema.STRING_TYPE_NAME:
                builder = SchemaBuilder.string();
                break;
            case JsonSchema.ARRAY_TYPE_NAME:
                JSONObject elemSchema = (JSONObject) jsonSchema.get(JsonSchema.ARRAY_ITEMS_FIELD_NAME);
                if (Objects.isNull(elemSchema)) {
                    throw new ConnectException("Array schema did not specify the element type");
                }
                builder = SchemaBuilder.array(asConnectSchema(elemSchema));
                break;
            case JsonSchema.MAP_TYPE_NAME:
                JSONObject keySchema = (JSONObject) jsonSchema.get(JsonSchema.MAP_KEY_FIELD_NAME);
                if (keySchema == null) {
                    throw new ConnectException("Map schema did not specify the key type");
                }
                JSONObject valueSchema = (JSONObject) jsonSchema.get(JsonSchema.MAP_VALUE_FIELD_NAME);
                if (valueSchema == null) {
                    throw new ConnectException("Map schema did not specify the value type");
                }
                builder = SchemaBuilder.map(asConnectSchema(keySchema), asConnectSchema(valueSchema));
                break;
            case JsonSchema.STRUCT_TYPE_NAME:
                builder = SchemaBuilder.struct();
                List<JSONObject> fields = (List<JSONObject>) jsonSchema.get(JsonSchema.STRUCT_FIELDS_FIELD_NAME);
                if (Objects.isNull(fields)) {
                    throw new ConnectException("Struct schema's \"fields\" argument is not an array.");
                }
                for (JSONObject field : fields) {
                    String jsonFieldName = field.getString(JsonSchema.STRUCT_FIELD_NAME_FIELD_NAME);
                    if (jsonFieldName == null) {
                        throw new ConnectException("Struct schema's field name not specified properly");
                    }
                    builder.field(jsonFieldName, asConnectSchema(field));
                }
                break;
            default:
                throw new ConnectException("Unknown schema type: " + schemaType);
        }

        // optional
        Boolean isOptional = jsonSchema.getBoolean(JsonSchema.SCHEMA_OPTIONAL_FIELD_NAME);
        if (isOptional != null && isOptional) {
            builder.optional();
        }

        // schema name
        String schemaName = jsonSchema.getString(JsonSchema.SCHEMA_NAME_FIELD_NAME);
        builder.name(schemaName);

        // schema version
        Object version = jsonSchema.get(JsonSchema.SCHEMA_VERSION_FIELD_NAME);
        if (version != null && version instanceof Integer) {
            builder.version(Integer.parseInt(version.toString()));
        }

        // schema doc
        String doc = jsonSchema.getString(JsonSchema.SCHEMA_DOC_FIELD_NAME);
        if (StringUtils.isNotEmpty(doc)) {
            builder.doc(doc);
        }

        // schema parameter
        JSONObject schemaParams = (JSONObject) jsonSchema.get(JsonSchema.SCHEMA_PARAMETERS_FIELD_NAME);
        if (schemaParams != null) {
            Map<String, Object> paramsIt = schemaParams.getInnerMap();
            paramsIt.forEach((k, v) -> {
                builder.parameter(k, String.valueOf(v));
            });
        }

        Object schemaDefaultNode = jsonSchema.get(JsonSchema.SCHEMA_DEFAULT_FIELD_NAME);
        if (schemaDefaultNode != null) {
            builder.defaultValue(convertToConnect(builder.build(), schemaDefaultNode));
        }
        Schema result = builder.build();
        toConnectSchemaCache.put(jsonSchema, result);
        return result;
    }

    /**
     * convert to connect
     *
     * @param schema
     * @param value
     * @return
     */
    private Object convertToConnect(Schema schema, Object value) {
        final FieldType schemaType;
        if (schema != null) {
            schemaType = schema.getFieldType();
            if (value == null) {
                if (schema.getDefaultValue() != null) {
                    return schema.getDefaultValue(); // any logical type conversions should already have been applied
                }
                if (schema.isOptional()) {
                    return null;
                }
                throw new ConnectException("Invalid null value for required " + schemaType + " field");
            }
        } else {
            if (value == null) {
                return null;
            } else if (value instanceof String) {
                schemaType = FieldType.STRING;
            } else if (value instanceof Integer) {
                schemaType = FieldType.INT32;
            } else if (value instanceof Long) {
                schemaType = FieldType.INT64;
            } else if (value instanceof Float) {
                schemaType = FieldType.FLOAT32;
            } else if (value instanceof Double) {
                schemaType = FieldType.FLOAT64;
            } else if (value instanceof BigDecimal) {
                schemaType = FieldType.FLOAT64;
            } else if (value instanceof Boolean) {
                schemaType = FieldType.BOOLEAN;
            } else if (value instanceof List) {
                schemaType = FieldType.ARRAY;
            } else if (value instanceof Map) {
                schemaType = FieldType.MAP;
            } else {
                schemaType = null;
            }
        }

        final JsonToConnectTypeConverter typeConverter = TO_CONNECT_CONVERTERS.get(schemaType);
        if (typeConverter == null) {
            throw new ConnectException("Unknown schema type: " + schemaType);
        }

        if (schema != null && schema.getName() != null) {
            LogicalTypeConverter logicalConverter = LOGICAL_CONVERTERS.get(schema.getName());
            if (logicalConverter != null) {
                return logicalConverter.toConnect(schema, value);
            }
        }

        return typeConverter.convert(schema, value);
    }

}