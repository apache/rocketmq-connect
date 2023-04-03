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
package org.apache.rocketmq.schema.json;


import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.openmessaging.connector.api.data.Field;
import io.openmessaging.connector.api.data.FieldType;
import io.openmessaging.connector.api.data.Schema;
import io.openmessaging.connector.api.data.SchemaBuilder;
import io.openmessaging.connector.api.data.Struct;
import io.openmessaging.connector.api.data.logical.Date;
import io.openmessaging.connector.api.data.logical.Decimal;
import io.openmessaging.connector.api.data.logical.Time;
import io.openmessaging.connector.api.data.logical.Timestamp;
import io.openmessaging.connector.api.errors.ConnectException;
import org.apache.rocketmq.schema.registry.client.rest.JacksonMapper;
import org.everit.json.schema.ArraySchema;
import org.everit.json.schema.BooleanSchema;
import org.everit.json.schema.CombinedSchema;
import org.everit.json.schema.ConstSchema;
import org.everit.json.schema.EnumSchema;
import org.everit.json.schema.NullSchema;
import org.everit.json.schema.NumberSchema;
import org.everit.json.schema.ObjectSchema;
import org.everit.json.schema.ReferenceSchema;
import org.everit.json.schema.StringSchema;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * json schema generate
 */
public class JsonSchemaData {

    public static final String NAMESPACE = "org.apache.rocketmq.connect.json";
    public static final String KEY_FIELD = "key";
    public static final String VALUE_FIELD = "value";

    public static final String CONNECT_TYPE_PROP = "connect.type";
    public static final String CONNECT_VERSION_PROP = "connect.version";
    public static final String CONNECT_PARAMETERS_PROP = "connect.parameters";
    public static final String CONNECT_INDEX_PROP = "connect.index";


    public static final String CONNECT_TYPE_INT8 = "int8";
    public static final String CONNECT_TYPE_INT16 = "int16";
    public static final String CONNECT_TYPE_INT32 = "int32";
    public static final String CONNECT_TYPE_INT64 = "int64";
    public static final String CONNECT_TYPE_FLOAT32 = "float32";
    public static final String CONNECT_TYPE_FLOAT64 = "float64";
    public static final String CONNECT_TYPE_BYTES = "bytes";
    public static final String CONNECT_TYPE_MAP = "map";

    public static final String JSON_TYPE_ENUM = NAMESPACE + ".Enum";
    public static final String JSON_TYPE_ENUM_PREFIX = JSON_TYPE_ENUM + ".";
    public static final String JSON_TYPE_ONE_OF = NAMESPACE + ".OneOf";

    private static final JsonNodeFactory JSON_NODE_FACTORY = JsonNodeFactory.withExactBigDecimals(true);
    private static final ObjectMapper OBJECT_MAPPER = JacksonMapper.INSTANCE;
    private static final Map<FieldType, JsonToConnectTypeConverter> TO_CONNECT_CONVERTERS = new ConcurrentHashMap<>();
    private static final HashMap<String, JsonToConnectLogicalTypeConverter>
            TO_CONNECT_LOGICAL_CONVERTERS = new HashMap<>();
    private static final HashMap<String, ConnectToJsonLogicalTypeConverter>
            TO_JSON_LOGICAL_CONVERTERS = new HashMap<>();

    static {
        TO_CONNECT_CONVERTERS.put(FieldType.BOOLEAN, (schema, value) -> value.booleanValue());
        TO_CONNECT_CONVERTERS.put(FieldType.INT8, (schema, value) -> (byte) value.shortValue());
        TO_CONNECT_CONVERTERS.put(FieldType.INT16, (schema, value) -> value.shortValue());
        TO_CONNECT_CONVERTERS.put(FieldType.INT32, (schema, value) -> value.intValue());
        TO_CONNECT_CONVERTERS.put(FieldType.INT64, (schema, value) -> value.longValue());
        TO_CONNECT_CONVERTERS.put(FieldType.FLOAT32, (schema, value) -> value.floatValue());
        TO_CONNECT_CONVERTERS.put(FieldType.FLOAT64, (schema, value) -> value.doubleValue());
        TO_CONNECT_CONVERTERS.put(FieldType.BYTES, (schema, value) -> {
            try {
                Object o = value.binaryValue();
                if (o == null) {
                    o = value.decimalValue();  // decimal logical type
                }
                return o;
            } catch (IOException e) {
                throw new ConnectException("Invalid bytes field", e);
            }
        });
        TO_CONNECT_CONVERTERS.put(FieldType.STRING, (schema, value) -> value.textValue());
        TO_CONNECT_CONVERTERS.put(FieldType.ARRAY, (schema, value) -> {
            Schema elemSchema = schema == null ? null : schema.getValueSchema();
            ArrayList<Object> result = new ArrayList<>();
            for (JsonNode elem : value) {
                result.add(toConnectData(elemSchema, elem));
            }
            return result;
        });
        TO_CONNECT_CONVERTERS.put(FieldType.MAP, (schema, value) -> {
            Schema keySchema = schema == null ? null : schema.getKeySchema();
            Schema valueSchema = schema == null ? null : schema.getValueSchema();
            Map<Object, Object> result = new HashMap<>();
            if (schema == null || (keySchema.getFieldType() == FieldType.STRING && !keySchema.isOptional())) {
                if (!value.isObject()) {
                    throw new ConnectException(
                            "Maps with string fields should be encoded as JSON objects, but found "
                                    + value.getNodeType());
                }
                Iterator<Map.Entry<String, JsonNode>> fieldIt = value.fields();
                while (fieldIt.hasNext()) {
                    Map.Entry<String, JsonNode> entry = fieldIt.next();
                    result.put(entry.getKey(), toConnectData(valueSchema, entry.getValue()));
                }
            } else {
                if (!value.isArray()) {
                    throw new ConnectException(
                            "Maps with non-string fields should be encoded as JSON array of objects, but "
                                    + "found "
                                    + value.getNodeType());
                }
                for (JsonNode entry : value) {
                    if (!entry.isObject()) {
                        throw new ConnectException("Found invalid map entry instead of object: "
                                + entry.getNodeType());
                    }
                    if (entry.size() != 2) {
                        throw new ConnectException("Found invalid map entry, expected length 2 but found :" + entry
                                .size());
                    }
                    result.put(toConnectData(keySchema, entry.get(KEY_FIELD)),
                            toConnectData(valueSchema, entry.get(VALUE_FIELD))
                    );
                }
            }
            return result;
        });
        TO_CONNECT_CONVERTERS.put(FieldType.STRUCT, (schema, value) -> {
            if (schema.getName() != null && schema.getName().equals(JSON_TYPE_ONE_OF)) {
                int numMatchingProperties = -1;
                Field matchingField = null;
                for (Field field : schema.getFields()) {
                    Schema fieldSchema = field.getSchema();

                    if (isSimpleSchema(fieldSchema, value)) {
                        return new Struct(schema).put(JSON_TYPE_ONE_OF + ".field." + field.getIndex(),
                                toConnectData(fieldSchema, value)
                        );
                    } else {
                        int matching = matchStructSchema(fieldSchema, value);
                        if (matching > numMatchingProperties) {
                            numMatchingProperties = matching;
                            matchingField = field;
                        }
                    }
                }
                if (matchingField != null) {
                    return new Struct(schema).put(
                            JSON_TYPE_ONE_OF + ".field." + matchingField.getIndex(),
                            toConnectData(matchingField.getSchema(), value)
                    );
                }
                throw new ConnectException("Did not find matching oneof field for data: " + value.toString());
            } else {
                if (!value.isObject()) {
                    throw new ConnectException("Structs should be encoded as JSON objects, but found "
                            + value.getNodeType());
                }

                Struct result = new Struct(schema);
                for (Field field : schema.getFields()) {
                    Object fieldValue = toConnectData(field.getSchema(), value.get(field.getName()));
                    if (fieldValue != null) {
                        result.put(field, fieldValue);
                    }
                }

                return result;
            }
        });
    }

    static {
        TO_CONNECT_LOGICAL_CONVERTERS.put(Decimal.LOGICAL_NAME, (schema, value) -> {
            if (value.isNumber()) {
                return value.decimalValue();
            }
            if (value.isBinary() || value.isTextual()) {
                try {
                    return Decimal.toLogical(schema, value.binaryValue());
                } catch (Exception e) {
                    throw new ConnectException("Invalid bytes for Decimal field", e);
                }
            }

            throw new ConnectException("Invalid type for Decimal, "
                    + "underlying representation should be numeric or bytes but was " + value.getNodeType());
        });

        TO_CONNECT_LOGICAL_CONVERTERS.put(Date.LOGICAL_NAME, (schema, value) -> {
            if (!(value.isInt())) {
                throw new ConnectException(
                        "Invalid type for Date, "
                                + "underlying representation should be integer but was " + value.getNodeType());
            }
            return Date.toLogical(schema, value.intValue());
        });

        TO_CONNECT_LOGICAL_CONVERTERS.put(Time.LOGICAL_NAME, (schema, value) -> {
            if (!(value.isInt())) {
                throw new ConnectException(
                        "Invalid type for Time, "
                                + "underlying representation should be integer but was " + value.getNodeType());
            }
            return Time.toLogical(schema, value.intValue());
        });

        TO_CONNECT_LOGICAL_CONVERTERS.put(Timestamp.LOGICAL_NAME, (schema, value) -> {
            if (!(value.isIntegralNumber())) {
                throw new ConnectException(
                        "Invalid type for Timestamp, "
                                + "underlying representation should be integral but was " + value.getNodeType());
            }
            return Timestamp.toLogical(schema, value.longValue());
        });
    }

    static {
        TO_JSON_LOGICAL_CONVERTERS.put(Decimal.LOGICAL_NAME, (schema, value, config) -> {
            if (!(value instanceof BigDecimal)) {
                throw new ConnectException("Invalid type for Decimal, "
                        + "expected BigDecimal but was " + value.getClass());
            }

            final BigDecimal decimal = (BigDecimal) value;
            switch (config.decimalFormat()) {
                case NUMERIC:
                    return JSON_NODE_FACTORY.numberNode(decimal);
                case BASE64:
                    return JSON_NODE_FACTORY.binaryNode(Decimal.fromLogical(schema, decimal));
                default:
                    throw new ConnectException("Unexpected "
                            + JsonSchemaConverterConfig.DECIMAL_FORMAT_CONFIG + ": " + config.decimalFormat());
            }
        });

        TO_JSON_LOGICAL_CONVERTERS.put(Date.LOGICAL_NAME, (schema, value, config) -> {
            if (!(value instanceof java.util.Date)) {
                throw new ConnectException("Invalid type for Date, expected Date but was " + value.getClass());
            }
            return JSON_NODE_FACTORY.numberNode(Date.fromLogical(schema, (java.util.Date) value));
        });

        TO_JSON_LOGICAL_CONVERTERS.put(Time.LOGICAL_NAME, (schema, value, config) -> {
            if (!(value instanceof java.util.Date)) {
                throw new ConnectException("Invalid type for Time, expected Date but was " + value.getClass());
            }
            return JSON_NODE_FACTORY.numberNode(Time.fromLogical(schema, (java.util.Date) value));
        });

        TO_JSON_LOGICAL_CONVERTERS.put(Timestamp.LOGICAL_NAME, (schema, value, config) -> {
            if (!(value instanceof java.util.Date)) {
                throw new ConnectException("Invalid type for Timestamp, "
                        + "expected Date but was " + value.getClass());
            }
            return JSON_NODE_FACTORY.numberNode(
                    Timestamp.fromLogical(schema, (java.util.Date) value));
        });
    }

    private final Map<Schema, org.everit.json.schema.Schema> fromConnectSchemaCache;
    private final Map<org.everit.json.schema.Schema, Schema> toConnectSchemaCache;
    private final JsonSchemaConverterConfig config;

    public JsonSchemaData(JsonSchemaConverterConfig jsonSchemaDataConfig) {
        this.config = jsonSchemaDataConfig;
        fromConnectSchemaCache = Collections.synchronizedMap(new LinkedHashMap<Schema, org.everit.json.schema.Schema>(300, 1.1F, true) {
            @Override
            protected boolean removeEldestEntry(Map.Entry<Schema, org.everit.json.schema.Schema> eldest) {
                return this.size() > jsonSchemaDataConfig.getSchemasCacheSize();
            }
        });
        toConnectSchemaCache = Collections.synchronizedMap(new LinkedHashMap<org.everit.json.schema.Schema, Schema>(300, 1.1F, true) {
            @Override
            protected boolean removeEldestEntry(Map.Entry<org.everit.json.schema.Schema, Schema> eldest) {
                return this.size() > jsonSchemaDataConfig.getSchemasCacheSize();
            }
        });
    }

    /**
     * to connect data
     *
     * @param schema
     * @param jsonValue
     * @return
     */
    public static Object toConnectData(Schema schema, JsonNode jsonValue) {
        final FieldType schemaType;
        if (schema != null) {
            schemaType = schema.getFieldType();
            if (jsonValue == null || jsonValue.isNull()) {
                if (schema.getDefaultValue() != null) {
                    // any logical type conversions should already have been applied
                    return schema.getDefaultValue();
                }
                if (jsonValue == null || schema.isOptional()) {
                    return null;
                }
                throw new ConnectException("Invalid null value for required " + schemaType + " field");
            }
        } else {
            if (jsonValue == null) {
                return null;
            }
            switch (jsonValue.getNodeType()) {
                case NULL:
                    return null;
                case BOOLEAN:
                    schemaType = FieldType.BOOLEAN;
                    break;
                case NUMBER:
                    if (jsonValue.isIntegralNumber()) {
                        schemaType = FieldType.INT64;
                    } else {
                        schemaType = FieldType.FLOAT64;
                    }
                    break;
                case ARRAY:
                    schemaType = FieldType.ARRAY;
                    break;
                case OBJECT:
                    schemaType = FieldType.MAP;
                    break;
                case STRING:
                    schemaType = FieldType.STRING;
                    break;

                case BINARY:
                case MISSING:
                case POJO:
                default:
                    schemaType = null;
                    break;
            }
        }

        final JsonToConnectTypeConverter typeConverter = TO_CONNECT_CONVERTERS.get(schemaType);
        if (typeConverter == null) {
            throw new ConnectException("Unknown schema type: " + schemaType);
        }

        if (schema != null && schema.getName() != null) {
            JsonToConnectLogicalTypeConverter logicalConverter =
                    TO_CONNECT_LOGICAL_CONVERTERS.get(schema.getName());
            if (logicalConverter != null) {
                return logicalConverter.convert(schema, jsonValue);
            }
        }
        return typeConverter.convert(schema, jsonValue);
    }

    /**
     * no optional schema
     *
     * @param schema
     * @return
     */
    private static Schema nonOptionalSchema(Schema schema) {
        return new Schema(schema.getName(),
                schema.getFieldType(),
                false,
                schema.getDefaultValue(),
                schema.getVersion(),
                schema.getDoc(),
                FieldType.STRUCT.equals(schema.getFieldType()) ? schema.getFields() : null,
                (FieldType.MAP.equals(schema.getFieldType())) ? schema.getKeySchema() : null,
                (FieldType.MAP.equals(schema.getFieldType()) || FieldType.ARRAY.equals(schema.getFieldType())) ? schema.getValueSchema() : null,
                schema.getParameters()
        );
    }

    private static boolean isSimpleSchema(Schema fieldSchema, JsonNode value) {
        switch (fieldSchema.getFieldType()) {
            case INT8:
            case INT16:
            case INT32:
            case INT64:
                return value.isIntegralNumber();
            case FLOAT32:
            case FLOAT64:
                return value.isNumber();
            case BOOLEAN:
                return value.isBoolean();
            case STRING:
                return value.isTextual();
            case BYTES:
                return value.isBinary() || value.isBigDecimal();
            case ARRAY:
                return value.isArray();
            case MAP:
                return value.isObject() || value.isArray();
            case STRUCT:
                return false;
            default:
                throw new IllegalArgumentException("Unsupported type " + fieldSchema.getFieldType());
        }
    }

    private static int matchStructSchema(Schema fieldSchema, JsonNode value) {
        if (fieldSchema.getFieldType() != FieldType.STRUCT || !value.isObject()) {
            return -1;
        }
        Set<String> schemaFields = fieldSchema.getFields()
                .stream()
                .map(Field::getName)
                .collect(Collectors.toSet());
        Set<String> objectFields = new HashSet<>();
        for (Iterator<Map.Entry<String, JsonNode>> iter = value.fields(); iter.hasNext(); ) {
            objectFields.add(iter.next().getKey());
        }
        Set<String> intersectSet = new HashSet<>(schemaFields);
        intersectSet.retainAll(objectFields);
        return intersectSet.size();
    }

    private org.everit.json.schema.Schema rawSchemaFromConnectSchema(Schema schema) {
        if (schema == null) {
            return null;
        }
        org.everit.json.schema.Schema cachedSchema = fromConnectSchemaCache.get(schema);
        if (cachedSchema != null) {
            return cachedSchema;
        }
        org.everit.json.schema.Schema resultSchema = rawSchemaFromConnectSchema(schema, null);
        fromConnectSchemaCache.put(schema, resultSchema);
        return resultSchema;
    }

    private org.everit.json.schema.Schema rawSchemaFromConnectSchema(Schema schema, Integer index) {
        return rawSchemaFromConnectSchema(schema, index, false);
    }

    private org.everit.json.schema.Schema rawSchemaFromConnectSchema(Schema schema, Integer index, boolean ignoreOptional) {
        if (schema == null) {
            return null;
        }
        org.everit.json.schema.Schema.Builder builder;
        Map<String, Object> unprocessedProps = new HashMap<>();
        FieldType schemaType = schema.getFieldType();
        switch (schemaType) {
            case INT8:
            case INT16:
            case INT32:
            case INT64:
                builder = NumberSchema.builder().requiresInteger(true);
                unprocessedProps.put(CONNECT_TYPE_PROP, schemaType.name().toLowerCase());
                break;
            case FLOAT32:
            case FLOAT64:
                builder = NumberSchema.builder().requiresInteger(false);
                unprocessedProps.put(CONNECT_TYPE_PROP, schemaType.name().toLowerCase());
                break;
            case BOOLEAN:
                builder = BooleanSchema.builder();
                break;
            case STRING:
                if (schema.getParameters() != null && schema.getParameters().containsKey(JSON_TYPE_ENUM)) {
                    EnumSchema.Builder enumBuilder = EnumSchema.builder();
                    for (Map.Entry<String, String> entry : schema.getParameters().entrySet()) {
                        if (entry.getKey().startsWith(JSON_TYPE_ENUM_PREFIX)) {
                            enumBuilder.possibleValue(entry.getValue());
                        }
                    }
                    builder = enumBuilder;
                } else {
                    builder = StringSchema.builder();
                }
                break;
            case BYTES:
                builder = Decimal.LOGICAL_NAME.equals(schema.getName())
                        ? NumberSchema.builder()
                        : StringSchema.builder();
                unprocessedProps.put(CONNECT_TYPE_PROP, schemaType.name().toLowerCase());
                break;
            case ARRAY:
                builder = ArraySchema.builder().addItemSchema(rawSchemaFromConnectSchema(schema.getValueSchema()));
                unprocessedProps.put(CONNECT_TYPE_PROP, schemaType.name().toLowerCase());
                break;
            case MAP:
                org.everit.json.schema.Schema keySchema = rawSchemaFromConnectSchema(schema.getKeySchema(), 0);
                org.everit.json.schema.Schema valueSchema = rawSchemaFromConnectSchema(schema.getValueSchema(), 1);
                builder = ObjectSchema.builder()
                        .addPropertySchema(KEY_FIELD, keySchema)
                        .addPropertySchema(VALUE_FIELD, valueSchema);
                builder = ArraySchema.builder().addItemSchema(builder.build());
                unprocessedProps.put(CONNECT_TYPE_PROP, schemaType.name().toLowerCase());
                break;
            case STRUCT:
                if (JSON_TYPE_ONE_OF.equals(schema.getName())) {
                    CombinedSchema.Builder combinedBuilder = CombinedSchema.builder();
                    combinedBuilder.criterion(CombinedSchema.ONE_CRITERION);
                    if (schema.isOptional()) {
                        combinedBuilder.subschema(NullSchema.INSTANCE);
                    }
                    for (Field field : schema.getFields()) {
                        combinedBuilder.subschema(rawSchemaFromConnectSchema(nonOptionalSchema(field.getSchema()),
                                field.getIndex(),
                                true
                        ));
                    }
                    builder = combinedBuilder;
                } else if (schema.isOptional()) {
                    CombinedSchema.Builder combinedBuilder = CombinedSchema.builder();
                    combinedBuilder.criterion(CombinedSchema.ONE_CRITERION);
                    combinedBuilder.subschema(NullSchema.INSTANCE);
                    combinedBuilder.subschema(rawSchemaFromConnectSchema(nonOptionalSchema(schema)));
                    builder = combinedBuilder;
                    break;
                } else {
                    ObjectSchema.Builder objectBuilder = ObjectSchema.builder();
                    for (Field field : schema.getFields()) {
                        org.everit.json.schema.Schema fieldSchema = rawSchemaFromConnectSchema(field.getSchema(),
                                field.getIndex()
                        );
                        objectBuilder.addPropertySchema(field.getName(), fieldSchema);
                    }
                    builder = objectBuilder;
                    break;
                }
            default:
                throw new IllegalArgumentException("Unsupported type " + schemaType);
        }
        if (!(builder instanceof CombinedSchema.Builder)) {
            if (schema.getName() != null) {
                builder.title(schema.getName());
            }
            if (schema.getVersion() != null) {
                unprocessedProps.put(CONNECT_VERSION_PROP, schema.getVersion());
            }
            if (schema.getDoc() != null) {
                builder.description(schema.getDoc());
            }
            if (schema.getParameters() != null) {
                Map<String, String> parameters = schema.getParameters()
                        .entrySet()
                        .stream()
                        .filter(e -> !e.getKey().startsWith(NAMESPACE))
                        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
                if (parameters.size() > 0) {
                    unprocessedProps.put(CONNECT_PARAMETERS_PROP, parameters);
                }
            }
            if (schema.getDefaultValue() != null) {
                builder.defaultValue(schema.getDefaultValue());
            }
            if (!ignoreOptional) {
                if (schema.isOptional()) {
                    CombinedSchema.Builder combinedBuilder = CombinedSchema.builder();
                    combinedBuilder.criterion(CombinedSchema.ONE_CRITERION);
                    combinedBuilder.subschema(NullSchema.INSTANCE);
                    combinedBuilder.subschema(builder.unprocessedProperties(unprocessedProps).build());
                    if (index != null) {
                        combinedBuilder.unprocessedProperties(Collections.singletonMap(CONNECT_INDEX_PROP,
                                index
                        ));
                    }
                    builder = combinedBuilder;
                    unprocessedProps = new HashMap<>();
                }
            }
        }
        if (index != null) {
            unprocessedProps.put(CONNECT_INDEX_PROP, index);
        }
        return builder.unprocessedProperties(unprocessedProps).build();
    }

    /**
     * from json schema
     *
     * @param schema
     * @return
     */
    public org.everit.json.schema.Schema fromJsonSchema(Schema schema) {
        return rawSchemaFromConnectSchema(schema);
    }

    /**
     * convert json schema to connect schema
     *
     * @param jsonSchema
     * @return
     */
    public Schema toConnectSchema(org.everit.json.schema.Schema jsonSchema) {
        return toConnectSchema(jsonSchema, null);
    }

    public Schema toConnectSchema(org.everit.json.schema.Schema schema, Integer version) {
        if (schema == null) {
            return null;
        }
        Schema cachedSchema = toConnectSchemaCache.get(schema);
        if (cachedSchema != null) {
            return cachedSchema;
        }
        Schema resultSchema = toConnectSchema(schema, version, false);
        toConnectSchemaCache.put(schema, resultSchema);
        return resultSchema;
    }

    /**
     * to connect schema
     *
     * @param jsonSchema
     * @param version
     * @param forceOptional
     * @return
     */
    private Schema toConnectSchema(org.everit.json.schema.Schema jsonSchema, Integer version, boolean forceOptional) {
        if (jsonSchema == null) {
            return null;
        }

        final SchemaBuilder builder;
        if (jsonSchema instanceof BooleanSchema) {
            builder = SchemaBuilder.bool();
        } else if (jsonSchema instanceof NumberSchema) {
            NumberSchema numberSchema = (NumberSchema) jsonSchema;
            String type = (String) numberSchema.getUnprocessedProperties().get(CONNECT_TYPE_PROP);
            if (type == null) {
                builder = numberSchema.requiresInteger() ? SchemaBuilder.int64() : SchemaBuilder.float64();
            } else {
                switch (type) {
                    case CONNECT_TYPE_INT8:
                        builder = SchemaBuilder.int8();
                        break;
                    case CONNECT_TYPE_INT16:
                        builder = SchemaBuilder.int16();
                        break;
                    case CONNECT_TYPE_INT32:
                        builder = SchemaBuilder.int32();
                        break;
                    case CONNECT_TYPE_INT64:
                        builder = SchemaBuilder.int64();
                        break;
                    case CONNECT_TYPE_FLOAT32:
                        builder = SchemaBuilder.float32();
                        break;
                    case CONNECT_TYPE_FLOAT64:
                        builder = SchemaBuilder.float64();
                        break;
                    case CONNECT_TYPE_BYTES:
                        builder = SchemaBuilder.bytes();
                        break;
                    default:
                        throw new IllegalArgumentException("Unsupported type " + type);
                }
            }
        } else if (jsonSchema instanceof StringSchema) {
            String type = (String) jsonSchema.getUnprocessedProperties().get(CONNECT_TYPE_PROP);
            builder = CONNECT_TYPE_BYTES.equals(type) ? SchemaBuilder.bytes() : SchemaBuilder.string();

        } else if (jsonSchema instanceof EnumSchema) {
            EnumSchema enumSchema = (EnumSchema) jsonSchema;
            builder = SchemaBuilder.string();
            builder.parameter(JSON_TYPE_ENUM, "");  // JSON enums have no name, use empty string as placeholder
            for (Object enumObj : enumSchema.getPossibleValuesAsList()) {
                String enumSymbol = enumObj.toString();
                builder.parameter(JSON_TYPE_ENUM_PREFIX + enumSymbol, enumSymbol);
            }
        } else if (jsonSchema instanceof CombinedSchema) {
            CombinedSchema combinedSchema = (CombinedSchema) jsonSchema;
            CombinedSchema.ValidationCriterion criterion = combinedSchema.getCriterion();
            String name = null;
            if (criterion == CombinedSchema.ONE_CRITERION || criterion == CombinedSchema.ANY_CRITERION) {
                name = JSON_TYPE_ONE_OF;
            } else if (criterion == CombinedSchema.ALL_CRITERION) {
                return allOfToConnectSchema(combinedSchema, version, forceOptional);
            } else {
                throw new IllegalArgumentException("Unsupported criterion: " + criterion);
            }
            if (combinedSchema.getSubschemas().size() == 2) {
                boolean foundNullSchema = false;
                org.everit.json.schema.Schema nonNullSchema = null;
                for (org.everit.json.schema.Schema subSchema : combinedSchema.getSubschemas()) {
                    if (subSchema instanceof NullSchema) {
                        foundNullSchema = true;
                    } else {
                        nonNullSchema = subSchema;
                    }
                }
                if (foundNullSchema) {
                    return toConnectSchema(nonNullSchema, version, true);
                }
            }
            int index = 0;
            builder = SchemaBuilder.struct().name(name);
            for (org.everit.json.schema.Schema subSchema : combinedSchema.getSubschemas()) {
                if (subSchema instanceof NullSchema) {
                    builder.optional();
                } else {
                    String subFieldName = name + ".field." + index++;
                    builder.field(subFieldName, toConnectSchema(subSchema, null, true));
                }
            }
        } else if (jsonSchema instanceof ArraySchema) {
            ArraySchema arraySchema = (ArraySchema) jsonSchema;
            org.everit.json.schema.Schema itemsSchema = arraySchema.getAllItemSchema();
            if (itemsSchema == null) {
                throw new ConnectException("Array schema did not specify the items type");
            }
            String type = (String) arraySchema.getUnprocessedProperties().get(CONNECT_TYPE_PROP);
            if (CONNECT_TYPE_MAP.equals(type) && itemsSchema instanceof ObjectSchema) {
                ObjectSchema objectSchema = (ObjectSchema) itemsSchema;
                builder = SchemaBuilder.map(toConnectSchema(objectSchema.getPropertySchemas().get(KEY_FIELD)),
                        toConnectSchema(objectSchema.getPropertySchemas().get(VALUE_FIELD))
                );
            } else {
                builder = SchemaBuilder.array(toConnectSchema(itemsSchema));
            }
        } else if (jsonSchema instanceof ObjectSchema) {
            ObjectSchema objectSchema = (ObjectSchema) jsonSchema;
            String type = (String) objectSchema.getUnprocessedProperties().get(CONNECT_TYPE_PROP);
            if (CONNECT_TYPE_MAP.equals(type)) {
                builder = SchemaBuilder.map(
                        SchemaBuilder.string().build(),
                        toConnectSchema(objectSchema.getSchemaOfAdditionalProperties())
                );
            } else {
                builder = SchemaBuilder.struct();
                Map<String, org.everit.json.schema.Schema> properties = objectSchema.getPropertySchemas();
                SortedMap<Integer, Map.Entry<String, org.everit.json.schema.Schema>> sortedMap = new TreeMap<>();
                for (Map.Entry<String, org.everit.json.schema.Schema> property : properties.entrySet()) {
                    org.everit.json.schema.Schema subSchema = property.getValue();
                    Integer index = (Integer) subSchema.getUnprocessedProperties().get(CONNECT_INDEX_PROP);
                    if (index == null) {
                        index = sortedMap.size();
                    }
                    sortedMap.put(index, property);
                }
                for (Map.Entry<String, org.everit.json.schema.Schema> property : sortedMap.values()) {
                    String subFieldName = property.getKey();
                    org.everit.json.schema.Schema subSchema = property.getValue();
                    boolean isFieldOptional = config.useOptionalForNonRequiredProperties()
                            && !objectSchema.getRequiredProperties().contains(subFieldName);
                    builder.field(subFieldName, toConnectSchema(subSchema, null, isFieldOptional));
                }
            }
        } else if (jsonSchema instanceof ReferenceSchema) {
            ReferenceSchema refSchema = (ReferenceSchema) jsonSchema;
            return toConnectSchema(refSchema.getReferredSchema(), version, forceOptional);
        } else {
            throw new ConnectException("Unsupported schema type " + jsonSchema.getClass().getName());
        }

        String title = jsonSchema.getTitle();
        if (title != null) {
            builder.name(title);
        }
        Integer connectVersion = (Integer) jsonSchema.getUnprocessedProperties()
                .get(CONNECT_VERSION_PROP);
        if (connectVersion != null) {
            builder.version(connectVersion);
        } else if (version != null) {
            builder.version(version);
        }
        String description = jsonSchema.getDescription();
        if (description != null) {
            builder.doc(description);
        }
        Map<String, String> parameters = (Map<String, String>) jsonSchema.getUnprocessedProperties()
                .get(CONNECT_PARAMETERS_PROP);
        if (parameters != null) {
            builder.parameters(parameters);
        }
        if (jsonSchema.hasDefaultValue()) {
            JsonNode jsonNode = OBJECT_MAPPER.convertValue(jsonSchema.getDefaultValue(), JsonNode.class);
            builder.defaultValue(toConnectData(builder.build(), jsonNode));
        }

        if (!forceOptional) {
            builder.required();
        }

        Schema result = builder.build();
        return result;
    }

    /**
     * all of to connect schema
     *
     * @param combinedSchema
     * @param version
     * @param forceOptional
     * @return
     */
    private Schema allOfToConnectSchema(CombinedSchema combinedSchema, Integer version, boolean forceOptional) {
        ConstSchema constSchema = null;
        EnumSchema enumSchema = null;
        NumberSchema numberSchema = null;
        StringSchema stringSchema = null;
        for (org.everit.json.schema.Schema subSchema : combinedSchema.getSubschemas()) {
            if (subSchema instanceof ConstSchema) {
                constSchema = (ConstSchema) subSchema;
            } else if (subSchema instanceof EnumSchema) {
                enumSchema = (EnumSchema) subSchema;
            } else if (subSchema instanceof NumberSchema) {
                numberSchema = (NumberSchema) subSchema;
            } else if (subSchema instanceof StringSchema) {
                stringSchema = (StringSchema) subSchema;
            }
        }
        if (constSchema != null && stringSchema != null) {
            return toConnectSchema(stringSchema, version, forceOptional);
        } else if (constSchema != null && numberSchema != null) {
            return toConnectSchema(numberSchema, version, forceOptional);
        } else if (enumSchema != null && stringSchema != null) {
            return toConnectSchema(enumSchema, version, forceOptional);
        } else if (numberSchema != null
                && stringSchema != null
                && stringSchema.getFormatValidator() != null) {
            return toConnectSchema(numberSchema, version, forceOptional);
        } else {
            throw new IllegalArgumentException("Unsupported criterion "
                    + combinedSchema.getCriterion() + " for " + combinedSchema);
        }
    }

    /**
     * Convert connect data to json schema
     *
     * @param schema
     * @param logicalValue
     * @return
     */
    public JsonNode fromConnectData(Schema schema, Object logicalValue) {
        if (logicalValue == null) {
            if (schema == null) {
                // Any schema is valid and we don't have a default, so treat this as an optional schema
                return null;
            }
            if (schema.getDefaultValue() != null) {
                return fromConnectData(schema, schema.getDefaultValue());
            }
            if (schema.isOptional()) {
                return JSON_NODE_FACTORY.nullNode();
            }
            return null;
        }

        Object value = logicalValue;
        if (schema != null && schema.getName() != null) {
            ConnectToJsonLogicalTypeConverter logicalConverter =
                    TO_JSON_LOGICAL_CONVERTERS.get(schema.getName());
            if (logicalConverter != null) {
                return logicalConverter.convert(schema, logicalValue, config);
            }
        }

        try {
            final FieldType schemaType;
            if (schema == null) {
                schemaType = Schema.schemaType(value.getClass());
                if (schemaType == null) {
                    throw new ConnectException("Java class "
                            + value.getClass()
                            + " does not have corresponding schema type.");
                }
            } else {
                schemaType = schema.getFieldType();
            }
            switch (schemaType) {
                case INT8:
                    // Use shortValue to create a ShortNode, otherwise an IntNode will be created
                    return JSON_NODE_FACTORY.numberNode(((Byte) value).shortValue());
                case INT16:
                    return JSON_NODE_FACTORY.numberNode((Short) value);
                case INT32:
                    return JSON_NODE_FACTORY.numberNode((Integer) value);
                case INT64:
                    return JSON_NODE_FACTORY.numberNode((Long) value);
                case FLOAT32:
                    return JSON_NODE_FACTORY.numberNode((Float) value);
                case FLOAT64:
                    return JSON_NODE_FACTORY.numberNode((Double) value);
                case BOOLEAN:
                    return JSON_NODE_FACTORY.booleanNode((Boolean) value);
                case STRING:
                    CharSequence charSeq = (CharSequence) value;
                    return JSON_NODE_FACTORY.textNode(charSeq.toString());
                case BYTES:
                    if (value instanceof byte[]) {
                        return JSON_NODE_FACTORY.binaryNode((byte[]) value);
                    } else if (value instanceof ByteBuffer) {
                        return JSON_NODE_FACTORY.binaryNode(((ByteBuffer) value).array());
                    } else if (value instanceof BigDecimal) {
                        return JSON_NODE_FACTORY.numberNode((BigDecimal) value);
                    } else {
                        throw new ConnectException("Invalid type for bytes type: " + value.getClass());
                    }
                case ARRAY: {
                    Collection collection = (Collection) value;
                    ArrayNode list = JSON_NODE_FACTORY.arrayNode();
                    for (Object elem : collection) {
                        Schema valueSchema = schema == null ? null : schema.getValueSchema();
                        JsonNode fieldValue = fromConnectData(valueSchema, elem);
                        list.add(fieldValue);
                    }
                    return list;
                }
                case MAP: {
                    Map<?, ?> map = (Map<?, ?>) value;
                    // If true, using string keys and JSON object; if false, using non-string keys and
                    // Array-encoding
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
                        objectMode = schema.getKeySchema().getFieldType() == FieldType.STRING && !schema.getKeySchema()
                                .isOptional();
                    }
                    ObjectNode obj = null;
                    ArrayNode list = null;
                    if (objectMode) {
                        obj = JSON_NODE_FACTORY.objectNode();
                    } else {
                        list = JSON_NODE_FACTORY.arrayNode();
                    }
                    for (Map.Entry<?, ?> entry : map.entrySet()) {
                        Schema keySchema = schema == null ? null : schema.getKeySchema();
                        Schema valueSchema = schema == null ? null : schema.getValueSchema();
                        JsonNode mapKey = fromConnectData(keySchema, entry.getKey());
                        JsonNode mapValue = fromConnectData(valueSchema, entry.getValue());
                        if (objectMode) {
                            obj.set(mapKey.asText(), mapValue);
                        } else {
                            ObjectNode o = JSON_NODE_FACTORY.objectNode();
                            o.set(KEY_FIELD, mapKey);
                            o.set(VALUE_FIELD, mapValue);
                            list.add(o);
                        }
                    }
                    return objectMode ? obj : list;
                }
                case STRUCT: {
                    Struct struct = (Struct) value;
                    if (!struct.schema().equals(schema)) {
                        throw new ConnectException("Mismatching schema.");
                    }
                    //This handles the inverting of a union which is held as a struct, where each field is
                    // one of the union types.
                    if (JSON_TYPE_ONE_OF.equals(schema.getName())) {
                        for (Field field : schema.getFields()) {
                            Object object = struct.get(field);
                            if (object != null) {
                                return fromConnectData(field.getSchema(), object);
                            }
                        }
                        return fromConnectData(schema, null);
                    } else {
                        ObjectNode obj = JSON_NODE_FACTORY.objectNode();
                        for (Field field : schema.getFields()) {
                            JsonNode jsonNode = fromConnectData(field.getSchema(), struct.get(field));
                            if (jsonNode != null) {
                                obj.set(field.getName(), jsonNode);
                            }
                        }
                        return obj;
                    }
                }
                default:
                    break;
            }

            throw new ConnectException("Couldn't convert " + value + " to JSON.");
        } catch (ClassCastException e) {
            String schemaTypeStr = (schema != null) ? schema.getFieldType().toString() : "unknown schema";
            throw new ConnectException("Invalid type for " + schemaTypeStr + ": " + value.getClass());
        }
    }


    private interface JsonToConnectTypeConverter {
        Object convert(Schema schema, JsonNode value);
    }

    private interface ConnectToJsonLogicalTypeConverter {
        JsonNode convert(Schema schema, Object value, JsonSchemaConverterConfig config);
    }

    private interface JsonToConnectLogicalTypeConverter {
        Object convert(Schema schema, JsonNode value);
    }
}