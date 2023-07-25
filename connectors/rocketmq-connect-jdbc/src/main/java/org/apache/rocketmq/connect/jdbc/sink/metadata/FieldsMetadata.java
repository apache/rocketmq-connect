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
package org.apache.rocketmq.connect.jdbc.sink.metadata;

import io.openmessaging.KeyValue;
import io.openmessaging.connector.api.data.Field;
import io.openmessaging.connector.api.data.FieldType;
import io.openmessaging.connector.api.data.Schema;
import io.openmessaging.connector.api.errors.ConnectException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.rocketmq.connect.jdbc.sink.JdbcSinkConfig;


/**
 * fields metadata
 */
public class FieldsMetadata {

    public final Set<String> keyFieldNames;
    public final Set<String> nonKeyFieldNames;
    public final Map<String, SinkRecordField> allFields;

    // visible for testing
    public FieldsMetadata(
            Set<String> keyFieldNames,
            Set<String> nonKeyFieldNames,
            Map<String, SinkRecordField> allFields
    ) {
        boolean fieldCountsMatch = (keyFieldNames.size() + nonKeyFieldNames.size()) == allFields.size();
        boolean allFieldsContained = allFields.keySet().containsAll(keyFieldNames)
                && allFields.keySet().containsAll(nonKeyFieldNames);
        if (!fieldCountsMatch || !allFieldsContained) {
            throw new IllegalArgumentException(String.format(
                    "Validation fail -- keyFieldNames:%s nonKeyFieldNames:%s allFields:%s",
                    keyFieldNames, nonKeyFieldNames, allFields
            ));
        }
        this.keyFieldNames = keyFieldNames;
        this.nonKeyFieldNames = nonKeyFieldNames;
        this.allFields = allFields;
    }

    public static FieldsMetadata extract(
            final String tableName,
            final JdbcSinkConfig.PrimaryKeyMode pkMode,
            final List<String> configuredPkFields,
            final Set<String> fieldsWhitelist,
            final SchemaPair schemaPair
    ) {
        return extract(
            tableName,
            pkMode,
            configuredPkFields,
            fieldsWhitelist,
            schemaPair.keySchema,
            schemaPair.valueSchema,
            schemaPair.extensions
        );
    }

    /**
     * extract metadata info
     *
     * @param tableName
     * @param pkMode
     * @param configuredPkFields
     * @param fieldsWhitelist
     * @param schema
     * @param headers
     * @return
     */
    public static FieldsMetadata extract(
            final String tableName,
            final JdbcSinkConfig.PrimaryKeyMode pkMode,
            final List<String> configuredPkFields,
            final Set<String> fieldsWhitelist,
            final Schema keySchema,
            final Schema schema,
            final KeyValue headers
    ) {
        if (schema != null && schema.getFieldType() != FieldType.STRUCT) {
            throw new ConnectException("Value schema must be of type Struct");
        }
        final Map<String, SinkRecordField> allFields = new HashMap<>();
        final Set<String> keyFieldNames = new LinkedHashSet<>();
        switch (pkMode) {
            case NONE:
                break;
            case RECORD_KEY:
                extractRecordKeyPk(tableName, configuredPkFields, keySchema, allFields, keyFieldNames);
                break;
            case RECORD_VALUE:
                extractRecordValuePk(tableName, configuredPkFields, schema, headers, allFields, keyFieldNames);
                break;
            default:
                throw new ConnectException("Unknown primary key mode: " + pkMode);
        }
        final Set<String> nonKeyFieldNames = new LinkedHashSet<>();
        if (schema != null) {
            for (Field field : schema.getFields()) {
                if (keyFieldNames.contains(field.getName())) {
                    continue;
                }
                if (!fieldsWhitelist.isEmpty() && !fieldsWhitelist.contains(field.getName())) {
                    continue;
                }
                nonKeyFieldNames.add(field.getName());
                final Schema fieldSchema = field.getSchema();
                allFields.put(field.getName(), new SinkRecordField(fieldSchema, field.getName(), false));
            }
        }

        if (allFields.isEmpty()) {
            throw new ConnectException(
                    "No fields found using key and value schemas for table: " + tableName
            );
        }
        final Map<String, SinkRecordField> allFieldsOrdered = new LinkedHashMap<>();
        if (schema != null) {
            for (Field field : schema.getFields()) {
                String fieldName = field.getName();
                if (allFields.containsKey(fieldName)) {
                    allFieldsOrdered.put(fieldName, allFields.get(fieldName));
                }
            }
        }

        if (allFieldsOrdered.size() < allFields.size()) {
            ArrayList<String> fieldKeys = new ArrayList<>(allFields.keySet());
            Collections.sort(fieldKeys);
            for (String fieldName : fieldKeys) {
                if (!allFieldsOrdered.containsKey(fieldName)) {
                    allFieldsOrdered.put(fieldName, allFields.get(fieldName));
                }
            }
        }

        return new FieldsMetadata(keyFieldNames, nonKeyFieldNames, allFieldsOrdered);
    }

    private static void extractRecordKeyPk(
            final String tableName,
            final List<String> configuredPkFields,
            final Schema keySchema,
            final Map<String, SinkRecordField> allFields,
            final Set<String> keyFieldNames
    ) {
        if (keySchema == null) {
            throw new ConnectException(String.format(
                    "PK mode for table '%s' is %s, but record key schema is missing",
                    tableName,
                    JdbcSinkConfig.PrimaryKeyMode.RECORD_KEY
            ));
        }
        final FieldType keySchemaType = keySchema.getFieldType();
        switch (keySchemaType) {
            case STRUCT:
                if (configuredPkFields.isEmpty()) {
                    keySchema.getFields().forEach(keyField -> {
                        keyFieldNames.add(keyField.getName());
                    });
                } else {
                    for (String fieldName : configuredPkFields) {
                        final Field keyField = keySchema.getField(fieldName);
                        if (keyField == null) {
                            throw new ConnectException(String.format(
                                    "PK mode for table '%s' is %s with configured PK fields %s, but record key "
                                            + "schema does not contain field: %s",
                                    tableName, JdbcSinkConfig.PrimaryKeyMode.RECORD_KEY, configuredPkFields, fieldName
                            ));
                        }
                    }
                    keyFieldNames.addAll(configuredPkFields);
                }
                for (String fieldName : keyFieldNames) {
                    final Schema fieldSchema = keySchema.getField(fieldName).getSchema();
                    allFields.put(fieldName, new SinkRecordField(fieldSchema, fieldName, true));
                }
                break;
            default:
                if (keySchemaType.isPrimitive()) {
                    if (configuredPkFields.size() != 1) {
                        throw new ConnectException(String.format(
                                "Need exactly one PK column defined since the key schema for records is a "
                                        + "primitive type, defined columns are: %s",
                                configuredPkFields
                        ));
                    }
                    final String fieldName = configuredPkFields.get(0);
                    keyFieldNames.add(fieldName);
                    allFields.put(fieldName, new SinkRecordField(keySchema, fieldName, true));
                } else {
                    throw new ConnectException(
                            "Key schema must be primitive type or Struct, but is of type: " + keySchemaType
                    );
                }
        }
    }

    /**
     * record value
     *
     * @param tableName
     * @param configuredPkFields
     * @param valueSchema
     * @param headers
     * @param allFields
     * @param keyFieldNames
     */
    private static void extractRecordValuePk(
            final String tableName,
            final List<String> configuredPkFields,
            final Schema valueSchema,
            final KeyValue headers,
            final Map<String, SinkRecordField> allFields,
            final Set<String> keyFieldNames
    ) {
        if (valueSchema == null) {
            throw new ConnectException(String.format(
                    "PK mode for table '%s' is %s, but record value schema is missing",
                    tableName,
                    JdbcSinkConfig.PrimaryKeyMode.RECORD_VALUE)
            );
        }
        List<String> pkFields = new ArrayList<>(configuredPkFields);
        if (pkFields.isEmpty()) {
            for (Field keyField : valueSchema.getFields()) {
                keyFieldNames.add(keyField.getName());
            }
        } else {
            for (Field keyField : valueSchema.getFields()) {
                keyFieldNames.add(keyField.getName());
            }
            for (String fieldName : pkFields) {
                if (!keyFieldNames.contains(fieldName)) {
                    throw new ConnectException(String.format(
                            "PK mode for table '%s' is %s with configured PK fields %s, but record value "
                                    + "schema does not contain field: %s",
                            tableName,
                            JdbcSinkConfig.PrimaryKeyMode.RECORD_VALUE,
                            pkFields,
                            fieldName
                    ));
                }
            }
            keyFieldNames.addAll(pkFields);
        }
        for (String fieldName : keyFieldNames) {
            final Schema fieldSchema = valueSchema.getField(fieldName).getSchema();
            allFields.put(fieldName, new SinkRecordField(fieldSchema, fieldName, true));
        }

    }

    public static boolean isPrimitive(FieldType type) {
        switch (type) {
            case INT8:
            case INT32:
            case INT64:
            case FLOAT32:
            case FLOAT64:
            case BOOLEAN:
            case STRING:
            case BYTES:
                return true;
            default:
                return false;
        }
    }

    @Override
    public String toString() {
        return "FieldsMetadata{"
                + "keyFieldNames=" + keyFieldNames
                + ", nonKeyFieldNames=" + nonKeyFieldNames
                + ", allFields=" + allFields
                + '}';
    }

}
