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
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.apache.rocketmq.connect.kafka.connect.adaptor.schema;

import io.openmessaging.connector.api.data.Field;
import io.openmessaging.connector.api.data.FieldType;
import io.openmessaging.connector.api.data.Schema;
import io.openmessaging.connector.api.data.logical.Timestamp;
import io.openmessaging.connector.api.errors.ConnectException;
import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * convert rocketmq connect record data to kafka sink record data
 */
public class KafkaSinkSchemaConverter {
    private static Logger logger = LoggerFactory.getLogger(KafkaSinkSchemaConverter.class);
    private static Map<String, String> logicalMapping = new HashMap<>();

    static {
        logicalMapping.put(io.openmessaging.connector.api.data.logical.Decimal.LOGICAL_NAME, Decimal.LOGICAL_NAME);
        logicalMapping.put(io.openmessaging.connector.api.data.logical.Date.LOGICAL_NAME, Date.LOGICAL_NAME);
        logicalMapping.put(io.openmessaging.connector.api.data.logical.Time.LOGICAL_NAME, Time.LOGICAL_NAME);
        logicalMapping.put(io.openmessaging.connector.api.data.logical.Timestamp.LOGICAL_NAME, Timestamp.LOGICAL_NAME);
    }

    private SchemaBuilder builder;

    public KafkaSinkSchemaConverter(Schema schema) {
        builder = convertKafkaSchema(schema);
    }

    public org.apache.kafka.connect.data.Schema schema() {
        return builder.build();
    }

    /**
     * convert kafka schema
     *
     * @param originalSchema
     * @return
     */
    private SchemaBuilder convertKafkaSchema(io.openmessaging.connector.api.data.Schema originalSchema) {
        String schemaName = convertSchemaName(originalSchema.getName());
        Map<String, String> parameters = originalSchema.getParameters() == null ? new HashMap<>() : originalSchema.getParameters();

        switch (originalSchema.getFieldType()) {
            case INT8:
                return SchemaBuilder
                        .int8()
                        .optional()
                        .name(schemaName)
                        .doc(originalSchema.getDoc())
                        .defaultValue(originalSchema.getDefaultValue())
                        .parameters(parameters)
                        ;
            case INT16:
                return SchemaBuilder
                        .int16()
                        .optional()
                        .name(schemaName)
                        .doc(originalSchema.getDoc())
                        .defaultValue(originalSchema.getDefaultValue())
                        .parameters(parameters)
                        ;
            case INT32:
                return SchemaBuilder
                        .int32()
                        .optional()
                        .name(schemaName)
                        .doc(originalSchema.getDoc())
                        .defaultValue(originalSchema.getDefaultValue())
                        .parameters(parameters)
                        ;
            case INT64:
                return SchemaBuilder
                        .int64()
                        .optional()
                        .name(schemaName)
                        .doc(originalSchema.getDoc())
                        .defaultValue(originalSchema.getDefaultValue())
                        .parameters(parameters)
                        ;
            case FLOAT32:
                return SchemaBuilder
                        .float32()
                        .optional()
                        .name(schemaName)
                        .doc(originalSchema.getDoc())
                        .defaultValue(originalSchema.getDefaultValue())
                        .parameters(parameters)
                        ;
            case FLOAT64:
                return SchemaBuilder
                        .float64()
                        .optional()
                        .name(schemaName)
                        .doc(originalSchema.getDoc())
                        .defaultValue(originalSchema.getDefaultValue())
                        .parameters(parameters)
                        ;
            case BOOLEAN:
                return SchemaBuilder
                        .bool()
                        .optional()
                        .name(schemaName)
                        .doc(originalSchema.getDoc())
                        .defaultValue(originalSchema.getDefaultValue())
                        .parameters(parameters)
                        ;
            case STRING:
                return SchemaBuilder.
                        string()
                        .optional()
                        .name(schemaName)
                        .doc(originalSchema.getDoc())
                        .defaultValue(originalSchema.getDefaultValue())
                        .parameters(parameters)
                        ;
            case BYTES:
                return SchemaBuilder
                        .bytes()
                        .optional()
                        .name(schemaName)
                        .doc(originalSchema.getDoc())
                        .defaultValue(originalSchema.getDefaultValue())
                        .parameters(parameters)
                        ;
            case STRUCT:
                SchemaBuilder schemaBuilder = SchemaBuilder
                        .struct()
                        .optional()
                        .name(schemaName)
                        .doc(originalSchema.getDoc())
                        .defaultValue(originalSchema.getDefaultValue())
                        .parameters(parameters);
                convertStructSchema(schemaBuilder, originalSchema);
                return schemaBuilder;
            case ARRAY:
                return SchemaBuilder.array(convertKafkaSchema(originalSchema.getValueSchema()).build())
                        .optional()
                        .name(schemaName)
                        .doc(originalSchema.getDoc())
                        .defaultValue(originalSchema.getDefaultValue())
                        .parameters(parameters)
                        ;
            case MAP:
                return SchemaBuilder.map(
                        convertKafkaSchema(originalSchema.getKeySchema()).build(),
                        convertKafkaSchema(originalSchema.getValueSchema()).build()
                ).optional()
                        .name(schemaName)
                        .doc(originalSchema.getDoc())
                        .defaultValue(originalSchema.getDefaultValue())
                        .parameters(parameters)
                        ;
            default:
                throw new RuntimeException(" Type not supported: {}" + originalSchema.getFieldType());

        }

    }

    /**
     * convert schema
     *
     * @param schemaBuilder
     * @param originalSchema
     */
    private void convertStructSchema(org.apache.kafka.connect.data.SchemaBuilder schemaBuilder, io.openmessaging.connector.api.data.Schema originalSchema) {
        for (Field field : originalSchema.getFields()) {
            try {

                // schema
                Schema schema = field.getSchema();
                String schemaName = convertSchemaName(field.getSchema().getName());

                // field name
                String fieldName = field.getName();
                FieldType type = schema.getFieldType();

                Map<String, String> parameters = schema.getParameters() == null ? new HashMap<>() : schema.getParameters();

                switch (type) {
                    case INT8:
                        schemaBuilder.field(
                                fieldName,
                                SchemaBuilder
                                        .int8()
                                        .name(schemaName)
                                        .doc(schema.getDoc())
                                        .defaultValue(schema.getDefaultValue())
                                        .parameters(parameters)
                                        .optional()
                                        .build()
                        );
                        break;
                    case INT16:
                        schemaBuilder.field(
                                fieldName,
                                SchemaBuilder
                                        .int16()
                                        .name(schemaName)
                                        .doc(schema.getDoc())
                                        .defaultValue(schema.getDefaultValue())
                                        .parameters(parameters)
                                        .optional()
                                        .build()
                        );
                        break;
                    case INT32:
                        schemaBuilder.field(
                                fieldName,
                                SchemaBuilder
                                        .int32()
                                        .name(schemaName)
                                        .doc(schema.getDoc())
                                        .defaultValue(schema.getDefaultValue())
                                        .parameters(parameters)
                                        .optional()
                                        .build()
                        );
                        break;
                    case INT64:
                        schemaBuilder.field(
                                fieldName,
                                SchemaBuilder
                                        .int64()
                                        .name(schemaName)
                                        .doc(schema.getDoc())
                                        .defaultValue(schema.getDefaultValue())
                                        .parameters(parameters)
                                        .optional()
                                        .build()
                        );
                        break;
                    case FLOAT32:
                        schemaBuilder.field(
                                fieldName,
                                SchemaBuilder
                                        .float32()
                                        .name(schemaName)
                                        .doc(schema.getDoc())
                                        .defaultValue(schema.getDefaultValue())
                                        .parameters(parameters)
                                        .optional()
                                        .build()
                        );
                        break;
                    case FLOAT64:
                        schemaBuilder.field(
                                fieldName,
                                SchemaBuilder
                                        .float64()
                                        .name(schemaName)
                                        .doc(schema.getDoc())
                                        .defaultValue(schema.getDefaultValue())
                                        .parameters(parameters)
                                        .optional()
                                        .build()
                        );
                        break;
                    case BOOLEAN:
                        schemaBuilder.field(
                                fieldName,
                                SchemaBuilder
                                        .bool()
                                        .name(schemaName)
                                        .doc(schema.getDoc())
                                        .defaultValue(schema.getDefaultValue())
                                        .parameters(parameters)
                                        .optional()
                                        .build()
                        );
                        break;
                    case STRING:
                        schemaBuilder.field(
                                fieldName,
                                SchemaBuilder
                                        .string()
                                        .name(schemaName)
                                        .doc(schema.getDoc())
                                        .defaultValue(schema.getDefaultValue())
                                        .parameters(parameters)
                                        .optional()
                                        .build()
                        );
                        break;
                    case BYTES:
                        schemaBuilder.field(
                                fieldName,
                                SchemaBuilder
                                        .bytes()
                                        .name(schemaName)
                                        .doc(schema.getDoc())
                                        .defaultValue(schema.getDefaultValue())
                                        .parameters(parameters)
                                        .optional()
                                        .build()
                        );
                        break;
                    case STRUCT:
                    case ARRAY:
                    case MAP:
                        schemaBuilder.field(
                                fieldName,
                                convertKafkaSchema(field.getSchema()).build()
                        );
                        break;
                    default:
                        break;
                }
            } catch (Exception ex) {
                logger.error("Convert schema failure! ex {}", ex);
                throw new ConnectException(ex);
            }
        }
    }


    private String convertSchemaName(String schemaName) {
        if (logicalMapping.containsKey(schemaName)) {
            return logicalMapping.get(schemaName);
        }
        return schemaName;
    }

}
