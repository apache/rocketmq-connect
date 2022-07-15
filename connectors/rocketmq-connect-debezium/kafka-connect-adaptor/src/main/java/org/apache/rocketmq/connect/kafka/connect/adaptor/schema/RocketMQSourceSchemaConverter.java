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

import io.openmessaging.connector.api.data.SchemaBuilder;
import io.openmessaging.connector.api.data.logical.Timestamp;
import io.openmessaging.connector.api.errors.ConnectException;
import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;


/**
 * schema  converter
 */
public class RocketMQSourceSchemaConverter {
    private static Logger logger = LoggerFactory.getLogger(RocketMQSourceSchemaConverter.class);
    private static Map<String, String> logicalMapping = new HashMap<>();

    static {
        logicalMapping.put(Decimal.LOGICAL_NAME, io.openmessaging.connector.api.data.logical.Decimal.LOGICAL_NAME);
        logicalMapping.put(Date.LOGICAL_NAME, io.openmessaging.connector.api.data.logical.Date.LOGICAL_NAME);
        logicalMapping.put(Time.LOGICAL_NAME, io.openmessaging.connector.api.data.logical.Time.LOGICAL_NAME);
        logicalMapping.put(Timestamp.LOGICAL_NAME, io.openmessaging.connector.api.data.logical.Timestamp.LOGICAL_NAME);
    }

    private SchemaBuilder builder;

    public RocketMQSourceSchemaConverter(Schema schema) {
        builder = convertKafkaSchema(schema);
    }

    public io.openmessaging.connector.api.data.Schema schema() {
        return builder.build();
    }

    private SchemaBuilder convertKafkaSchema(org.apache.kafka.connect.data.Schema originalSchema) {
        String schemaName = convertSchemaName(originalSchema.name());
        Map<String, String> parameters = originalSchema.parameters() == null ? new HashMap<>() : originalSchema.parameters();
        switch (originalSchema.type()) {
            case INT8:
                return SchemaBuilder
                        .int8()
                        .optional()
                        .name(schemaName)
                        .doc(originalSchema.doc())
                        .defaultValue(originalSchema.defaultValue())
                        .parameters(parameters)
                        ;
            case INT16:
                return SchemaBuilder
                        .int16()
                        .optional()
                        .name(schemaName)
                        .doc(originalSchema.doc())
                        .defaultValue(originalSchema.defaultValue())
                        .parameters(parameters)
                        ;
            case INT32:
                return SchemaBuilder
                        .int32()
                        .optional()
                        .name(schemaName)
                        .doc(originalSchema.doc())
                        .defaultValue(originalSchema.defaultValue())
                        .parameters(parameters)
                        ;
            case INT64:
                return SchemaBuilder
                        .int64()
                        .optional()
                        .name(schemaName)
                        .doc(originalSchema.doc())
                        .defaultValue(originalSchema.defaultValue())
                        .parameters(parameters)
                        ;
            case FLOAT32:
                return SchemaBuilder
                        .float32()
                        .optional()
                        .name(schemaName)
                        .doc(originalSchema.doc())
                        .defaultValue(originalSchema.defaultValue())
                        .parameters(parameters)
                        ;
            case FLOAT64:
                return SchemaBuilder
                        .float64()
                        .optional()
                        .name(schemaName)
                        .doc(originalSchema.doc())
                        .defaultValue(originalSchema.defaultValue())
                        .parameters(parameters)
                        ;
            case BOOLEAN:
                return SchemaBuilder
                        .bool()
                        .optional()
                        .name(schemaName)
                        .doc(originalSchema.doc())
                        .defaultValue(originalSchema.defaultValue())
                        .parameters(parameters)
                        ;
            case STRING:
                return SchemaBuilder.
                        string()
                        .optional()
                        .name(schemaName)
                        .doc(originalSchema.doc())
                        .defaultValue(originalSchema.defaultValue())
                        .parameters(parameters)
                        ;
            case BYTES:
                return SchemaBuilder
                        .bytes()
                        .optional()
                        .name(schemaName)
                        .doc(originalSchema.doc())
                        .defaultValue(originalSchema.defaultValue())
                        .parameters(parameters)
                        ;
            case STRUCT:
                SchemaBuilder schemaBuilder = SchemaBuilder
                        .struct()
                        .optional()
                        .name(schemaName)
                        .doc(originalSchema.doc())
                        .defaultValue(originalSchema.defaultValue())
                        .parameters(parameters);
                convertStructSchema(schemaBuilder, originalSchema);
                return schemaBuilder;
            case ARRAY:
                return SchemaBuilder.array(convertKafkaSchema(originalSchema.valueSchema()).build())
                        .optional()
                        .name(schemaName)
                        .doc(originalSchema.doc())
                        .defaultValue(originalSchema.defaultValue())
                        .parameters(parameters)
                        ;
            case MAP:
                return SchemaBuilder.map(
                        convertKafkaSchema(originalSchema.keySchema()).build(),
                        convertKafkaSchema(originalSchema.valueSchema()).build()
                ).optional()
                        .name(schemaName)
                        .doc(originalSchema.doc())
                        .defaultValue(originalSchema.defaultValue())
                        .parameters(parameters)
                        ;
            default:
                throw new RuntimeException(" Type not supported: {}" + originalSchema.type());

        }

    }

    /**
     * convert schema
     *
     * @param schemaBuilder
     * @param originalSchema
     */
    private void convertStructSchema(io.openmessaging.connector.api.data.SchemaBuilder schemaBuilder, org.apache.kafka.connect.data.Schema originalSchema) {
        for (Field field : originalSchema.fields()) {
            try {
                Schema schema = field.schema();
                org.apache.kafka.connect.data.Schema.Type type = schema.type();
                String schemaName = convertSchemaName(schema.name());
                Map<String, String> parameters = schema.parameters() == null ? new HashMap<>() : schema.parameters();
                switch (type) {
                    case INT8:
                        schemaBuilder.field(
                                field.name(),
                                SchemaBuilder
                                        .int8()
                                        .name(schemaName)
                                        .doc(schema.doc())
                                        .defaultValue(schema.defaultValue())
                                        .parameters(parameters)
                                        .optional()
                                        .build()
                        );
                        break;
                    case INT16:
                        schemaBuilder.field(
                                field.name(),
                                SchemaBuilder
                                        .int16()
                                        .name(schemaName)
                                        .doc(schema.doc())
                                        .defaultValue(schema.defaultValue())
                                        .parameters(parameters)
                                        .optional()
                                        .build()
                        );
                        break;
                    case INT32:
                        schemaBuilder.field(
                                field.name(),
                                SchemaBuilder
                                        .int32()
                                        .name(schemaName)
                                        .doc(schema.doc())
                                        .defaultValue(schema.defaultValue())
                                        .parameters(parameters)
                                        .optional()
                                        .build()
                        );

                        break;
                    case INT64:
                        schemaBuilder.field(
                                field.name(),
                                SchemaBuilder
                                        .int64()
                                        .name(schemaName)
                                        .doc(schema.doc())
                                        .defaultValue(schema.defaultValue())
                                        .parameters(parameters)
                                        .optional()
                                        .build()
                        );
                        break;
                    case FLOAT32:
                        schemaBuilder.field(
                                field.name(),
                                SchemaBuilder
                                        .float32()
                                        .name(schemaName)
                                        .doc(schema.doc())
                                        .defaultValue(schema.defaultValue())
                                        .parameters(parameters)
                                        .optional()
                                        .build()
                        );
                        break;
                    case FLOAT64:
                        schemaBuilder.field(
                                field.name(),
                                SchemaBuilder
                                        .float64()
                                        .name(schemaName)
                                        .doc(schema.doc())
                                        .defaultValue(schema.defaultValue())
                                        .parameters(parameters)
                                        .optional()
                                        .build()
                        );
                        break;
                    case BOOLEAN:
                        schemaBuilder.field(
                                field.name(),
                                SchemaBuilder
                                        .bool()
                                        .name(schemaName)
                                        .doc(schema.doc())
                                        .defaultValue(schema.defaultValue())
                                        .parameters(parameters)
                                        .optional()
                                        .build()
                        );
                        break;
                    case STRING:
                        schemaBuilder.field(
                                field.name(),
                                SchemaBuilder
                                        .string()
                                        .name(schemaName)
                                        .doc(schema.doc())
                                        .defaultValue(schema.defaultValue())
                                        .parameters(parameters)
                                        .optional()
                                        .build()
                        );
                        break;
                    case BYTES:
                        schemaBuilder.field(
                                field.name(),
                                SchemaBuilder
                                        .bytes()
                                        .name(schemaName)
                                        .doc(schema.doc())
                                        .defaultValue(schema.defaultValue())
                                        .parameters(parameters)
                                        .optional()
                                        .build()
                        );
                        break;
                    case STRUCT:
                    case ARRAY:
                    case MAP:
                        schemaBuilder.field(
                                field.name(),
                                convertKafkaSchema(field.schema()).build()
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
