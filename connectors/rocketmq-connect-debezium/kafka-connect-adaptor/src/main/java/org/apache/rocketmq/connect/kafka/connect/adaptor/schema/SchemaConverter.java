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
import io.openmessaging.connector.api.errors.ConnectException;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * schema  converter
 */
public class SchemaConverter {
    private static Logger logger = LoggerFactory.getLogger(SchemaConverter.class);

    private SchemaBuilder builder;

    public SchemaConverter(SourceRecord record) {
        builder = convertKafkaSchema(record.valueSchema());
    }

    public SchemaBuilder schemaBuilder() {
        return builder;
    }

    private SchemaBuilder convertKafkaSchema(org.apache.kafka.connect.data.Schema originalSchema) {
        switch (originalSchema.type()) {
            case INT8:
                return SchemaBuilder.int8().optional().name(originalSchema.name());
            case INT16:
                return SchemaBuilder.int16().optional().name(originalSchema.name());
            case INT32:
                return SchemaBuilder.int32().optional().name(originalSchema.name());
            case INT64:
                return SchemaBuilder.int64().optional().name(originalSchema.name());
            case FLOAT32:
                return SchemaBuilder.float32().optional().name(originalSchema.name());
            case FLOAT64:
                return SchemaBuilder.float64().optional().name(originalSchema.name());
            case BOOLEAN:
                return SchemaBuilder.bool().optional().name(originalSchema.name());
            case STRING:
                return SchemaBuilder.string().optional().name(originalSchema.name());
            case BYTES:
                return SchemaBuilder.bytes().optional().name(originalSchema.name());
            case STRUCT:
                SchemaBuilder schemaBuilder = SchemaBuilder.struct().optional().optional().name(originalSchema.name());
                convertStructSchema(schemaBuilder, originalSchema);
                return schemaBuilder;
            case ARRAY:
                return SchemaBuilder.array(convertKafkaSchema(originalSchema.valueSchema()).build()).optional().name(originalSchema.name());
            case MAP:
                return SchemaBuilder.map(
                        convertKafkaSchema(originalSchema.keySchema()).build(),
                        convertKafkaSchema(originalSchema.valueSchema()).build()
                ).optional().name(originalSchema.name());
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
                org.apache.kafka.connect.data.Schema.Type type = field.schema().type();
                String schemaName =  field.schema().name();
                switch (type) {
                    case INT8:
                        schemaBuilder.field(field.name(), SchemaBuilder.int8().name(schemaName).optional().build());
                        break;
                    case INT16:
                        schemaBuilder.field(field.name(), SchemaBuilder.int16().name(schemaName).optional().build());
                        break;
                    case INT32:
                        schemaBuilder.field(field.name(), SchemaBuilder.int32().name(schemaName).optional().build());
                        break;
                    case INT64:
                        schemaBuilder.field(field.name(), SchemaBuilder.int64().name(schemaName).optional().build());
                        break;
                    case FLOAT32:
                        schemaBuilder.field(field.name(), SchemaBuilder.float32().name(schemaName).optional().build());
                        break;
                    case FLOAT64:
                        schemaBuilder.field(field.name(), SchemaBuilder.float64().name(schemaName).optional().build());
                        break;
                    case BOOLEAN:
                        schemaBuilder.field(field.name(), SchemaBuilder.bool().name(schemaName).optional().build());
                        break;
                    case STRING:
                        schemaBuilder.field(field.name(), SchemaBuilder.string().name(schemaName).optional().build());
                        break;
                    case BYTES:
                        schemaBuilder.field(field.name(), SchemaBuilder.bytes().name(schemaName).optional().build());
                        break;
                    case STRUCT:
                    case ARRAY:
                    case MAP:
                        schemaBuilder.field(field.name(), convertKafkaSchema(field.schema()).build());
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

}
