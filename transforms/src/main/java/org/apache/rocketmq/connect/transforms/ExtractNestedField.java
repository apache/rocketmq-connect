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
package org.apache.rocketmq.connect.transforms;

import com.google.common.base.Strings;
import io.openmessaging.KeyValue;
import io.openmessaging.connector.api.data.ConnectRecord;
import io.openmessaging.connector.api.data.Field;
import io.openmessaging.connector.api.data.Schema;
import io.openmessaging.connector.api.data.SchemaAndValue;
import io.openmessaging.connector.api.data.SchemaBuilder;
import io.openmessaging.connector.api.data.Struct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * extract nested field
 *
 * @param <R>
 */
public abstract class ExtractNestedField<R extends ConnectRecord> extends BaseTransformation<R> {
    private static final Logger log = LoggerFactory.getLogger(ExtractNestedField.class);
    private ExtractNestedFieldConfig config;
    Map<Schema, Schema> schemaCache;

    @Override
    public void start(KeyValue keyValue) {
        this.config = new ExtractNestedFieldConfig(keyValue);
        this.schemaCache = new HashMap<>();
    }

    @Override
    protected SchemaAndValue processStruct(R record, Schema inputSchema, Struct input) {
        final Struct innerStruct = input.getStruct(this.config.outerFieldName);
        final Schema outputSchema = this.schemaCache.computeIfAbsent(inputSchema, s -> {

            final Field innerField = innerStruct.schema().getField(this.config.innerFieldName);
            final SchemaBuilder builder = SchemaBuilder.struct();
            if (!Strings.isNullOrEmpty(inputSchema.getName())) {
                builder.name(inputSchema.getName());
            }
            if (inputSchema.isOptional()) {
                builder.optional();
            }
            for (Field inputField : inputSchema.getFields()) {
                builder.field(inputField.getName(), inputField.getSchema());
            }
            builder.field(this.config.innerFieldName, innerField.getSchema());
            return builder.build();
        });
        final Struct outputStruct = new Struct(outputSchema);
        for (Field inputField : inputSchema.getFields()) {
            final Object value = input.get(inputField);
            outputStruct.put(inputField.getName(), value);
        }
        final Object innerFieldValue = innerStruct.get(this.config.innerFieldName);
        outputStruct.put(this.config.innerFieldName, innerFieldValue);

        return new SchemaAndValue(outputSchema, outputStruct);

    }

    /**
     * transform key
     */
    public static class Key extends ExtractNestedField<ConnectRecord> {
        @Override
        public ConnectRecord doTransform(ConnectRecord r) {
            final SchemaAndValue transformed = process(r, r.getKeySchema(), r.getKey());
            ConnectRecord record = new ConnectRecord(
                r.getPosition().getPartition(),
                r.getPosition().getOffset(),
                r.getTimestamp(),
                transformed.schema(),
                transformed.value(),
                r.getSchema(),
                r.getData()
            );
            record.setExtensions(r.getExtensions());
            return record;
        }
    }

    /**
     * transform value
     */
    public static class Value extends ExtractNestedField<ConnectRecord> {
        @Override
        public ConnectRecord doTransform(ConnectRecord r) {
            final SchemaAndValue transformed = process(r, r.getSchema(), r.getData());
            ConnectRecord record = new ConnectRecord(
                r.getPosition().getPartition(),
                r.getPosition().getOffset(),
                r.getTimestamp(),
                r.getKeySchema(),
                r.getKey(),
                transformed.schema(),
                transformed.value()
            );
            record.setExtensions(r.getExtensions());
            return record;
        }
    }

}
