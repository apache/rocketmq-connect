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
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * change case
 *
 * @param <R>
 */
public abstract class ChangeCase<R extends ConnectRecord> extends BaseTransformation<R> {
    private static final Logger LOGGER = LoggerFactory.getLogger(ChangeCase.class);

    class State {
        public final Map<String, String> columnMapping;
        public final Schema schema;

        State(Map<String, String> columnMapping, Schema schema) {
            this.columnMapping = columnMapping;
            this.schema = schema;
        }
    }

    Map<Schema, State> schemaState = new HashMap<>();
    private ChangeCaseConfig config;

    @Override
    public void start(KeyValue config) {
        this.config = new ChangeCaseConfig(config);
    }

    @Override
    protected SchemaAndValue processStruct(R record, Schema inputSchema, Struct input) {
        // state
        final State state = this.schemaState.computeIfAbsent(inputSchema, schema -> {
            final SchemaBuilder builder = SchemaBuilder.struct();
            if (!Strings.isNullOrEmpty(schema.getName())) {
                builder.name(schema.getName());
            }
            if (schema.isOptional()) {
                builder.optional();
            }
            final Map<String, String> columnMapping = new LinkedHashMap<>();
            for (Field field : schema.getFields()) {
                final String newFieldName = this.config.from.to(this.config.to, field.getName());
                LOGGER.trace("processStruct() - Mapped '{}' to '{}'", field.getName(), newFieldName);
                columnMapping.put(field.getName(), newFieldName);
                builder.field(newFieldName, field.getSchema());
            }
            return new State(columnMapping, builder.build());
        });

        final Struct outputStruct = new Struct(state.schema);
        for (Map.Entry<String, String> kvp : state.columnMapping.entrySet()) {
            final Object value = input.get(kvp.getKey());
            outputStruct.put(kvp.getValue(), value);
        }
        return new SchemaAndValue(state.schema, outputStruct);
    }

    /**
     * transform key
     */
    public static class Key extends ChangeCase<ConnectRecord> {
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
    public static class Value extends ChangeCase<ConnectRecord> {
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
