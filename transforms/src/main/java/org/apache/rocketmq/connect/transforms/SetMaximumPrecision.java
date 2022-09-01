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

import io.openmessaging.KeyValue;
import io.openmessaging.connector.api.data.ConnectRecord;
import io.openmessaging.connector.api.data.Field;
import io.openmessaging.connector.api.data.Schema;
import io.openmessaging.connector.api.data.SchemaAndValue;
import io.openmessaging.connector.api.data.SchemaBuilder;
import io.openmessaging.connector.api.data.Struct;
import io.openmessaging.connector.api.data.logical.Decimal;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * set maximum precision
 *
 * @param <R>
 */
public abstract class SetMaximumPrecision<R extends ConnectRecord> extends BaseTransformation<R> {
    private static final Logger log = LoggerFactory.getLogger(SetMaximumPrecision.class);

    SetMaximumPrecisionConfig config;

    @Override
    public void start(KeyValue keyValue) {
        config = new SetMaximumPrecisionConfig(keyValue);
    }

    static final State NOOP = new State(true, null, null);

    static class State {
        public final boolean noop;
        public final Schema outputSchema;
        public final Set<String> decimalFields;

        State(boolean noop, Schema outputSchema, Set<String> decimalFields) {
            this.noop = noop;
            this.outputSchema = outputSchema;
            this.decimalFields = decimalFields;
        }
    }

    Map<Schema, State> schemaLookup = new HashMap<>();

    public static final String CONNECT_AVRO_DECIMAL_PRECISION_PROP = "connect.decimal.precision";

    State state(Schema inputSchema) {
        return this.schemaLookup.computeIfAbsent(inputSchema, new Function<Schema, State>() {
            @Override
            public State apply(Schema schema) {
                Set<String> decimalFields = inputSchema.getFields().stream()
                    .filter(f -> Decimal.LOGICAL_NAME.equals(f.getSchema().getName()))
                    .filter(f -> Integer.parseInt(f.getSchema().getParameters().getOrDefault(CONNECT_AVRO_DECIMAL_PRECISION_PROP, "64")) > config.maxPrecision())
                    .map(Field::getName)
                    .collect(Collectors.toSet());
                State result;

                if (decimalFields.size() == 0) {
                    result = NOOP;
                } else {
                    log.trace("state() - processing schema '{}'", schema.getName());
                    SchemaBuilder builder = SchemaBuilder.struct()
                        .name(inputSchema.getName())
                        .doc(inputSchema.getDoc())
                        .version(inputSchema.getVersion());
                    if (null != inputSchema.getParameters() && !inputSchema.getParameters().isEmpty()) {
                        builder.parameters(inputSchema.getParameters());
                    }

                    for (Field field : inputSchema.getFields()) {
                        log.trace("state() - processing field '{}'", field.getName());
                        if (decimalFields.contains(field.getName())) {
                            Map<String, String> parameters = new LinkedHashMap<>();
                            if (null != field.getSchema().getParameters() && !field.getSchema().getParameters().isEmpty()) {
                                parameters.putAll(field.getSchema().getParameters());
                            }
                            parameters.put(CONNECT_AVRO_DECIMAL_PRECISION_PROP, Integer.toString(config.maxPrecision()));
                            int scale = Integer.parseInt(parameters.get(Decimal.SCALE_FIELD));
                            SchemaBuilder fieldBuilder = Decimal.builder(scale)
                                .parameters(parameters)
                                .doc(field.getSchema().getDoc())
                                .version(field.getSchema().getVersion());
                            if (field.getSchema().isOptional()) {
                                fieldBuilder.optional();
                            }
                            Schema fieldSchema = fieldBuilder.build();
                            builder.field(field.getName(), fieldSchema);
                        } else {
                            log.trace("state() - copying field '{}' to new schema.", field.getName());
                            builder.field(field.getName(), field.getSchema());
                        }
                    }

                    Schema outputSchema = builder.build();
                    result = new State(false, outputSchema, decimalFields);
                }

                return result;
            }
        });

    }

    @Override
    protected SchemaAndValue processStruct(R record, Schema inputSchema, Struct input) {
        State state = state(inputSchema);
        SchemaAndValue result;

        if (state.noop) {
            result = new SchemaAndValue(inputSchema, input);
        } else {
            Struct struct = new Struct(state.outputSchema);
            for (Field field : inputSchema.getFields()) {
                struct.put(field.getName(), input.get(field.getName()));
            }
            result = new SchemaAndValue(state.outputSchema, struct);
        }
        return result;
    }

    /**
     * transform key
     */
    public static class Key extends SetMaximumPrecision<ConnectRecord> {
        @Override
        public ConnectRecord doTransform(ConnectRecord r) {
            SchemaAndValue transformed = this.process(r, r.getKeySchema(), r.getKey());
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
    public static class Value extends SetMaximumPrecision<ConnectRecord> {
        @Override
        public ConnectRecord doTransform(ConnectRecord r) {
            SchemaAndValue transformed = this.process(r, r.getSchema(), r.getData());
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


