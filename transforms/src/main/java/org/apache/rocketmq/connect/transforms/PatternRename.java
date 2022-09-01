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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.regex.Matcher;

/**
 * pattern rename
 *
 * @param <R>
 */
public abstract class PatternRename<R extends ConnectRecord> extends BaseTransformation<R> {
    private static final Logger log = LoggerFactory.getLogger(PatternRename.class);
    PatternRenameConfig config;

    @Override
    public void start(KeyValue keyValue) {
        config = new PatternRenameConfig(keyValue);
    }

    @Override
    protected SchemaAndValue processStruct(R record, Schema inputSchema, Struct inputStruct) {
        final SchemaBuilder outputSchemaBuilder = SchemaBuilder.struct();
        outputSchemaBuilder.name(inputSchema.getName());
        outputSchemaBuilder.doc(inputSchema.getDoc());
        if (null != inputSchema.getDefaultValue()) {
            outputSchemaBuilder.defaultValue(inputSchema.getDefaultValue());
        }
        if (null != inputSchema.getParameters() && !inputSchema.getParameters().isEmpty()) {
            outputSchemaBuilder.parameters(inputSchema.getParameters());
        }
        if (inputSchema.isOptional()) {
            outputSchemaBuilder.optional();
        }
        Map<String, String> fieldMappings = new HashMap<>(inputSchema.getFields().size());
        for (final Field inputField : inputSchema.getFields()) {
            log.trace("process() - Processing field '{}'", inputField.getName());
            final Matcher fieldMatcher = this.config.pattern.matcher(inputField.getName());
            final String outputFieldName;
            if (fieldMatcher.find()) {
                // replace
                outputFieldName = fieldMatcher.replaceAll(this.config.replacement);
            } else {
                outputFieldName = inputField.getName();
            }
            log.trace("process() - Mapping field '{}' to '{}'", inputField.getName(), outputFieldName);
            fieldMappings.put(inputField.getName(), outputFieldName);
            outputSchemaBuilder.field(outputFieldName, inputField.getSchema());
        }
        final Schema outputSchema = outputSchemaBuilder.build();
        final Struct outputStruct = new Struct(outputSchema);
        for (Map.Entry<String, String> entry : fieldMappings.entrySet()) {
            final String inputField = entry.getKey(), outputField = entry.getValue();
            log.trace("process() - Copying '{}' to '{}'", inputField, outputField);
            final Object value = inputStruct.get(inputField);
            outputStruct.put(outputField, value);
        }
        return new SchemaAndValue(outputSchema, outputStruct);
    }

    @Override
    protected SchemaAndValue processMap(R record, Map<String, Object> input) {
        final Map<String, Object> outputMap = new LinkedHashMap<>(input.size());
        for (final String inputFieldName : input.keySet()) {
            log.trace("process() - Processing field '{}'", inputFieldName);
            final Matcher fieldMatcher = this.config.pattern.matcher(inputFieldName);
            final String outputFieldName;
            // replace
            if (fieldMatcher.find()) {
                outputFieldName = fieldMatcher.replaceAll(this.config.replacement);
            } else {
                outputFieldName = inputFieldName;
            }
            final Object value = input.get(inputFieldName);
            outputMap.put(outputFieldName, value);
        }
        return new SchemaAndValue(null, outputMap);
    }

    /**
     * transform key
     */
    public static class Key extends PatternRename<ConnectRecord> {
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
    public static class Value extends PatternRename<ConnectRecord> {
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
