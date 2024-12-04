/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.rocketmq.connect.doris.converter;

import io.openmessaging.connector.api.data.ConnectRecord;
import io.openmessaging.connector.api.data.Field;
import io.openmessaging.connector.api.data.Schema;
import io.openmessaging.connector.api.data.Struct;
import io.openmessaging.connector.api.errors.ConnectException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.rocketmq.connect.doris.converter.type.Type;
import org.apache.rocketmq.connect.doris.utils.ConnectRecordUtil;

public class RecordDescriptor {
    private final ConnectRecord record;
    private final String topicName;
    private final List<String> keyFieldNames;
    private final List<String> nonKeyFieldNames;
    private final Map<String, FieldDescriptor> fields;
    private final boolean flattened;

    private RecordDescriptor(
        ConnectRecord record,
        String topicName,
        List<String> keyFieldNames,
        List<String> nonKeyFieldNames,
        Map<String, FieldDescriptor> fields,
        boolean flattened) {
        this.record = record;
        this.topicName = topicName;
        this.keyFieldNames = keyFieldNames;
        this.nonKeyFieldNames = nonKeyFieldNames;
        this.fields = fields;
        this.flattened = flattened;
    }

    public String getTopicName() {
        return topicName;
    }

    public long getQueueOffset() {
        return ConnectRecordUtil.getQueueOffset(record.getPosition().getOffset());
    }

    public List<String> getKeyFieldNames() {
        return keyFieldNames;
    }

    public List<String> getNonKeyFieldNames() {
        return nonKeyFieldNames;
    }

    public Map<String, FieldDescriptor> getFields() {
        return fields;
    }

    public boolean isDebeziumSinkRecord() {
        return !flattened;
    }

    public boolean isTombstone() {
        // Debezium TOMBSTONE has both value and valueSchema to null.
        return record.getData() == null && record.getSchema().getValueSchema() == null;
    }

    public boolean isDelete() {
        if (!isDebeziumSinkRecord()) {
            return record.getData() == null;
        } else if (record.getData() != null) {
            final Struct value = (Struct) record.getData();
            return "d".equals(value.getString("op"));
        }
        return false;
    }

    public Struct getAfterStruct() {
        if (isDebeziumSinkRecord()) {
            return ((Struct) record.getData()).getStruct("after");
        } else {
            return ((Struct) record.getData());
        }
    }

    public Struct getBeforeStruct() {
        if (isDebeziumSinkRecord()) {
            return ((Struct) record.getData()).getStruct("before");
        } else {
            return ((Struct) record.getData());
        }
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class FieldDescriptor {
        private final Schema schema;
        private final String name;
        private final Map<String, Type> typeRegistry;
        private final Type type;
        private final String typeName;
        private final String schemaName;
        private String comment;
        private String defaultValue;

        public FieldDescriptor(Schema schema, String name, Map<String, Type> typeRegistry) {
            this.schema = schema;
            this.name = name;
            this.typeRegistry = typeRegistry;
            this.schemaName = schema.getName();
            this.type =
                typeRegistry.getOrDefault(
                    schema.getName(), typeRegistry.get(schema.getFieldType().name()));
            if (this.type == null) {
                throw new IllegalArgumentException(
                    "Type not found in registry for schema: " + schema);
            }
            this.typeName = type.getTypeName(schema);
        }

        public FieldDescriptor(
            Schema schema,
            String name,
            Map<String, Type> typeRegistry,
            String comment,
            String defaultValue) {
            this(schema, name, typeRegistry);
            this.comment = comment;
            this.defaultValue = defaultValue;
        }

        public String getName() {
            return name;
        }

        public Type getType() {
            return type;
        }

        public String getTypeName() {
            return typeName;
        }

        public String getSchemaName() {
            return schemaName;
        }

        public Schema getSchema() {
            return schema;
        }

        public String getComment() {
            return comment;
        }

        public String getDefaultValue() {
            return defaultValue;
        }
    }

    public static class Builder {

        private ConnectRecord sinkRecord;
        private Map<String, Type> typeRegistry;

        // Internal build state
        private final List<String> keyFieldNames = new ArrayList<>();
        private final List<String> nonKeyFieldNames = new ArrayList<>();
        private final Map<String, FieldDescriptor> allFields = new LinkedHashMap<>();

        public Builder withSinkRecord(ConnectRecord record) {
            this.sinkRecord = record;
            return this;
        }

        public Builder withTypeRegistry(Map<String, Type> typeRegistry) {
            this.typeRegistry = typeRegistry;
            return this;
        }

        public RecordDescriptor build() {
            Objects.requireNonNull(sinkRecord, "The sink record must be provided.");

            final boolean flattened = !isTombstone(sinkRecord) && isFlattened(sinkRecord);
            readSinkRecordNonKeyData(sinkRecord, flattened);

            return new RecordDescriptor(
                sinkRecord,
                ConnectRecordUtil.getTopicName(sinkRecord.getPosition().getPartition()),
                keyFieldNames,
                nonKeyFieldNames,
                allFields,
                flattened);
        }

        private boolean isFlattened(ConnectRecord record) {
            return record.getSchema().getValueSchema().getName() == null || !record.getSchema().getValueSchema()
                .getName().contains("Envelope");
        }

        private boolean isTombstone(ConnectRecord record) {
            return record.getData() == null && record.getSchema() == null;
        }

        private void readSinkRecordNonKeyData(ConnectRecord record, boolean flattened) {
            final Schema valueSchema = record.getSchema().getValueSchema();
            if (valueSchema != null) {
                if (flattened) {
                    // In a flattened event type, it's safe to read the field names directly
                    // from the schema as this isn't a complex Debezium message type.
                    applyNonKeyFields(valueSchema);
                } else {
                    Field after = valueSchema.getField("after");
                    if (after == null) {
                        throw new ConnectException(
                            "Received an unexpected message type that does not have an 'after' Debezium block");
                    }
                    applyNonKeyFields(after.getSchema());
                }
            }
        }

        private void applyNonKeyFields(Schema schema) {
            for (Field field : schema.getFields()) {
                if (!keyFieldNames.contains(field.getName())) {
                    applyNonKeyField(field.getName(), field.getSchema());
                }
            }
        }

        private void applyNonKeyField(String name, Schema schema) {
            FieldDescriptor fieldDescriptor = new FieldDescriptor(schema, name, typeRegistry);
            nonKeyFieldNames.add(fieldDescriptor.getName());
            allFields.put(fieldDescriptor.getName(), fieldDescriptor);
        }
    }
}
