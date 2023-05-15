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

import io.openmessaging.connector.api.data.FieldType;
import io.openmessaging.connector.api.data.Schema;


public class SinkRecordField {
    private final Schema schema;
    private final String name;
    private final boolean isPrimaryKey;

    public SinkRecordField(Schema schema, String name, boolean isPrimaryKey) {
        this.schema = schema;
        this.name = name;
        this.isPrimaryKey = isPrimaryKey;
    }

    public Schema schema() {
        return schema;
    }

    public String schemaName() {
        return schema.getName();
    }

    public FieldType schemaType() {
        return schema.getFieldType();
    }

    public String name() {
        return name;
    }

    public boolean isOptional() {
        return !isPrimaryKey;
    }

    public Object defaultValue() {
        return schema.getDefaultValue();
    }

    public boolean isPrimaryKey() {
        return isPrimaryKey;
    }


    public Object getDefaultValue(FieldType type) {
        switch (type) {
            case BOOLEAN:
            case INT8:
                return (byte) 0;
            case INT32:
                return 0;
            case INT64:
                return 0L;
            case FLOAT32:
                return 0.0f;
            case FLOAT64:
                return 0.0d;
            default:
                return null;
        }

    }

    @Override
    public String toString() {
        return "SinkRecordField{"
                + "schema=" + schema
                + ", name='" + name + '\''
                + ", isPrimaryKey=" + isPrimaryKey
                + '}';
    }
}
