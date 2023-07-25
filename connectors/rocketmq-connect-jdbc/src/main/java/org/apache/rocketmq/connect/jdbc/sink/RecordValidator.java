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
package org.apache.rocketmq.connect.jdbc.sink;

import io.openmessaging.connector.api.data.ConnectRecord;
import io.openmessaging.connector.api.data.FieldType;
import io.openmessaging.connector.api.data.Schema;
import io.openmessaging.connector.api.errors.ConnectException;

@FunctionalInterface
public interface RecordValidator {

    RecordValidator NO_OP = (record) -> { };

    static RecordValidator create(JdbcSinkConfig config) {
        RecordValidator requiresKey = requiresKey(config);
        RecordValidator requiresValue = requiresValue(config);

        RecordValidator keyValidator = NO_OP;
        RecordValidator valueValidator = NO_OP;
        switch (config.pkMode) {
            case RECORD_KEY:
                keyValidator = keyValidator.and(requiresKey);
                break;
            case RECORD_VALUE:
            case NONE:
                valueValidator = valueValidator.and(requiresValue);
                break;
            default:
                // no primary key is required
                break;
        }

        if (config.isDeleteEnabled()) {
            // When delete is enabled, we need a key
            keyValidator = keyValidator.and(requiresKey);
        } else {
            // When delete is disabled, we need non-tombstone values
            valueValidator = valueValidator.and(requiresValue);
        }

        // Compose the validator that may or may be NO_OP
        return keyValidator.and(valueValidator);
    }

    static RecordValidator requiresValue(JdbcSinkConfig config) {
        return record -> {
            Schema valueSchema = record.getSchema();
            if (record.getData() != null
                    && valueSchema != null
                    && valueSchema.getFieldType() == FieldType.STRUCT) {
                return;
            }
            throw new ConnectException(record.toString());
        };
    }

    static RecordValidator requiresKey(JdbcSinkConfig config) {
        return record -> {
            Schema keySchema = record.getKeySchema();
            if (record.getKey() != null
                    && keySchema != null
                    && (keySchema.getFieldType() == FieldType.STRUCT || keySchema.getFieldType().isPrimitive())) {
                return;
            }
            throw new ConnectException(record.toString());
        };
    }

    void validate(ConnectRecord record);

    default RecordValidator and(RecordValidator other) {
        if (other == null || other == NO_OP || other == this) {
            return this;
        }
        if (this == NO_OP) {
            return other;
        }
        RecordValidator thisValidator = this;
        return (record) -> {
            thisValidator.validate(record);
            other.validate(record);
        };
    }
}
