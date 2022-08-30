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
import io.openmessaging.connector.api.data.FieldType;
import io.openmessaging.connector.api.data.SchemaAndValue;
import io.openmessaging.connector.api.data.Struct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * pattern filter
 *
 * @param <R>
 */
public abstract class PatternFilter<R extends ConnectRecord> extends BaseTransformation<R> {

    private static final Logger log = LoggerFactory.getLogger(PatternFilter.class);
    public Pattern pattern;
    public Set<String> fields;

    private PatternFilterConfig config;

    @Override
    public void start(KeyValue config) {
        this.config = new PatternFilterConfig(config);
        this.pattern = this.config.pattern();
        this.fields = this.config.fields();
    }

    R filter(R record, Struct struct) {
        for (Field field : struct.schema().getFields()) {
            if (!this.fields.contains(field.getName()) || field.getSchema().getFieldType() != FieldType.STRING) {
                continue;
            }
            String input = struct.getString(field.getName());
            if (null != input) {
                if (this.pattern.matcher(input).matches()) {
                    return null;
                }
            }
        }
        return record;
    }

    /**
     * filter map
     *
     * @param record
     * @param map
     * @return
     */
    R filter(R record, Map map) {
        for (Object field : map.keySet()) {
            if (!this.fields.contains(field)) {
                continue;
            }
            Object value = map.get(field);
            if (value instanceof String) {
                String input = (String) value;
                if (this.pattern.matcher(input).matches()) {
                    return null;
                }
            }
        }
        return record;
    }

    R filter(R record, final boolean key) {
        final SchemaAndValue input = key ?
            new SchemaAndValue(record.getKeySchema(), record.getKey()) :
            new SchemaAndValue(record.getSchema(), record.getData());
        final R result;
        if (input.schema() != null) {
            if (FieldType.STRUCT == input.schema().getFieldType()) {
                result = filter(record, (Struct) input.value());
            } else if (FieldType.MAP == input.schema().getFieldType()) {
                result = filter(record, (Map) input.value());
            } else {
                result = record;
            }
        } else if (input.value() instanceof Map) {
            result = filter(record, (Map) input.value());
        } else {
            result = record;
        }
        return result;
    }

    /**
     * filter key
     *
     * @param <R>
     */
    public static class Key<R extends ConnectRecord> extends PatternFilter<R> {
        @Override
        public R doTransform(R r) {
            return filter(r, true);
        }
    }

    /**
     * filter value
     *
     * @param <R>
     */
    public static class Value<R extends ConnectRecord> extends PatternFilter<R> {
        @Override
        public R doTransform(R r) {
            return filter(r, false);
        }
    }
}
