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

package org.apache.rocketmq.connect.transforms.util;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import io.openmessaging.connector.api.data.FieldType;
import io.openmessaging.connector.api.data.Schema;
import io.openmessaging.connector.api.data.SchemaBuilder;
import io.openmessaging.connector.api.data.logical.Decimal;
import io.openmessaging.connector.api.data.logical.Timestamp;

import java.math.BigDecimal;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class SchemaHelper {
    static final Map<Class<?>, FieldType> PRIMITIVES;

    public SchemaHelper() {
    }

    public static Schema schema(Object input) {
        return builder(input).build();
    }

    public static SchemaBuilder builder(Object input) {
        Preconditions.checkNotNull(input, "input cannot be null.");
        SchemaBuilder builder;
        if (PRIMITIVES.containsKey(input.getClass())) {
            FieldType type = PRIMITIVES.get(input.getClass());
            builder = new SchemaBuilder(type);
        } else if (input instanceof Date) {
            builder = Timestamp.builder();
        } else {
            if (!(input instanceof BigDecimal)) {
                throw new UnsupportedOperationException(String.format("Unsupported Type: %s", input.getClass()));
            }
            builder = Decimal.builder(((BigDecimal) input).scale());
        }

        return builder.optional();
    }

    static {
        Map<Class<?>, FieldType> primitives = new HashMap();
        primitives.put(String.class, FieldType.STRING);
        primitives.put(Boolean.class, FieldType.BOOLEAN);
        primitives.put(Byte.class, FieldType.INT8);
        primitives.put(Short.class, FieldType.INT16);
        primitives.put(Integer.class, FieldType.INT32);
        primitives.put(Long.class, FieldType.INT64);
        primitives.put(Float.class, FieldType.FLOAT32);
        primitives.put(Double.class, FieldType.FLOAT64);
        primitives.put(byte[].class, FieldType.BYTES);
        PRIMITIVES = ImmutableMap.copyOf(primitives);
    }
}
