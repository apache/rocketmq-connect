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

import io.openmessaging.connector.api.data.Schema;
import io.openmessaging.connector.api.data.SchemaBuilder;

/**
 * schema util
 */
public class SchemaUtil {
    public static final Schema INT8_SCHEMA = SchemaBuilder.int8().build();
    public static final Schema INT16_SCHEMA = SchemaBuilder.int16().build();
    public static final Schema INT32_SCHEMA = SchemaBuilder.int32().build();
    public static final Schema INT64_SCHEMA = SchemaBuilder.int64().build();
    public static final Schema FLOAT32_SCHEMA = SchemaBuilder.float32().build();
    public static final Schema FLOAT64_SCHEMA = SchemaBuilder.float64().build();
    public static final Schema BOOLEAN_SCHEMA = SchemaBuilder.bool().build();
    public static final Schema STRING_SCHEMA = SchemaBuilder.string().build();
    public static final Schema BYTES_SCHEMA = SchemaBuilder.bytes().build();

    public static final Schema OPTIONAL_INT8_SCHEMA = SchemaBuilder.int8().optional().build();
    public static final Schema OPTIONAL_INT16_SCHEMA = SchemaBuilder.int16().optional().build();
    public static final Schema OPTIONAL_INT32_SCHEMA = SchemaBuilder.int32().optional().build();
    public static final Schema OPTIONAL_INT64_SCHEMA = SchemaBuilder.int64().optional().build();
    public static final Schema OPTIONAL_FLOAT32_SCHEMA = SchemaBuilder.float32().optional().build();
    public static final Schema OPTIONAL_FLOAT64_SCHEMA = SchemaBuilder.float64().optional().build();
    public static final Schema OPTIONAL_BOOLEAN_SCHEMA = SchemaBuilder.bool().optional().build();
    public static final Schema OPTIONAL_STRING_SCHEMA = SchemaBuilder.string().optional().build();
    public static final Schema OPTIONAL_BYTES_SCHEMA = SchemaBuilder.bytes().optional().build();
}
