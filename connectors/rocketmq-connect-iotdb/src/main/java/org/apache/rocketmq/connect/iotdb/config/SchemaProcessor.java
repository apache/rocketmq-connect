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

package org.apache.rocketmq.connect.iotdb.config;

import io.openmessaging.connector.api.data.Schema;
import io.openmessaging.connector.api.data.SchemaBuilder;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

public class SchemaProcessor {

    private SchemaProcessor() {}

    public static Schema getSchema(String fieldType) {
        if ("INT64".equals(fieldType)) {
            return SchemaBuilder.int64().build();
        } else if ("FLOAT".equals(fieldType)) {
            return SchemaBuilder.float32().build();
        } else if ("BOOLEAN".equals(fieldType)) {
            return SchemaBuilder.bool().build();
        } else if ("INT32".equals(fieldType)) {
            return SchemaBuilder.int32().build();
        } else if ("DOUBLE".equals(fieldType)) {
            return SchemaBuilder.float64().build();
        } else if ("TEXT".equals(fieldType)) {
            return SchemaBuilder.string().build();
        }
        return SchemaBuilder.string().build();
    }

    public static TSDataType getTSDataType(String fieldType) {
        if ("INT64".equals(fieldType)) {
            return TSDataType.INT64;
        } else if ("FLOAT".equals(fieldType)) {
            return TSDataType.FLOAT;
        } else if ("BOOLEAN".equals(fieldType)) {
            return TSDataType.BOOLEAN;
        } else if ("INT32".equals(fieldType)) {
            return TSDataType.INT32;
        } else if ("DOUBLE".equals(fieldType)) {
            return TSDataType.DOUBLE;
        } else if ("TEXT".equals(fieldType)) {
            return TSDataType.TEXT;
        }
        return TSDataType.TEXT;
    }
}
