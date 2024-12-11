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
 *
 * Copied from
 * https://github.com/debezium/debezium-connector-jdbc/blob/main/src/main/java/io/debezium/connector/jdbc/util/SchemaUtils.java
 * modified by doris.
 */

package org.apache.rocketmq.connect.doris.converter.type.util;

import io.openmessaging.connector.api.data.Schema;
import java.util.Objects;
import java.util.Optional;

public class SchemaUtils {
    private static final String SCHEMA_PARAMETER_COLUMN_TYPE = "__debezium.source.column.type";
    private static final String SCHEMA_PARAMETER_COLUMN_LENGTH = "__debezium.source.column.length";
    private static final String SCHEMA_PARAMETER_COLUMN_PRECISION =
        "__debezium.source.column.scale";
    private static final String SCHEMA_PARAMETER_COLUMN_NAME = "__debezium.source.column.name";

    public static Optional<String> getSourceColumnType(Schema schema) {
        return getSchemaParameter(schema, SCHEMA_PARAMETER_COLUMN_TYPE);
    }

    public static Optional<String> getSourceColumnLength(Schema schema) {
        return getSchemaParameter(schema, SCHEMA_PARAMETER_COLUMN_LENGTH);
    }

    public static Optional<String> getSourceColumnPrecision(Schema schema) {
        return getSchemaParameter(schema, SCHEMA_PARAMETER_COLUMN_PRECISION);
    }

    public static Optional<String> getSourceColumnName(Schema schema) {
        return getSchemaParameter(schema, SCHEMA_PARAMETER_COLUMN_NAME);
    }

    public static Optional<String> getSchemaParameter(Schema schema, String parameterName) {
        if (!Objects.isNull(schema.getParameters())) {
            return Optional.ofNullable(schema.getParameters().get(parameterName));
        }
        return Optional.empty();
    }
}
