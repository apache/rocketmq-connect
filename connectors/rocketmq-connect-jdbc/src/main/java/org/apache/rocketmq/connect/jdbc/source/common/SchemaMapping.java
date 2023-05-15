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
package org.apache.rocketmq.connect.jdbc.source.common;

import io.openmessaging.connector.api.data.Field;
import io.openmessaging.connector.api.data.Schema;
import io.openmessaging.connector.api.data.SchemaBuilder;
import java.sql.Connection;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.rocketmq.connect.jdbc.converter.JdbcColumnConverter;
import org.apache.rocketmq.connect.jdbc.dialect.DatabaseDialect;
import org.apache.rocketmq.connect.jdbc.schema.column.ColumnDefinition;
import org.apache.rocketmq.connect.jdbc.schema.column.ColumnId;
import org.apache.rocketmq.connect.jdbc.schema.table.TableId;

/**
 * schema mapping
 */
public final class SchemaMapping {

    private final Schema schema;
    private final Map<String, ColumnMapping> columnMappings;

    private SchemaMapping(
            Schema schema,
        Map<String, ColumnMapping> columnMappings
    ) {
        assert schema != null;
        assert columnMappings != null;
        assert !columnMappings.isEmpty();
        this.schema = schema;
        this.columnMappings = columnMappings;
    }

    public static SchemaMapping create(
        Connection conn,
        TableId tableId,
        ResultSetMetaData metadata,
        DatabaseDialect dialect
    ) throws SQLException {
        String schemaName = tableId != null ? tableId.tableName() : null;
        // Get columns
        Map<ColumnId, ColumnDefinition> columnDefinitionMap = dialect.describeColumns(conn, tableId, metadata);
        Map<String, ColumnMapping> columnMappingMap = new LinkedHashMap<>();

        // Build schema
        SchemaBuilder builder = SchemaBuilder.struct().name(schemaName);
        JdbcColumnConverter jdbcColumnConverter = dialect.createJdbcColumnConverter();
        AtomicInteger columnNumber = new AtomicInteger(0);
        for (ColumnDefinition columnDefinition : columnDefinitionMap.values()) {
            String fieldName = jdbcColumnConverter.convertToConnectFieldSchema(columnDefinition, builder);
            if (fieldName == null) {
                continue;
            }
            Field field = builder.field(fieldName);
            ColumnMapping columnMapping = new ColumnMapping(columnDefinition, columnNumber.incrementAndGet(), field);
            columnMappingMap.put(fieldName, columnMapping);
        }
        return new SchemaMapping(builder.build(), columnMappingMap);
    }

    /**
     * schema
     *
     * @return
     */
    public Schema schema() {
        return schema;
    }

    public Collection<ColumnMapping> columnMappings() {
        return columnMappings.values();
    }

    @Override
    public String toString() {
        return "Mapping for " + schema.getName();
    }
}
