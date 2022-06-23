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
package org.apache.rocketmq.connect.jdbc.source.metadata;

import io.openmessaging.connector.api.data.Field;
import io.openmessaging.connector.api.data.Schema;
import io.openmessaging.connector.api.data.SchemaBuilder;
import io.openmessaging.connector.api.data.Struct;
import org.apache.rocketmq.connect.jdbc.dialect.DatabaseDialect;
import org.apache.rocketmq.connect.jdbc.schema.column.ColumnDefinition;
import org.apache.rocketmq.connect.jdbc.schema.column.ColumnId;
import org.apache.rocketmq.connect.jdbc.schema.table.TableId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * schema mapping
 */
public final class SchemaMapping {
    private static final Logger log = LoggerFactory.getLogger(SchemaMapping.class);

    public static SchemaMapping create(
            Connection conn,
            TableId tableId,
            ResultSetMetaData metadata,
            DatabaseDialect dialect
    ) throws SQLException {
        // backwards compatible
        String schemaName = tableId != null ? tableId.tableName() : null;
        // describe columns
        Map<ColumnId, ColumnDefinition> colDefins = dialect.describeColumns(conn, tableId, metadata);
        Map<String, DatabaseDialect.ColumnConverter> colConvertersByFieldName = new LinkedHashMap<>();
        SchemaBuilder builder = SchemaBuilder.struct().name(schemaName);

        int columnNumber = 0;
        for (ColumnDefinition colDefn : colDefins.values()) {
            ++columnNumber;
            String fieldName = dialect.addFieldToSchema(colDefn, builder);
            if (fieldName == null) {
                continue;
            }
            Field field = builder.field(fieldName);
            ColumnMapping mapping = new ColumnMapping(colDefn, columnNumber, field);
            DatabaseDialect.ColumnConverter converter = dialect.createColumnConverter(mapping);
            colConvertersByFieldName.put(fieldName, converter);
        }
        return new SchemaMapping(builder.build(), colConvertersByFieldName);
    }

    private final Schema schema;
    private final List<FieldSetter> fieldSetters;

    private SchemaMapping(
            Schema schema,
            Map<String, DatabaseDialect.ColumnConverter> convertersByFieldName
    ) {
        assert schema != null;
        assert convertersByFieldName != null;
        assert !convertersByFieldName.isEmpty();
        this.schema = schema;
        List<FieldSetter> fieldSetters = new ArrayList<>(convertersByFieldName.size());
        for (Map.Entry<String, DatabaseDialect.ColumnConverter> entry : convertersByFieldName.entrySet()) {
            DatabaseDialect.ColumnConverter converter = entry.getValue();
            Field field = schema.getField(entry.getKey());
            assert field != null;
            fieldSetters.add(new FieldSetter(converter, field));
        }
        this.fieldSetters = Collections.unmodifiableList(fieldSetters);
    }

    /**
     * schema
     *
     * @return
     */
    public Schema schema() {
        return schema;
    }

    /**
     * field setters
     *
     * @return
     */
    public List<FieldSetter> fieldSetters() {
        return fieldSetters;
    }

    @Override
    public String toString() {
        return "Mapping for " + schema.getName();
    }

    public static final class FieldSetter {

        private final DatabaseDialect.ColumnConverter converter;
        private final Field field;

        private FieldSetter(
                DatabaseDialect.ColumnConverter converter,
                Field field
        ) {
            this.converter = converter;
            this.field = field;
        }

        /**
         * Get the {@link Field} that this setter function sets.
         *
         * @return the field; never null
         */
        public Field field() {
            return field;
        }

        /**
         * set field
         *
         * @param payload
         * @param resultSet
         * @throws SQLException
         * @throws IOException
         */
        public void setField(
                Struct payload,
                ResultSet resultSet
        ) throws SQLException, IOException {
            Object value = this.converter.convert(resultSet);
            payload.put(field, value);
        }

        @Override
        public String toString() {
            return field.getName();
        }
    }
}
