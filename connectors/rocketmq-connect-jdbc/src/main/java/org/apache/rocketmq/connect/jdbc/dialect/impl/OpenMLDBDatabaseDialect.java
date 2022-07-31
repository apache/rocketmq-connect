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
package org.apache.rocketmq.connect.jdbc.dialect.impl;

import io.openmessaging.connector.api.data.Schema;
import io.openmessaging.connector.api.data.logical.Date;
import io.openmessaging.connector.api.data.logical.Time;
import io.openmessaging.connector.api.data.logical.Timestamp;
import org.apache.rocketmq.connect.jdbc.config.AbstractConfig;
import org.apache.rocketmq.connect.jdbc.dialect.DatabaseDialect;
import org.apache.rocketmq.connect.jdbc.dialect.DropOptions;
import org.apache.rocketmq.connect.jdbc.dialect.provider.DatabaseDialectProvider;
import org.apache.rocketmq.connect.jdbc.schema.column.ColumnId;
import org.apache.rocketmq.connect.jdbc.schema.table.TableId;
import org.apache.rocketmq.connect.jdbc.sink.metadata.SinkRecordField;
import org.apache.rocketmq.connect.jdbc.util.ExpressionBuilder;
import org.apache.rocketmq.connect.jdbc.util.IdentifierRules;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.List;


/**
 * openmldb database dialect
 */
public class OpenMLDBDatabaseDialect extends GenericDatabaseDialect {

    private final Logger log = LoggerFactory.getLogger(OpenMLDBDatabaseDialect.class);

    /**
     * The provider for {@link OpenMLDBDatabaseDialect}.
     */
    public static class Provider extends DatabaseDialectProvider {
        public Provider() throws ClassNotFoundException {
            super(OpenMLDBDatabaseDialect.class.getSimpleName(), "openmldb");
            Class.forName("com._4paradigm.openmldb.jdbc.SQLDriver");
        }

        @Override
        public DatabaseDialect create(AbstractConfig config) {
            return new OpenMLDBDatabaseDialect(config);
        }
    }

    /**
     * create openMLDB database dialect
     *
     * @param config
     */
    public OpenMLDBDatabaseDialect(AbstractConfig config) {
        super(config, new IdentifierRules(".", "`", "`"));
    }


    @Override
    protected String currentTimestampDatabaseQuery() {
        return null;
    }

    @Override
    protected String getSqlType(SinkRecordField field) {
        if (field.schemaName() != null) {
            String schema = field.schemaName();
            switch (schema) {
                case Timestamp.LOGICAL_NAME:
                    return "TIMESTAMP";

                case Date.LOGICAL_NAME:
                case Time.LOGICAL_NAME:
                    return "DATE";
            }
        }

        switch (field.schemaType()) {
            case INT32:
                return "INT";
            case INT64:
                return "BIGINT";
            case FLOAT32:
                return "FLOAT";
            case FLOAT64:
                return "DOUBLE";
            case BOOLEAN:
                return "BOOL";
            case STRING:
                return "VARCHAR";
            default:
                return super.getSqlType(field);
        }
    }

    @Override
    public String buildCreateTableStatement(TableId table, Collection<SinkRecordField> fields) {
        List<String> pkFieldNames = this.extractPrimaryKeyFieldNames(fields);
        if (!pkFieldNames.isEmpty()) {
            throw new UnsupportedOperationException("pk is unsupported in openmldb");
        } else {
            return super.buildCreateTableStatement(table, fields);
        }
    }

    @Override
    protected void writeColumnSpec(ExpressionBuilder builder, SinkRecordField f) {
        builder.appendColumnName(f.name());
        builder.append(" ");
        String sqlType = this.getSqlType(f);
        builder.append(sqlType);
        if (f.defaultValue() != null) {
            builder.append(" DEFAULT ");
            this.formatColumnValue(builder, f.schemaType(), f.defaultValue());
        } else if (!this.isColumnOptional(f)) {
            builder.append(" NOT NULL");
        }

    }

    @Override
    public String buildDropTableStatement(TableId table, DropOptions options) {
        ExpressionBuilder builder = this.expressionBuilder();
        builder.append("DROP TABLE ");
        builder.append(table);
        return builder.toString();
    }

    @Override
    public List<String> buildAlterTable(TableId table, Collection<SinkRecordField> fields) {
        throw new UnsupportedOperationException("alter is unsupported");
    }

    @Override
    public String buildUpdateStatement(TableId table, Collection<ColumnId> keyColumns, Collection<ColumnId> nonKeyColumns) {
        throw new UnsupportedOperationException("update is unsupported");
    }

    @Override
    public String buildDeleteStatement(TableId table, Collection<ColumnId> keyColumns) {
        throw new UnsupportedOperationException("delete is unsupported");
    }

    @Override
    protected Integer getSqlTypeForSchema(Schema schema) {
        return 0;
    }


    @Override
    protected String sanitizedUrl(String url) {
        // MySQL can also have "username:password@" at the beginning of the host list and
        // in parenthetical properties
        return super.sanitizedUrl(url)
                .replaceAll("(?i)([(,]password=)[^,)]*", "$1****")
                .replaceAll("(://[^:]*:)([^@]*)@", "$1****@");
    }
}
