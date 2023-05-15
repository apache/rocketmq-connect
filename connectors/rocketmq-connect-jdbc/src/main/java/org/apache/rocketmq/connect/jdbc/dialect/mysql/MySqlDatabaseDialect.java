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
package org.apache.rocketmq.connect.jdbc.dialect.mysql;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import org.apache.rocketmq.connect.jdbc.config.AbstractConfig;
import org.apache.rocketmq.connect.jdbc.dialect.GenericDatabaseDialect;
import org.apache.rocketmq.connect.jdbc.schema.column.ColumnId;
import org.apache.rocketmq.connect.jdbc.schema.table.TableId;
import org.apache.rocketmq.connect.jdbc.sink.metadata.SinkRecordField;
import org.apache.rocketmq.connect.jdbc.util.ExpressionBuilder;
import org.apache.rocketmq.connect.jdbc.util.IdentifierRules;

/**
 * mysql database dialect
 */
public class MySqlDatabaseDialect extends GenericDatabaseDialect {
    public MySqlDatabaseDialect(AbstractConfig config) {
        super(config, new IdentifierRules(".", "`", "`"));
    }

    @Override
    public String name() {
        return "mysql";
    }

    @Override public PreparedStatement createPreparedStatement(Connection db, String query) throws SQLException {
        return createPreparedStatement(db, query, Integer.MIN_VALUE);
    }

    @Override
    public PreparedStatement createPreparedStatement(Connection db, String query,
        int batchMaxRows) throws SQLException {
        PreparedStatement stmt = db.prepareStatement(query);
        if (batchMaxRows > 0 || batchMaxRows == Integer.MIN_VALUE) {
            stmt.setFetchSize(batchMaxRows);
        }
        stmt.setFetchDirection(ResultSet.FETCH_FORWARD);
        return stmt;
    }

    /**
     * get sql type
     *
     * @param field
     * @return
     */
    @Override
    protected String getSqlType(SinkRecordField field) {
        switch (field.schemaType()) {
            case INT8:
                return "TINYINT";
            case INT32:
                return "INT";
            case INT64:
                return "BIGINT";
            case FLOAT32:
                return "FLOAT";
            case FLOAT64:
                return "DOUBLE";
            case BOOLEAN:
                return "TINYINT";
            case STRING:
                return "TEXT";
            case BYTES:
                return "VARBINARY(1024)";
            default:
                return super.getSqlType(field);
        }
    }

    @Override
    public String buildUpsertQueryStatement(
            TableId table,
            Collection<ColumnId> keyColumns,
            Collection<ColumnId> nonKeyColumns
    ) {
        //MySql doesn't support SQL 2003:merge so here how the upsert is handled
        final ExpressionBuilder.Transform<ColumnId> transform = (builder, col) -> {
            builder.appendColumnName(col.name());
            builder.append("=values(");
            builder.appendColumnName(col.name());
            builder.append(")");
        };

        ExpressionBuilder builder = expressionBuilder();
        builder.append("insert into ");
        builder.append(table);
        builder.append("(");
        builder.appendList()
                .delimitedBy(",")
                .transformedBy(ExpressionBuilder.columnNames())
                .of(keyColumns, nonKeyColumns);
        builder.append(") values(");
        builder.appendMultiple(",", "?", keyColumns.size() + nonKeyColumns.size());
        builder.append(") on duplicate key update ");
        builder.appendList()
                .delimitedBy(",")
                .transformedBy(transform)
                .of(nonKeyColumns.isEmpty() ? keyColumns : nonKeyColumns);
        return builder.toString();
    }
}
