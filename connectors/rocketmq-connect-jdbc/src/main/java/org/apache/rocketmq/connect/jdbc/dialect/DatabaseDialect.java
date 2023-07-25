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
package org.apache.rocketmq.connect.jdbc.dialect;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.rocketmq.connect.jdbc.binder.JdbcRecordBinder;
import org.apache.rocketmq.connect.jdbc.converter.JdbcColumnConverter;
import org.apache.rocketmq.connect.jdbc.connection.ConnectionProvider;
import org.apache.rocketmq.connect.jdbc.schema.column.ColumnDefinition;
import org.apache.rocketmq.connect.jdbc.schema.column.ColumnId;
import org.apache.rocketmq.connect.jdbc.schema.table.TableDefinition;
import org.apache.rocketmq.connect.jdbc.schema.table.TableId;
import org.apache.rocketmq.connect.jdbc.sink.JdbcSinkConfig;
import org.apache.rocketmq.connect.jdbc.sink.metadata.FieldsMetadata;
import org.apache.rocketmq.connect.jdbc.sink.metadata.SchemaPair;
import org.apache.rocketmq.connect.jdbc.sink.metadata.SinkRecordField;
import org.apache.rocketmq.connect.jdbc.source.TimestampIncrementingCriteria;
import org.apache.rocketmq.connect.jdbc.util.ExpressionBuilder;
import org.apache.rocketmq.connect.jdbc.util.IdentifierRules;

import static java.util.Objects.nonNull;

/**
 * database dialect
 */
public interface DatabaseDialect extends ConnectionProvider {
    /**
     * dialect name
     *
     * @return
     */
    String name();

    JdbcColumnConverter createJdbcColumnConverter();

    JdbcRecordBinder getJdbcRecordBinder(PreparedStatement statement, JdbcSinkConfig.PrimaryKeyMode pkMode,
        SchemaPair schemaPair, FieldsMetadata fieldsMetadata, TableDefinition tableDefinition,
        JdbcSinkConfig.InsertMode insertMode);

    default PreparedStatement createPreparedStatement(Connection db, String query,
        int batchMaxRows) throws SQLException {
        PreparedStatement stmt = db.prepareStatement(query);
        if (batchMaxRows > 0) {
            stmt.setFetchSize(batchMaxRows);
        }
        return stmt;
    }

    default PreparedStatement createPreparedStatement(Connection db, String query) throws SQLException {
        return createPreparedStatement(db, query, 0);
    }

    default Optional<Long> executeUpdates(PreparedStatement updatePreparedStatement) throws SQLException {
        Optional<Long> count = Optional.empty();
        if (nonNull(updatePreparedStatement)) {
            try {
                for (int updateCount : updatePreparedStatement.executeBatch()) {
                    if (updateCount != Statement.SUCCESS_NO_INFO) {
                        count = count.isPresent()
                            ? count.map(total -> total + updateCount)
                            : Optional.of((long) updateCount);
                    }
                }
            } catch (SQLException e) {
                throw new SQLException(e);
            }
        }
        return count;
    }

    default Optional<Long> executeDeletes(PreparedStatement deletePreparedStatement) throws SQLException {
        Optional<Long> totalDeleteCount = Optional.empty();
        if (nonNull(deletePreparedStatement)) {
            try {
                for (int deleteCount : deletePreparedStatement.executeBatch()) {
                    if (deleteCount != Statement.SUCCESS_NO_INFO) {
                        totalDeleteCount = totalDeleteCount.isPresent()
                            ? totalDeleteCount.map(total -> total + deleteCount)
                            : Optional.of((long) deleteCount);
                    }
                }
            } catch (SQLException e) {
                throw new SQLException(e);
            }
        }
        return totalDeleteCount;
    }

    /**
     * parse to Table Id
     *
     * @param fqn
     * @return
     */
    default TableId parseTableNameToTableId(String fqn) {
        List<String> parts = identifierRules().parseQualifiedIdentifier(fqn);
        if (parts.isEmpty()) {
            throw new IllegalArgumentException("Invalid fully qualified name: '" + fqn + "'");
        }
        if (parts.size() == 1) {
            return new TableId(null, null, parts.get(0));
        }
        if (parts.size() == 3) {
            return new TableId(parts.get(0), parts.get(1), parts.get(2));
        }
        if (useCatalog()) {
            return new TableId(parts.get(0), null, parts.get(1));
        }
        return new TableId(null, parts.get(0), parts.get(1));
    }

    default boolean useCatalog() {
        return true;
    }

    /**
     * Get the identifier rules for this database.
     *
     * @return the identifier rules
     */
    IdentifierRules identifierRules();

    ExpressionBuilder expressionBuilder();

    List<TableId> listTableIds(Connection connection) throws SQLException;

    boolean tableExists(Connection connection, TableId tableId) throws SQLException;

    TableDefinition describeTable(Connection connection, TableId tableId) throws SQLException;

    Map<ColumnId, ColumnDefinition> describeColumns(Connection connection, String tablePattern,
        String columnPattern) throws SQLException;

    Map<ColumnId, ColumnDefinition> describeColumns(Connection connection, String catalogPattern, String schemaPattern,
        String tablePattern, String columnPattern) throws SQLException;

    Map<ColumnId, ColumnDefinition> describeColumns(Connection conn, TableId tableId,
        ResultSetMetaData rsMetadata) throws SQLException;

    Map<ColumnId, ColumnDefinition> describeColumnsByQuerying(Connection connection,
        TableId tableId) throws SQLException;

    default void executeSchemaChangeStatements(Connection connection, List<String> statements) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            for (String ddl : statements) {
                statement.executeUpdate(ddl);
            }
        }
    }

    // Insert statement
    String getInsertSql(JdbcSinkConfig config, FieldsMetadata fieldsMetadata, TableId tableId);

    default String buildInsertStatement(TableId table, Collection<ColumnId> keyColumns,
        Collection<ColumnId> nonKeyColumns) {
        ExpressionBuilder builder = expressionBuilder();
        builder.append("INSERT INTO ");
        builder.append(table);
        builder.append("(");
        builder.appendList()
            .delimitedBy(",")
            .transformedBy(ExpressionBuilder.columnNames())
            .of(keyColumns, nonKeyColumns);
        builder.append(") VALUES(");
        builder.appendMultiple(",", "?", keyColumns.size() + nonKeyColumns.size());
        builder.append(")");
        return builder.toString();
    }

    default String buildUpdateStatement(TableId table, Collection<ColumnId> keyColumns,
        Collection<ColumnId> nonKeyColumns) {
        ExpressionBuilder builder = expressionBuilder();
        builder.append("UPDATE ");
        builder.append(table);
        builder.append(" SET ");
        builder.appendList()
            .delimitedBy(", ")
            .transformedBy(ExpressionBuilder.columnNamesWith(" = ?"))
            .of(nonKeyColumns);
        if (!keyColumns.isEmpty()) {
            builder.append(" WHERE ");
            builder.appendList()
                .delimitedBy(" AND ")
                .transformedBy(ExpressionBuilder.columnNamesWith(" = ?"))
                .of(keyColumns);
        }
        return builder.toString();
    }

    default String buildUpsertQueryStatement(TableId table, Collection<ColumnId> keyColumns,
        Collection<ColumnId> nonKeyColumns) {
        throw new UnsupportedOperationException();
    }

    // build delete statement
    String getDeleteSql(JdbcSinkConfig config, FieldsMetadata fieldsMetadata, TableId tableId);

    default String buildDeleteStatement(TableId table, Collection<ColumnId> keyColumns) {
        ExpressionBuilder builder = expressionBuilder();
        builder.append("DELETE FROM ");
        builder.append(table);
        if (!keyColumns.isEmpty()) {
            builder.append(" WHERE ");
            builder.appendList()
                .delimitedBy(" AND ")
                .transformedBy(ExpressionBuilder.columnNamesWith(" = ?"))
                .of(keyColumns);
        }
        return builder.toString();
    }

    // drop table
    default String buildDropTableStatement(TableId table, boolean ifExists, boolean cascade) {
        ExpressionBuilder builder = expressionBuilder();
        builder.append("DROP TABLE ");
        builder.append(table);
        if (ifExists) {
            builder.append(" IF EXISTS");
        }
        if (cascade) {
            builder.append(" CASCADE");
        }
        return builder.toString();
    }

    // create table
    String buildCreateTableStatement(TableId table, Collection<SinkRecordField> fields);

    // alter table
    List<String> buildAlterTable(TableId table, Collection<SinkRecordField> fields);

    default void validateColumnTypes(ResultSetMetaData rsMetadata,
        List<ColumnId> columns) throws io.openmessaging.connector.api.errors.ConnectException {
        // do nothing
    }

    default String buildSelectTableMode() {
        return "SELECT * FROM ";
    }

    default void buildSelectTable(ExpressionBuilder builder, TableId tableId) {
        String mode = buildSelectTableMode();
        builder.append(mode).append(tableId);
    }

    TimestampIncrementingCriteria criteriaFor(ColumnId incrementingColumn, List<ColumnId> timestampColumns);

    default Long getMinTimestampValue(Connection con, String tableOrQuery,
        List<String> timestampColumns) throws SQLException {
        if (timestampColumns == null || timestampColumns.isEmpty()) {
            return null;
        }
        StringBuilder builder = new StringBuilder();
        builder.append("SELECT ");
        boolean appendComma = false;
        for (String column : timestampColumns) {
            builder.append("MIN(");
            builder.append(column);
            builder.append(")");
            if (appendComma) {
                builder.append(",");
            } else {
                appendComma = true;
            }
        }
        builder.append(" FROM ");
        builder.append(tableOrQuery);
        String querySql = builder.toString();
        PreparedStatement st = con.prepareStatement(querySql);
        ResultSet resultSet = st.executeQuery();
        ResultSetMetaData metaData = resultSet.getMetaData();
        long minTimestampValue = Long.MAX_VALUE;
        for (int i = 1; i <= metaData.getColumnCount(); ++i) {
            long t = resultSet.getLong(i);
            minTimestampValue = Math.min(minTimestampValue, t);
        }
        st.close();
        return minTimestampValue;
    }

    Timestamp currentTimeOnDB(Connection connection, Calendar cal) throws SQLException;
}