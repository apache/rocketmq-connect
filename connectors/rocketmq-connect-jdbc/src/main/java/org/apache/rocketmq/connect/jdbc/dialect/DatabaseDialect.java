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

import io.openmessaging.connector.api.data.ConnectRecord;
import io.openmessaging.connector.api.data.Schema;
import io.openmessaging.connector.api.data.SchemaBuilder;
import org.apache.rocketmq.connect.jdbc.connector.JdbcSinkConfig;
import org.apache.rocketmq.connect.jdbc.dialect.provider.ConnectionProvider;
import org.apache.rocketmq.connect.jdbc.schema.column.ColumnDefinition;
import org.apache.rocketmq.connect.jdbc.schema.column.ColumnId;
import org.apache.rocketmq.connect.jdbc.schema.table.TableDefinition;
import org.apache.rocketmq.connect.jdbc.schema.table.TableId;
import org.apache.rocketmq.connect.jdbc.sink.metadata.FieldsMetadata;
import org.apache.rocketmq.connect.jdbc.sink.metadata.SchemaPair;
import org.apache.rocketmq.connect.jdbc.sink.metadata.SinkRecordField;
import org.apache.rocketmq.connect.jdbc.source.TimestampIncrementingCriteria;
import org.apache.rocketmq.connect.jdbc.source.metadata.ColumnMapping;
import org.apache.rocketmq.connect.jdbc.util.ExpressionBuilder;
import org.apache.rocketmq.connect.jdbc.util.IdentifierRules;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Collection;
import java.util.List;
import java.util.Map;

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

    /**
     * get dialect class
     *
     * @return
     */
    default Class getDialectClass() {
        return this.getClass();
    }

    /**
     * create jdbc prepared statement
     *
     * @param connection
     * @param query
     * @return
     * @throws SQLException
     */
    PreparedStatement createPreparedStatement(Connection connection, String query) throws SQLException;

    /**
     * parse to Table Id
     *
     * @param fqn
     * @return
     */
    TableId parseToTableId(String fqn);

    /**
     * Get the identifier rules for this database.
     *
     * @return the identifier rules
     */
    IdentifierRules identifierRules();

    /**
     * Get a new expression builder that can be used to build expressions with quoted identifiers.
     *
     * @return
     */
    ExpressionBuilder expressionBuilder();

    Timestamp currentTimeOnDB(Connection connection, Calendar cal) throws SQLException;

    /**
     * Get a list of identifiers of the non-system tables in the database.
     *
     * @param connection
     * @return
     * @throws SQLException
     */
    List<TableId> tableIds(Connection connection) throws SQLException;

    /**
     * table exists
     *
     * @param connection
     * @param tableId
     * @return
     * @throws SQLException
     */
    boolean tableExists(Connection connection, TableId tableId) throws SQLException;


    /**
     * Create the definition for the columns described by the database metadata.
     */
    Map<ColumnId, ColumnDefinition> describeColumns(Connection connection, String tablePattern, String columnPattern) throws SQLException;

    /**
     * Create the definition for the columns described by the database metadata.
     */
    Map<ColumnId, ColumnDefinition> describeColumns(Connection connection, String catalogPattern, String schemaPattern, String tablePattern, String columnPattern) throws SQLException;

    /**
     * Create the definition for the columns in the result set.
     */
    Map<ColumnId, ColumnDefinition> describeColumns(Connection conn, TableId tableId, ResultSetMetaData rsMetadata) throws SQLException;

    /**
     * describe table info
     *
     * @param connection
     * @param tableId
     * @return
     * @throws SQLException
     */
    TableDefinition describeTable(Connection connection, TableId tableId) throws SQLException;

    /**
     * describe columns by query sql
     *
     * @param connection
     * @param tableId
     * @return
     * @throws SQLException
     */
    Map<ColumnId, ColumnDefinition> describeColumnsByQuerying(Connection connection, TableId tableId) throws SQLException;

    /**
     * add field to schema
     *
     * @param column
     * @return
     */
    String addFieldToSchema(ColumnDefinition column, SchemaBuilder schemaBuilder);

    /**
     * Apply the supplied DDL statements using the given connection. This gives the dialect the
     * opportunity to execute the statements with a different autocommit setting.
     */
    void applyDdlStatements(Connection connection, List<String> statements) throws SQLException;

    /**
     * build dml statement
     *
     * @param table
     * @param keyColumns
     * @param nonKeyColumns
     * @return
     */
    String buildInsertStatement(TableId table, Collection<ColumnId> keyColumns, Collection<ColumnId> nonKeyColumns);

    /**
     * update statement
     *
     * @param table
     * @param keyColumns
     * @param nonKeyColumns
     * @return
     */
    String buildUpdateStatement(TableId table, Collection<ColumnId> keyColumns, Collection<ColumnId> nonKeyColumns);

    /**
     * upsert statment
     *
     * @param table
     * @param keyColumns
     * @param nonKeyColumns
     * @return
     */
    String buildUpsertQueryStatement(TableId table, Collection<ColumnId> keyColumns, Collection<ColumnId> nonKeyColumns);

    /**
     * delete statement
     *
     * @param table
     * @param keyColumns
     * @return
     */
    default String buildDeleteStatement(TableId table, Collection<ColumnId> keyColumns) {
        throw new UnsupportedOperationException();
    }


    /**
     * build select table
     */
    String buildSelectTableMode();

    void buildSelectTable(ExpressionBuilder builder, TableId tableId);


    /**
     * drop table
     *
     * @param table
     * @param options
     * @return
     */
    String buildDropTableStatement(TableId table, DropOptions options);

    /**
     * create table
     *
     * @param table
     * @param fields
     * @return
     */
    String buildCreateTableStatement(TableId table, Collection<SinkRecordField> fields);

    /**
     * build alter table
     *
     * @param table
     * @param fields
     * @return
     */
    List<String> buildAlterTable(TableId table, Collection<SinkRecordField> fields);

    /**
     * Create a component that can bind record values into the supplied prepared statement.
     *
     * @param statement       the prepared statement
     * @param pkMode          the primary key mode; may not be null
     * @param schemaPair      the key and value schemas; may not be null
     * @param fieldsMetadata  the field metadata; may not be null
     * @param tableDefinition the table definition; may be null
     * @param insertMode      the insert mode; may not be null
     * @return the statement binder; may not be null
     */
    StatementBinder statementBinder(
            PreparedStatement statement,
            JdbcSinkConfig.PrimaryKeyMode pkMode,
            SchemaPair schemaPair,
            FieldsMetadata fieldsMetadata,
            TableDefinition tableDefinition,
            JdbcSinkConfig.InsertMode insertMode
    );

    /**
     * value column types
     *
     * @param rsMetadata
     * @param columns
     * @throws io.openmessaging.connector.api.errors.ConnectException
     */
    void validateColumnTypes(
            ResultSetMetaData rsMetadata,
            List<ColumnId> columns
    ) throws io.openmessaging.connector.api.errors.ConnectException;


    /**
     * bind field
     *
     * @param statement
     * @param index
     * @param schema
     * @param value
     * @param colDef
     * @throws SQLException
     */
    void bindField(PreparedStatement statement, int index, Schema schema, Object value, ColumnDefinition colDef) throws SQLException;

    /**
     * criteria for
     *
     * @param incrementingColumn
     * @param timestampColumns
     * @return
     */
    TimestampIncrementingCriteria criteriaFor(
            ColumnId incrementingColumn,
            List<ColumnId> timestampColumns
    );

    /**
     * get min timestamp value
     *
     * @param con
     * @param tableOrQuery
     * @param timestampColumns
     * @return
     * @throws SQLException
     */
    Long getMinTimestampValue(Connection con, String tableOrQuery, List<String> timestampColumns) throws SQLException;

    /**
     * A function to bind the values from a sink record into a prepared statement.
     */
    @FunctionalInterface
    interface StatementBinder {
        /**
         * bind record
         *
         * @param record
         * @throws SQLException
         */
        void bindRecord(ConnectRecord record) throws SQLException;
    }


    /**
     * Create a function that converts column values for the column defined by the specified mapping.
     *
     * @param mapping
     * @return
     */
    ColumnConverter createColumnConverter(ColumnMapping mapping);

    /**
     * A function that obtains a column value from the current row of the specified result set.
     */
    @FunctionalInterface
    interface ColumnConverter {
        /**
         * convert
         *
         * @param resultSet
         * @return
         * @throws SQLException
         * @throws IOException
         */
        Object convert(ResultSet resultSet) throws SQLException, IOException;
    }
}