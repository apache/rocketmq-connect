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
package org.apache.rocketmq.connect.jdbc.sink;

import io.openmessaging.connector.api.data.ConnectRecord;
import io.openmessaging.connector.api.data.Schema;
import io.openmessaging.connector.api.errors.ConnectException;
import org.apache.rocketmq.connect.jdbc.dialect.DatabaseDialect;
import org.apache.rocketmq.connect.jdbc.dialect.GenericDatabaseDialect;
import org.apache.rocketmq.connect.jdbc.schema.db.DbStructure;
import org.apache.rocketmq.connect.jdbc.schema.table.TableId;
import org.apache.rocketmq.connect.jdbc.sink.metadata.FieldsMetadata;
import org.apache.rocketmq.connect.jdbc.sink.metadata.SchemaPair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;

/**
 * buffered records
 */
public class BufferedRecords {
    private static final Logger log = LoggerFactory.getLogger(BufferedRecords.class);

    private final TableId tableId;
    private final JdbcSinkConfig config;
    private final DatabaseDialect dbDialect;
    private final DbStructure dbStructure;
    private final Connection connection;
    private List<ConnectRecord> records = new ArrayList<>();
    private Schema keySchema;
    private Schema valueSchema;
    private FieldsMetadata fieldsMetadata;
    private RecordValidator recordValidator;
    private PreparedStatement updatePreparedStatement;
    private PreparedStatement deletePreparedStatement;
    private DatabaseDialect.StatementBinder updateStatementBinder;
    private DatabaseDialect.StatementBinder deleteStatementBinder;
    private boolean deletesInBatch = false;

    public BufferedRecords(JdbcSinkConfig config, TableId tableId, DatabaseDialect dbDialect, DbStructure dbStructure, Connection connection) {
        this.tableId = tableId;
        this.config = config;
        this.dbDialect = dbDialect;
        this.dbStructure = dbStructure;
        this.connection = connection;
        this.recordValidator = RecordValidator.create(config);
    }

    /**
     * add record
     *
     * @param record
     * @return
     * @throws SQLException
     */
    public List<ConnectRecord> add(ConnectRecord record) throws SQLException {
        recordValidator.validate(record);
        final List<ConnectRecord> flushed = new ArrayList<>();
        // check schema changed
        boolean schemaChanged = isSchemaChanged(record);
        //Judge and flush delete data
        flushDeletedRecord(record, flushed);

        if (schemaChanged || updateStatementBinder == null) {
            // Each batch needs to have the same schemas, so get the buffered records out
            flushed.addAll(flush());
            // re-initialize everything that depends on the record schema
            final SchemaPair schemaPair = new SchemaPair(record.getKeySchema(), record.getSchema(), record.getExtensions());
            // extract field
            fieldsMetadata = FieldsMetadata.extract(tableId.tableName(), config.pkMode, config.getPkFields(), config.getFieldsWhitelist(), schemaPair);
            // create or alter table
            dbStructure.createOrAmendIfNecessary(config, connection, tableId, fieldsMetadata);
            // build insert sql and delete sql
            final String insertSql = dbDialect.getInsertSql(config, fieldsMetadata, tableId);
            final String deleteSql = dbDialect.getDeleteSql(config, fieldsMetadata, tableId);

            log.debug("{} sql: {} deleteSql: {} meta: {}", config.getInsertMode(), insertSql, deleteSql, fieldsMetadata);
            close();
            updatePreparedStatement = dbDialect.createPreparedStatement(connection, insertSql);
            updateStatementBinder = dbDialect.statementBinder(updatePreparedStatement, config.pkMode, schemaPair, fieldsMetadata, dbStructure.tableDefinition(connection, tableId), config.getInsertMode());
            if (config.isDeleteEnabled() && nonNull(deleteSql)) {
                deletePreparedStatement = dbDialect.createPreparedStatement(connection, deleteSql);
                deleteStatementBinder = dbDialect.statementBinder(deletePreparedStatement, config.pkMode, schemaPair, fieldsMetadata, dbStructure.tableDefinition(connection, tableId), config.getInsertMode());
            }
        }
        // set deletesInBatch if schema value is not null
        if (isNull(record.getData()) && config.isDeleteEnabled()) {
            deletesInBatch = true;
        }
        records.add(record);
        if (records.size() >= config.getBatchSize()) {
            flushed.addAll(flush());
        }
        return flushed;
    }

    public List<ConnectRecord> flush() throws SQLException {
        if (records.isEmpty()) {
            log.debug("Records is empty");
            return new ArrayList<>();
        }
        log.debug("Flushing {} buffered records", records.size());
        for (ConnectRecord record : records) {
            if (isNull(record.getData()) && nonNull(deleteStatementBinder)) {
                deleteStatementBinder.bindRecord(record);
            } else {
                updateStatementBinder.bindRecord(record);
            }
        }
        Optional<Long> totalUpdateCount =
                updateStatementBinder != null ? updateStatementBinder.executeUpdates(updatePreparedStatement) :
                        Optional.empty();
        long totalDeleteCount = deleteStatementBinder != null ?
                deleteStatementBinder.executeDeletes(deletePreparedStatement) : 0;

        final long expectedCount = updateRecordCount();
        log.trace("{} records:{} resulting in totalUpdateCount:{} totalDeleteCount:{}",
                config.getInsertMode(),
                records.size(),
                totalUpdateCount,
                totalDeleteCount
        );
        if (totalUpdateCount.filter(total -> total != expectedCount).isPresent() && config.getInsertMode() == JdbcSinkConfig.InsertMode.INSERT) {
            if (dbDialect.name().equals(GenericDatabaseDialect.DialectName.generateDialectName(dbDialect.getDialectClass())) && totalUpdateCount.get() == 0) {
                // openMLDB execute success result 0; do nothing
            } else {
                throw new ConnectException(
                        String.format(
                                "Update count (%d) did not sum up to total number of records inserted (%d)",
                                totalUpdateCount.get(),
                                expectedCount
                        )
                );
            }
        }
        if (!totalUpdateCount.isPresent()) {
            log.info("{} records:{} , but no count of the number of rows it affected is available", config.getInsertMode(), records.size());
        }
        final List<ConnectRecord> flushedRecords = records;
        records = new ArrayList<>();
        return flushedRecords;
    }

    private long updateRecordCount() {
        return records
                .stream()
                // ignore deletes
                .filter(record -> nonNull(record.getData()) || !config.isDeleteEnabled())
                .count();
    }

    private boolean isSchemaChanged(ConnectRecord record) {
        boolean schemaChanged = false;
        if (!Objects.equals(keySchema, record.getKeySchema())) {
            keySchema = record.getKeySchema();
            schemaChanged = true;
        }
        if (!isNull(record.getSchema()) && !Objects.equals(valueSchema, record.getSchema())) {
            // value schema is not null and has changed. This is a real schema change.
            valueSchema = record.getSchema();
            schemaChanged = true;
        }
        return schemaChanged;
    }

    private void flushDeletedRecord(ConnectRecord record, List<ConnectRecord> flushed) throws SQLException {
        if (isNull(record.getSchema())) {
            if (config.isDeleteEnabled()) {
                deletesInBatch = true;
            }
        } else if (Objects.equals(valueSchema, record.getSchema())) {
            if (config.isDeleteEnabled() && deletesInBatch) {
                // flush so an insert after a delete of same record isn't lost
                flushed.addAll(flush());
            }
        }
    }

    public void close() throws SQLException {
        log.debug(
                "Closing BufferedRecords with updatePreparedStatement: {} deletePreparedStatement: {}",
                updatePreparedStatement,
                deletePreparedStatement
        );
        if (nonNull(updatePreparedStatement)) {
            updatePreparedStatement.close();
            updatePreparedStatement = null;
        }
        if (nonNull(deletePreparedStatement)) {
            deletePreparedStatement.close();
            deletePreparedStatement = null;
        }
    }

}
