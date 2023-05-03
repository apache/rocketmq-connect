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
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import org.apache.rocketmq.connect.jdbc.binder.JdbcRecordBinder;
import org.apache.rocketmq.connect.jdbc.dialect.DatabaseDialect;
import org.apache.rocketmq.connect.jdbc.schema.table.TableId;
import org.apache.rocketmq.connect.jdbc.sink.metadata.FieldsMetadata;
import org.apache.rocketmq.connect.jdbc.sink.metadata.SchemaPair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    private JdbcRecordBinder updateRecordBinder;
    private JdbcRecordBinder deleteRecordBinder;
    private boolean deletesInBatch = false;

    public BufferedRecords(JdbcSinkConfig config, TableId tableId, DatabaseDialect dbDialect, DbStructure dbStructure,
        Connection connection) {
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
        boolean schemaChanged = schemaChangedOrFlushedDeleteRecordIfNeed(record, flushed);

        // First record add or schema changed
        if (schemaChanged || updateRecordBinder == null) {
            flushedAndReinitializeStatement(record, flushed);
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

    private void flushedAndReinitializeStatement(ConnectRecord record,
        List<ConnectRecord> flushed) throws SQLException {
        // Each batch needs to have the same schemas, so get the buffered records out
        flushed.addAll(flush());
        // First close statement
        close();
        // re-initialize everything that depends on the record schema
        final SchemaPair schemaPair = new SchemaPair(record.getKeySchema(), record.getSchema(), record.getExtensions());
        // extract field
        fieldsMetadata = FieldsMetadata.extract(tableId.tableName(), config.pkMode, config.getPkFields(), config.getFieldsWhitelist(), schemaPair);
        // create or alter table
        dbStructure.createOrAmendIfNecessary(config, connection, tableId, fieldsMetadata);
        // build insert sql and delete sql
        final String insertSql = dbDialect.getInsertSql(config, fieldsMetadata, tableId);
        final String deleteSql = dbDialect.getDeleteSql(config, fieldsMetadata, tableId);
        log.info("{} sql: {} deleteSql: {} meta: {}", config.getInsertMode(), insertSql, deleteSql, fieldsMetadata);
        updatePreparedStatement = dbDialect.createPreparedStatement(connection, insertSql);
        updateRecordBinder = dbDialect.getJdbcRecordBinder(updatePreparedStatement, config.pkMode, schemaPair, fieldsMetadata, dbStructure.tableDefinition(connection, tableId), config.getInsertMode());
        if (config.isDeleteEnabled() && nonNull(deleteSql)) {
            deletePreparedStatement = dbDialect.createPreparedStatement(connection, deleteSql);
            deleteRecordBinder = dbDialect.getJdbcRecordBinder(deletePreparedStatement, config.pkMode, schemaPair, fieldsMetadata, dbStructure.tableDefinition(connection, tableId), config.getInsertMode());
        }
    }

    private boolean schemaChangedOrFlushedDeleteRecordIfNeed(ConnectRecord record,
        List<ConnectRecord> flushed) throws SQLException {
        boolean schemaChanged = false;
        if (!Objects.equals(keySchema, record.getKeySchema())) {
            keySchema = record.getKeySchema();
            schemaChanged = true;
        }
        if (isNull(record.getSchema())) {
            if (config.isDeleteEnabled()) {
                deletesInBatch = true;
            }
        } else if (Objects.equals(valueSchema, record.getSchema())) {
            if (config.isDeleteEnabled() && deletesInBatch) {
                flushed.addAll(flush());
            }
        } else {
            valueSchema = record.getSchema();
            schemaChanged = true;
        }
        return schemaChanged;
    }

    public List<ConnectRecord> flush() throws SQLException {
        if (records.isEmpty()) {
            log.debug("Records is empty");
            return new ArrayList<>();
        }
        log.debug("Flushing {} buffered records", records.size());
        for (ConnectRecord record : records) {
            if (isNull(record.getData()) && nonNull(deleteRecordBinder)) {
                deleteRecordBinder.bindRecord(record);
            } else {
                updateRecordBinder.bindRecord(record);
            }
        }
        Optional<Long> totalUpdateCount = dbDialect.executeUpdates(updatePreparedStatement);
        Optional<Long> totalDeleteCount = dbDialect.executeDeletes(deletePreparedStatement);

        final long expectedCount = updateRecordCount();
        log.trace("{} records:{} resulting in totalUpdateCount:{} totalDeleteCount:{}", config.getInsertMode(), records.size(), totalUpdateCount, totalDeleteCount);

        if (totalUpdateCount.filter(total -> total != expectedCount).isPresent() && config.getInsertMode() == JdbcSinkConfig.InsertMode.INSERT) {
            log.warn(String.format("Update count (%d) did not sum up to total number of records inserted (%d)", totalUpdateCount.get(), expectedCount));
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
