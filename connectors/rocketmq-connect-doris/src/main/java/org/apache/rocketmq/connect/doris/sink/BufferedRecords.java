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
package org.apache.rocketmq.connect.doris.sink;

import io.openmessaging.connector.api.data.ConnectRecord;
import io.openmessaging.connector.api.data.Schema;
import io.openmessaging.connector.api.errors.ConnectException;
import org.apache.rocketmq.connect.doris.exception.DorisException;
import org.apache.rocketmq.connect.doris.schema.table.TableId;
import org.apache.rocketmq.connect.doris.connector.DorisSinkConfig;
import org.apache.rocketmq.connect.doris.schema.db.DbStructure;
import org.apache.rocketmq.connect.doris.sink.metadata.FieldsMetadata;
import org.apache.rocketmq.connect.doris.sink.metadata.SchemaPair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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
    private final DorisSinkConfig config;
    private final DbStructure dbStructure;
    private List<ConnectRecord> records = new ArrayList<>();
    private Schema keySchema;
    private Schema schema;
    private FieldsMetadata fieldsMetadata;
    private RecordValidator recordValidator;
    private List<ConnectRecord> updatePreparedRecords = new ArrayList<>();
    private List<ConnectRecord> deletePreparedRecords = new ArrayList<>();
    private boolean deletesInBatch = false;
    private DorisStreamLoader loader;

    public BufferedRecords(
            DorisSinkConfig config,
            TableId tableId,
            DbStructure dbStructure
    ) {
        this.tableId = tableId;
        this.config = config;
        this.dbStructure = dbStructure;
        this.recordValidator = RecordValidator.create(config);
        this.loader = DorisStreamLoader.create(config);
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
        boolean schemaChanged = false;
        if (!Objects.equals(keySchema, record.getKeySchema())) {
            keySchema = record.getKeySchema();
            schemaChanged = true;
        }
        if (isNull(record.getSchema())) {
            // For deletes, value and optionally value schema come in as null.
            // We don't want to treat this as a schema change if key schemas is the same
            // otherwise we flush unnecessarily.
            if (config.isDeleteEnabled()) {
                deletesInBatch = true;
            }
        } else if (Objects.equals(schema, record.getSchema())) {
            if (config.isDeleteEnabled() && deletesInBatch) {
                // flush so an insert after a delete of same record isn't lost
                flushed.addAll(flush());
            }
        } else {
            // value schema is not null and has changed. This is a real schema change.
            schema = record.getSchema();
            schemaChanged = true;
        }

        if (schemaChanged) {
            // Each batch needs to have the same schemas, so get the buffered records out
            flushed.addAll(flush());
            // re-initialize everything that depends on the record schema
            final SchemaPair schemaPair = new SchemaPair(
                    record.getKeySchema(),
                    record.getSchema(),
                    record.getExtensions()
            );
            // extract field
            fieldsMetadata = FieldsMetadata.extract(
                    tableId.tableName(),
                    config.pkMode,
                    config.getPkFields(),
                    config.getFieldsWhitelist(),
                    schemaPair
            );
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
            if (isNull(record.getData())) {
                deletePreparedRecords.add(record);
            } else {
                updatePreparedRecords.add(record);
            }
        }
        Optional<Long> totalUpdateCount = executeUpdates();
        Optional<Long> totalDeleteCount = executeDeletes();
        final long expectedCount = updateRecordCount();
        log.trace("{} records:{} resulting in totalUpdateCount:{} totalDeleteCount:{}",
                config.getInsertMode(),
                records.size(),
                totalUpdateCount,
                totalDeleteCount
        );
        if (totalUpdateCount.filter(total -> total != expectedCount).isPresent()
                && config.getInsertMode() == DorisSinkConfig.InsertMode.INSERT) {
            throw new ConnectException(
                    String.format(
                            "Update count (%d) did not sum up to total number of records inserted (%d)",
                            totalUpdateCount.get(),
                            expectedCount
                    )
            );
        }
        if (!totalUpdateCount.isPresent()) {
            log.info(
                    "{} records:{} , but no count of the number of rows it affected is available",
                    config.getInsertMode(),
                    records.size()
            );
        }

        final List<ConnectRecord> flushedRecords = records;
        records = new ArrayList<>();
        return flushedRecords;
    }

    /**
     * @return an optional count of all updated rows or an empty optional if no info is available
     */
    private Optional<Long> executeUpdates() throws DorisException {
        Optional<Long> count = Optional.empty();
        if (updatePreparedRecords.isEmpty()) {
            return count;
        }
        for (ConnectRecord record : updatePreparedRecords) {
            String jsonData = DorisDialect.convertToUpdateJsonString(record);
            try {
                log.info("[executeUpdates]" + jsonData);
                loader.loadJson(jsonData, record.getSchema().getName());
            } catch (DorisException e) {
                log.error("executeUpdates failed");
                throw e;
            } catch (Exception e) {
                throw new DorisException("doris error");
            }
            count = count.isPresent()
                    ? count.map(total -> total + 1)
                    : Optional.of(1L);
        }
        return count;
    }

    private Optional<Long> executeDeletes() throws SQLException {
        Optional<Long> totalDeleteCount = Optional.empty();
        if (deletePreparedRecords.isEmpty()) {
            return totalDeleteCount;
        }
        for (ConnectRecord record : updatePreparedRecords) {
            String jsonData = DorisDialect.convertToDeleteJsonString(record);
            try {
                log.info("[executeDelete]" + jsonData);
                loader.loadJson(jsonData, record.getSchema().getName());
            } catch (DorisException e) {
                log.error("executeDelete failed");
                throw e;
            } catch (Exception e) {
                throw new DorisException("doris error");
            }
            totalDeleteCount = totalDeleteCount.isPresent()
                    ? totalDeleteCount.map(total -> total + 1)
                    : Optional.of(1L);
        }
        return totalDeleteCount;
    }

    private long updateRecordCount() {
        return records
                .stream()
                // ignore deletes
                .filter(record -> nonNull(record.getData()) || !config.isDeleteEnabled())
                .count();
    }
}
