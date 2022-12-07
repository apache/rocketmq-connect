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
import org.apache.rocketmq.connect.jdbc.common.HeaderField;
import org.apache.rocketmq.connect.jdbc.connector.JdbcSinkConfig;
import org.apache.rocketmq.connect.jdbc.dialect.DatabaseDialect;
import org.apache.rocketmq.connect.jdbc.dialect.provider.CachedConnectionProvider;
import org.apache.rocketmq.connect.jdbc.exception.TableAlterOrCreateException;
import org.apache.rocketmq.connect.jdbc.schema.db.DbStructure;
import org.apache.rocketmq.connect.jdbc.schema.table.TableId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * jdbc db updater
 */
public class Updater {

    private static final Logger log = LoggerFactory.getLogger(Updater.class);
    private final JdbcSinkConfig config;
    private final DatabaseDialect dbDialect;
    private final DbStructure dbStructure;
    final CachedConnectionProvider cachedConnectionProvider;

    public Updater(final JdbcSinkConfig config, DatabaseDialect dbDialect, DbStructure dbStructure) {
        this.config = config;
        this.dbDialect = dbDialect;
        this.dbStructure = dbStructure;

        this.cachedConnectionProvider = connectionProvider(
                config.getAttempts(),
                config.getRetryBackoffMs()
        );
    }

    protected CachedConnectionProvider connectionProvider(int maxConnAttempts, long retryBackoff) {
        return new CachedConnectionProvider(this.dbDialect, maxConnAttempts, retryBackoff) {
            @Override
            protected void onConnect(final Connection connection) throws SQLException {
                connection.setAutoCommit(false);
            }
        };
    }

    public void write(final Collection<ConnectRecord> records)
            throws SQLException, TableAlterOrCreateException {
        final Connection connection = cachedConnectionProvider.getConnection();
        try {
            final Map<TableId, BufferedRecords> bufferByTable = new HashMap<>();
            for (ConnectRecord record : records) {
                // destination table
                final TableId tableId = destinationTable(record);
                if (!config.filterWhiteTable(dbDialect, tableId)) {
                    continue;
                }
                BufferedRecords buffer = bufferByTable.get(tableId);
                if (buffer == null) {
                    buffer = new BufferedRecords(config, tableId, dbDialect, dbStructure, connection);
                    bufferByTable.put(tableId, buffer);
                }
                buffer.add(record);
            }
            for (Map.Entry<TableId, BufferedRecords> entry : bufferByTable.entrySet()) {
                TableId tableId = entry.getKey();
                BufferedRecords buffer = entry.getValue();
                log.debug("Flushing records in JDBC Writer for table ID: {}", tableId);
                buffer.flush();
                buffer.close();
            }
            connection.commit();
        } catch (SQLException | TableAlterOrCreateException e) {
            log.error("Jdbc writer error {}", e);
            connection.rollback();
        }
    }

    public void closeQuietly() {
        cachedConnectionProvider.close();
    }

    TableId destinationTable(ConnectRecord record) {
        // todo table from header
        if (config.isTableFromHeader()){
            return dbDialect.parseToTableId(record.getExtensions().getString(HeaderField.__source_table_key));
        }
        return dbDialect.parseToTableId(record.getSchema().getName());
    }
}
