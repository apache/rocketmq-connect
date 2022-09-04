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
import org.apache.rocketmq.connect.doris.connector.DorisSinkConfig;
import org.apache.rocketmq.connect.doris.exception.TableAlterOrCreateException;
import org.apache.rocketmq.connect.doris.schema.db.DbStructure;
import org.apache.rocketmq.connect.doris.schema.table.TableId;
//import org.apache.rocketmq.connect.doris.common.HeaderField;
//import org.apache.rocketmq.connect.doris.connector.JdbcSinkConfig;
//import org.apache.rocketmq.connect.doris.dialect.DatabaseDialect;
//import org.apache.rocketmq.connect.doris.dialect.provider.CachedConnectionProvider;
//import org.apache.rocketmq.connect.doris.exception.TableAlterOrCreateException;
//import org.apache.rocketmq.connect.doris.schema.db.DbStructure;
//import org.apache.rocketmq.connect.doris.schema.table.TableId;
import org.apache.rocketmq.connect.doris.util.TableUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * jdbc db updater
 */
public class Updater {

    private static final Logger log = LoggerFactory.getLogger(Updater.class);
    private final DorisSinkConfig config;
//    private final DatabaseDialect dbDialect;
    private final DbStructure dbStructure;
//    final CachedConnectionProvider cachedConnectionProvider;

    public Updater(final DorisSinkConfig config) {
        this.config = config;
//        this.dbDialect = dbDialect;
//        this.dbStructure = dbStructure;

//        this.cachedConnectionProvider = connectionProvider(
//                config.getAttempts(),
//                config.getRetryBackoffMs()
//        );
        this.dbStructure = null;
    }

//    protected CachedConnectionProvider connectionProvider(int maxConnAttempts, long retryBackoff) {
//        return new CachedConnectionProvider(this.dbDialect, maxConnAttempts, retryBackoff) {
//            @Override
//            protected void onConnect(final Connection connection) throws SQLException {
//                connection.setAutoCommit(false);
//            }
//        };
//    }

    public void write(final Collection<ConnectRecord> records)
            throws SQLException, TableAlterOrCreateException {
//        final Connection connection = cachedConnectionProvider.getConnection();
        try {
            final Map<TableId, BufferedRecords> bufferByTable = new HashMap<>();
            for (ConnectRecord record : records) {
                // destination table
                final TableId tableId = TableUtil.destinationTable(record);
//                if (!config.filterWhiteTable(tableId)) {
//                    continue;
//                }
                BufferedRecords buffer = bufferByTable.get(tableId);
                if (buffer == null) {
                    buffer = new BufferedRecords(config, tableId, dbStructure);
                    bufferByTable.put(tableId, buffer);
                }
                buffer.add(record);
            }
            for (Map.Entry<TableId, BufferedRecords> entry : bufferByTable.entrySet()) {
                TableId tableId = entry.getKey();
                BufferedRecords buffer = entry.getValue();
                log.debug("Flushing records in JDBC Writer for table ID: {}", tableId);
                buffer.flush();
//                buffer.close();
            }
//            connection.commit();
        } catch (SQLException | TableAlterOrCreateException e) {
//            connection.rollback();
        }
    }

//    public void closeQuietly() {
//        cachedConnectionProvider.close();
//    }

}

