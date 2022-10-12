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
package org.apache.rocketmq.connect.doris.schema.table;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

/**
 * A simple cache of {@link TableDefinition} keyed.
 */
public class TableDefinitions {

    private static final Logger log = LoggerFactory.getLogger(TableDefinitions.class);

    private final Map<TableId, TableDefinition> cache = new HashMap<>();
//    private final DatabaseDialect dialect;

    /**
     * Create an instance that uses the specified database dialect.
     *
     * @param dialect the database dialect; may not be null
     */
//    public TableDefinitions(DatabaseDialect dialect) {
//        this.dialect = dialect;
//    }

    /**
     * Get the {@link TableDefinition} for the given table.
     *
     * @param tableId    the table identifier; may not be null
     * @return the cached {@link TableDefinition}, or null if there is no such table
     * @throws SQLException if there is any problem using the connection
     */
    public TableDefinition get(
            final TableId tableId
    ) {
        return cache.get(tableId);
    }

    public boolean contains(
            final TableId tableId
    ) {
        return cache.containsKey(tableId);
    }

    /**
     * Refresh the cached {@link TableDefinition} for the given table.
     *
     * @param tableId    the table identifier; may not be null
     * @return the refreshed {@link TableDefinition}, or null if there is no such table
     * @throws SQLException if there is any problem using the connection
     */
    public TableDefinition refresh(
            TableId tableId
    ) throws SQLException {
        TableDefinition dbTable = null;
        log.info("Refreshing metadata for table {} to {}", tableId, dbTable);
        cache.put(dbTable.id(), dbTable);
        return dbTable;
    }
}
