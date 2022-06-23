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
package org.apache.rocketmq.connect.jdbc.source.querier;

import io.openmessaging.connector.api.data.ConnectRecord;
import org.apache.rocketmq.connect.jdbc.dialect.DatabaseDialect;
import org.apache.rocketmq.connect.jdbc.dialect.provider.CachedConnectionProvider;
import org.apache.rocketmq.connect.jdbc.schema.table.TableId;
import org.apache.rocketmq.connect.jdbc.source.metadata.SchemaMapping;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public abstract class Querier {
    public enum QueryMode {
        TABLE,
        QUERY
    }

    private final Logger log = LoggerFactory.getLogger(Querier.class);

    protected final DatabaseDialect dialect;
    protected final QueryMode mode;
    protected final String query;
    protected final String topicPrefix;
    protected final TableId tableId;
    protected final String suffix;
    protected String offsetSuffix;
    // Mutable state

    protected long lastUpdate;
    protected Connection db;
    protected PreparedStatement stmt;
    protected ResultSet resultSet;
    protected SchemaMapping schemaMapping;
    private String loggedQueryString;

    public Querier(
            DatabaseDialect dialect,
            QueryMode mode,
            String nameOrQuery,
            String topicPrefix,
            String suffix,
            String offsetSuffix
    ) {
        this.dialect = dialect;
        this.mode = mode;
        this.tableId = mode.equals(QueryMode.TABLE) ? dialect.parseToTableId(nameOrQuery) : null;
        this.query = mode.equals(QueryMode.QUERY) ? nameOrQuery : null;
        this.topicPrefix = topicPrefix;
        this.lastUpdate = 0;
        this.suffix = suffix;
        this.offsetSuffix = offsetSuffix;
    }

    public long getLastUpdate() {
        return lastUpdate;
    }

    public PreparedStatement getOrCreatePreparedStatement(Connection db) throws SQLException {
        if (stmt != null) {
            return stmt;
        }
        createPreparedStatement(db);
        return stmt;
    }

    protected abstract void createPreparedStatement(Connection db) throws SQLException;

    public boolean querying() {
        return resultSet != null;
    }

    public void maybeStartQuery(CachedConnectionProvider provider) throws SQLException {
        if (resultSet == null) {
            this.db = provider.getConnection();
            stmt = getOrCreatePreparedStatement(db);
            resultSet = executeQuery();
            schemaMapping = SchemaMapping.create(this.db, tableId, resultSet.getMetaData(), dialect);
        }
    }

    protected abstract ResultSet executeQuery() throws SQLException;

    public boolean hasNext() throws SQLException {
        return resultSet.next();
    }

    public abstract ConnectRecord extractRecord() throws SQLException;

    public void reset(long now) {
        closeResultSetQuietly();
        closeStatementQuietly();
        releaseLocksQuietly();
        // TODO: Can we cache this and quickly check that it's identical for the next query
        // instead of constructing from scratch since it's almost always the same
        schemaMapping = null;
        lastUpdate = now;
    }

    private void releaseLocksQuietly() {
        if (db != null) {
            try {
                db.commit();
            } catch (SQLException e) {
                log.warn("Error while committing read transaction, database locks may still be held", e);
            }
        }
        db = null;
    }

    private void closeStatementQuietly() {
        if (stmt != null) {
            try {
                stmt.close();
            } catch (SQLException ignored) {
                // intentionally ignored
            }
        }
        stmt = null;
    }

    private void closeResultSetQuietly() {
        if (resultSet != null) {
            try {
                resultSet.close();
            } catch (SQLException ignored) {
                // intentionally ignored
            }
        }
        resultSet = null;
    }


    protected void recordQuery(String query) {
        if (query != null && !query.equals(loggedQueryString)) {
            // For usability, log the statement at INFO level only when it changes
            log.info("Begin using SQL query: {}", query);
            loggedQueryString = query;
        }
    }

    public void setOffsetSuffix(String offsetSuffix) {
        this.offsetSuffix = offsetSuffix;
    }
}
