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
import io.openmessaging.connector.api.data.RecordOffset;
import io.openmessaging.connector.api.data.RecordPartition;
import io.openmessaging.connector.api.data.Schema;
import io.openmessaging.connector.api.data.Struct;
import io.openmessaging.connector.api.errors.ConnectException;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import org.apache.rocketmq.connect.jdbc.common.JdbcSourceConfigConstants;
import org.apache.rocketmq.connect.jdbc.dialect.DatabaseDialect;
import org.apache.rocketmq.connect.jdbc.source.common.ColumnMapping;
import org.apache.rocketmq.connect.jdbc.source.common.QueryContext;
import org.apache.rocketmq.connect.jdbc.util.ExpressionBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * bulk mode
 */
public class BulkQuerier extends Querier {
    private static final Logger log = LoggerFactory.getLogger(BulkQuerier.class);

    public BulkQuerier(DatabaseDialect dialect, QueryContext context) {
        super(dialect, context);
    }

    @Override
    protected void createPreparedStatement(Connection db) throws SQLException {
        ExpressionBuilder builder = dialect.expressionBuilder();
        switch (context.getMode()) {
            case TABLE:
                dialect.buildSelectTable(builder, context.getTableId());
                break;
            case QUERY:
                builder.append(context.getQuerySql());
                break;
            default:
                throw new ConnectException("Unknown mode: " + context.getMode());
        }

        String queryStr = builder.toString();
        recordQuery(queryStr);
        log.debug("{} prepared SQL query: {}", this, queryStr);
        stmt = dialect.createPreparedStatement(db, queryStr, context.getBatchMaxSize());
    }

    @Override
    protected ResultSet executeQuery() throws SQLException {
        log.info("Bulk executeQuery {}", stmt);
        long begin = System.currentTimeMillis();
        ResultSet resultSet = stmt.executeQuery();
        log.info("Bulk executeQuery  cost time {}", System.currentTimeMillis() - begin);
        return resultSet;
    }

    @Override
    public ConnectRecord extractRecord() throws SQLException {
        Schema schema = schemaMapping.schema();
        Struct payload = new Struct(schema);
        for (ColumnMapping columnMapping : schemaMapping.columnMappings()) {
            try {
                Object value = jdbcColumnConverter.convertToConnectFieldValue(resultSet, columnMapping.columnDefinition(), columnMapping.columnNumber());
                payload.put(columnMapping.field(), value);
            } catch (IOException e) {
                log.warn("Error mapping fields into Connect record", e);
                throw new ConnectException(e);
            } catch (SQLException e) {
                log.warn("SQL error mapping fields into Connect record", e);
                throw new SQLException(e);
            }
        }

        final String topic;
        final Map<String, String> partition = new HashMap<>();
        switch (context.getMode()) {
            case TABLE:
                String name = context.getTableId().tableName();
                topic = context.getTopicPrefix() + name;
                partition.put(JdbcSourceConfigConstants.TABLE_NAME_KEY(context.getOffsetSuffix()), name);
                partition.put("topic", topic);
                break;
            case QUERY:
                partition.put(JdbcSourceConfigConstants.QUERY_NAME_KEY(context.getOffsetSuffix()),
                    JdbcSourceConfigConstants.QUERY_NAME_VALUE);
                topic = context.getTopicPrefix();
                partition.put("topic", topic);
                break;
            default:
                throw new ConnectException("Unexpected query mode: " + context.getMode());
        }
        // build record
        ConnectRecord record = new ConnectRecord(
                new RecordPartition(partition),
                new RecordOffset(new HashMap<>()),
                System.currentTimeMillis(),
                schema,
                payload
        );
        return record;
    }

    @Override
    public String toString() {
        return "BulkTableQuerier{" + "table='" + context.getMode() + '\'' + ", query='" + context.getQuerySql() + '\''
            + ", topicPrefix='" + context.getTopicPrefix() + '\'' + '}';
    }

}
