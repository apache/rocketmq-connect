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
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import org.apache.rocketmq.connect.jdbc.dialect.DatabaseDialect;
import org.apache.rocketmq.connect.jdbc.connection.CachedConnectionProvider;
import org.apache.rocketmq.connect.jdbc.schema.column.ColumnDefinition;
import org.apache.rocketmq.connect.jdbc.schema.column.ColumnId;
import org.apache.rocketmq.connect.jdbc.source.TimestampIncrementingCriteria;
import org.apache.rocketmq.connect.jdbc.source.common.ColumnMapping;
import org.apache.rocketmq.connect.jdbc.source.common.IncrementContext;
import org.apache.rocketmq.connect.jdbc.source.common.SchemaMapping;
import org.apache.rocketmq.connect.jdbc.source.offset.SourceOffsetCompute;
import org.apache.rocketmq.connect.jdbc.source.offset.TimestampIncrementingOffset;
import org.apache.rocketmq.connect.jdbc.util.DateTimeUtils;
import org.apache.rocketmq.connect.jdbc.util.ExpressionBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TimestampIncrementingQuerier extends Querier implements TimestampIncrementingCriteria.CriteriaValues {
    private static final Logger log = LoggerFactory.getLogger(
            TimestampIncrementingQuerier.class
    );

    private final List<String> timestampColumnNames;
    private final List<ColumnId> timestampColumns;
    private String incrementingColumnName;
    private long timestampDelay;
    private TimestampIncrementingOffset offset;
    private TimestampIncrementingCriteria criteria;
    private Map<String, String> partition;

    private final TimeZone timeZone;

    public TimestampIncrementingQuerier(DatabaseDialect dialect, IncrementContext context) {
        super(dialect, context);
        this.incrementingColumnName = context.getIncrementingColumnName();
        this.timestampColumnNames = context.getTimestampColumnNames();
        this.timestampDelay = context.getTimestampDelay();
        this.offset = TimestampIncrementingOffset.fromMap(context.getOffsetMap());
        this.timestampColumns = new ArrayList<>();
        for (String timestampColumn : this.timestampColumnNames) {
            if (timestampColumn != null && !timestampColumn.isEmpty()) {
                timestampColumns.add(new ColumnId(context.getTableId(), timestampColumn));
            }
        }
        switch (context.getMode()) {
            case TABLE:
                partition = SourceOffsetCompute.sourcePartitions(context.getTopicPrefix(), context.getTableId(), context.getOffsetSuffix());
                break;
            case QUERY:
                partition = SourceOffsetCompute.sourceQueryPartitions(context.getTopicPrefix(), context.getOffsetSuffix());
                break;
            default:
                throw new ConnectException("Unexpected query mode: " + context.getMode());
        }
        this.timeZone = context.getTimeZone();
    }

    @Override
    protected void createPreparedStatement(Connection db) throws SQLException {
        findDefaultAutoIncrementingColumn(db);
        ColumnId incrementingColumn = null;
        if (incrementingColumnName != null && !incrementingColumnName.isEmpty()) {
            incrementingColumn = new ColumnId(context.getTableId(), incrementingColumnName);
        }
        ExpressionBuilder builder = dialect.expressionBuilder();
        switch (context.getMode()) {
            case TABLE:
                dialect.buildSelectTable(builder, context.getTableId());
                break;
            case QUERY:
                builder.append(context.getQuerySql());
                break;
            default:
                throw new ConnectException("Unknown mode encountered when preparing query: " + context.getMode());
        }

        // Append the criteria using the columns ...
        criteria = dialect.criteriaFor(incrementingColumn, timestampColumns);
        criteria.whereClause(builder);

        String queryString = builder.toString();
        recordQuery(queryString);
        log.debug("{} prepared SQL query: {}", this, queryString);
        stmt = dialect.createPreparedStatement(db, queryString, context.getBatchMaxSize());
    }

    @Override
    public void maybeStartQuery(CachedConnectionProvider provider) throws SQLException, ConnectException {
        if (resultSet == null) {
            this.db = provider.getConnection();
            stmt = getOrCreatePreparedStatement(db);
            resultSet = executeQuery();
            ResultSetMetaData metadata = resultSet.getMetaData();
            dialect.validateColumnTypes(metadata, timestampColumns);
            schemaMapping = SchemaMapping.create(this.db, context.getTableId(), metadata, dialect);
        }
    }

    private void findDefaultAutoIncrementingColumn(Connection db) throws SQLException {
        // Default when unspecified uses an autoincrementing column
        if (incrementingColumnName != null && incrementingColumnName.isEmpty()) {
            // Find the first auto-incremented column ...
            for (ColumnDefinition columnDefinition : dialect.describeColumns(
                db,
                context.getTableId().catalogName(),
                context.getTableId().schemaName(),
                context.getTableId().tableName(),
                null).values()) {
                if (columnDefinition.isAutoIncrement()) {
                    incrementingColumnName = columnDefinition.id().name();
                    break;
                }
            }
        }

        if (incrementingColumnName != null && incrementingColumnName.isEmpty()) {
            log.debug("Falling back to describe '{}' table by querying {}", context.getTableId(), db);
            for (ColumnDefinition defn : dialect.describeColumnsByQuerying(db, context.getTableId()).values()) {
                if (defn.isAutoIncrement()) {
                    incrementingColumnName = defn.id().name();
                    break;
                }
            }
        }
    }

    @Override
    protected ResultSet executeQuery() throws SQLException {
        criteria.setQueryParameters(stmt, this);
        log.debug("Timestamp increment Statement to execute: {}", stmt.toString());
        long begin = System.currentTimeMillis();
        ResultSet resultSet = stmt.executeQuery();
        log.debug("Timestamp increment Statement to execute cost time = {}", System.currentTimeMillis() - begin);
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

        offset = criteria.extractValues(schema, payload, offset);
        // build record
        return new ConnectRecord(
            // offset partition
            new RecordPartition(partition),
            new RecordOffset(offset.toMap()),
            System.currentTimeMillis(),
            schema,
            payload
        );
    }

    /**
     * get begin timestamp from offset topic
     *
     * @return
     */
    @Override
    public Timestamp beginTimestampValue() {
        return offset.getTimestampOffset();
    }

    //Get end timestamp from db
    @Override
    public Timestamp endTimestampValue(Timestamp beginTime) throws SQLException {
        long endTimestamp;
        final long currentDbTime = dialect.currentTimeOnDB(
            stmt.getConnection(),
            DateTimeUtils.getTimeZoneCalendar(timeZone)
        ).getTime();
        endTimestamp = currentDbTime - timestampDelay;
        return new Timestamp(endTimestamp);
    }

    @Override
    public Long lastIncrementedValue() {
        return offset.getIncrementingOffset();
    }

    @Override
    public String toString() {
        return "TimestampIncrementingQuerier{"
            + "table=" + context.getTableId()
            + ", query='" + context.getQuerySql() + '\''
            + ", topicPrefix='" + context.getTopicPrefix() + '\''
            + ", incrementingColumn='" + (incrementingColumnName != null
            ? incrementingColumnName
            : "") + '\''
            + ", timestampColumns=" + timestampColumnNames
            + '}';
    }

}
