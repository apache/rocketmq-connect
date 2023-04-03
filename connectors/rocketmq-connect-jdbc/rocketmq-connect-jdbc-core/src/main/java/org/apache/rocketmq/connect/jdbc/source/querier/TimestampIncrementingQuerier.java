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
import org.apache.rocketmq.connect.jdbc.dialect.DatabaseDialect;
import org.apache.rocketmq.connect.jdbc.dialect.provider.CachedConnectionProvider;
import org.apache.rocketmq.connect.jdbc.schema.column.ColumnDefinition;
import org.apache.rocketmq.connect.jdbc.schema.column.ColumnId;
import org.apache.rocketmq.connect.jdbc.source.TimestampIncrementingCriteria;
import org.apache.rocketmq.connect.jdbc.source.metadata.SchemaMapping;
import org.apache.rocketmq.connect.jdbc.source.offset.SourceOffsetCompute;
import org.apache.rocketmq.connect.jdbc.source.offset.TimestampIncrementingOffset;
import org.apache.rocketmq.connect.jdbc.util.DateTimeUtils;
import org.apache.rocketmq.connect.jdbc.util.ExpressionBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

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


    public TimestampIncrementingQuerier(DatabaseDialect dialect,
                                        QueryMode mode,
                                        String name,
                                        String topicPrefix,
                                        List<String> timestampColumnNames,
                                        String incrementingColumnName,
                                        Map<String, Object> offsetMap,
                                        Long timestampDelay,
                                        TimeZone timeZone,
                                        String suffix,
                                        String offsetSuffix) {
        super(dialect, mode, name, topicPrefix, suffix, offsetSuffix);
        this.incrementingColumnName = incrementingColumnName;
        this.timestampColumnNames = timestampColumnNames != null
                ? timestampColumnNames : Collections.<String>emptyList();
        this.timestampDelay = timestampDelay;
        this.offset = TimestampIncrementingOffset.fromMap(offsetMap);

        this.timestampColumns = new ArrayList<>();
        for (String timestampColumn : this.timestampColumnNames) {
            if (timestampColumn != null && !timestampColumn.isEmpty()) {
                timestampColumns.add(new ColumnId(tableId, timestampColumn));
            }
        }

        switch (mode) {
            case TABLE:
                partition = SourceOffsetCompute.sourcePartitions(topicPrefix, tableId, this.offsetSuffix);
                break;
            case QUERY:
                partition = SourceOffsetCompute.sourceQueryPartitions(topicPrefix, this.offsetSuffix);
                break;
            default:
                throw new ConnectException("Unexpected query mode: " + mode);
        }
        this.timeZone = timeZone;
    }

    @Override
    protected void createPreparedStatement(Connection db) throws SQLException {
        findDefaultAutoIncrementingColumn(db);

        ColumnId incrementingColumn = null;
        if (incrementingColumnName != null && !incrementingColumnName.isEmpty()) {
            incrementingColumn = new ColumnId(tableId, incrementingColumnName);
        }

        ExpressionBuilder builder = dialect.expressionBuilder();
        switch (mode) {
            case TABLE:
                dialect.buildSelectTable(builder, tableId);
                break;
            case QUERY:
                builder.append(query);
                break;
            default:
                throw new ConnectException("Unknown mode encountered when preparing query: " + mode);
        }

        // Append the criteria using the columns ...
        criteria = dialect.criteriaFor(incrementingColumn, timestampColumns);
        criteria.whereClause(builder);

        String queryString = builder.toString();
        recordQuery(queryString);
        log.debug("{} prepared SQL query: {}", this, queryString);
        stmt = dialect.createPreparedStatement(db, queryString);
    }

    @Override
    public void maybeStartQuery(CachedConnectionProvider provider) throws SQLException, ConnectException {
        if (resultSet == null) {
            this.db = provider.getConnection();
            stmt = getOrCreatePreparedStatement(db);
            resultSet = executeQuery();
            ResultSetMetaData metadata = resultSet.getMetaData();
            dialect.validateColumnTypes(metadata, timestampColumns);
            schemaMapping = SchemaMapping.create(this.db, tableId, metadata, dialect);
        }
    }

    private void findDefaultAutoIncrementingColumn(Connection db) throws SQLException {
        // Default when unspecified uses an autoincrementing column
        if (incrementingColumnName != null && incrementingColumnName.isEmpty()) {
            // Find the first auto-incremented column ...
            for (ColumnDefinition defn : dialect.describeColumns(
                    db,
                    tableId.catalogName(),
                    tableId.schemaName(),
                    tableId.tableName(),
                    null).values()) {
                if (defn.isAutoIncrement()) {
                    incrementingColumnName = defn.id().name();
                    break;
                }
            }
        }
        // If still not found, query the table and use the result set metadata.
        // This doesn't work if the table is empty.
        if (incrementingColumnName != null && incrementingColumnName.isEmpty()) {
            log.debug("Falling back to describe '{}' table by querying {}", tableId, db);
            for (ColumnDefinition defn : dialect.describeColumnsByQuerying(db, tableId).values()) {
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
        for (SchemaMapping.FieldSetter setter : schemaMapping.fieldSetters()) {
            try {
                setter.setField(payload, resultSet);
            } catch (IOException e) {
                log.warn("Error mapping fields into Connect record", e);
                throw new ConnectException(e);
            } catch (SQLException e) {
                log.warn("SQL error mapping fields into Connect record", e);
                throw new SQLException(e);
            }
        }
        offset = criteria.extractValues(schemaMapping.schema(), payload, offset);
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


    @Override
    public Timestamp beginTimestampValue() {
        return offset.getTimestampOffset();
    }

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
                + "table=" + tableId
                + ", query='" + query + '\''
                + ", topicPrefix='" + topicPrefix + '\''
                + ", incrementingColumn='" + (incrementingColumnName != null
                ? incrementingColumnName
                : "") + '\''
                + ", timestampColumns=" + timestampColumnNames
                + '}';
    }

}
