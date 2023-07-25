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
package org.apache.rocketmq.connect.jdbc.source.offset;

import com.google.common.collect.Maps;
import io.openmessaging.connector.api.component.task.source.SourceTaskContext;
import io.openmessaging.connector.api.data.RecordOffset;
import io.openmessaging.connector.api.data.RecordPartition;
import io.openmessaging.connector.api.errors.ConnectException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Calendar;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TimeZone;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.connect.jdbc.common.JdbcSourceConfigConstants;
import org.apache.rocketmq.connect.jdbc.dialect.DatabaseDialect;
import org.apache.rocketmq.connect.jdbc.connection.CachedConnectionProvider;
import org.apache.rocketmq.connect.jdbc.schema.column.ColumnDefinition;
import org.apache.rocketmq.connect.jdbc.schema.column.ColumnId;
import org.apache.rocketmq.connect.jdbc.schema.table.TableId;
import org.apache.rocketmq.connect.jdbc.source.JdbcSourceTaskConfig;
import org.apache.rocketmq.connect.jdbc.source.common.QueryMode;
import org.apache.rocketmq.connect.jdbc.source.common.TableLoadMode;
import org.apache.rocketmq.connect.jdbc.util.ExpressionBuilder;
import org.apache.rocketmq.connect.jdbc.util.QuoteMethod;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.rocketmq.connect.jdbc.source.JdbcSourceConfig.TIMESTAMP_INITIAL_CURRENT;

/**
 * offset compute utils
 */
public class SourceOffsetCompute {

    private static final Logger log = LoggerFactory.getLogger(SourceOffsetCompute.class);
    public static final String TOPIC = "topic";

    /**
     * source partitions
     *
     * @param tableId
     * @param offsetSuffix
     * @return
     */
    public static Map<String, String> sourcePartitions(String prefix, TableId tableId, String offsetSuffix) {
        String fqn = ExpressionBuilder.create().append(tableId, QuoteMethod.NEVER).toString();
        Map<String, String> partition = new HashMap<>();
        partition.put(JdbcSourceConfigConstants.TABLE_NAME_KEY(offsetSuffix), fqn);
        if (StringUtils.isNotEmpty(prefix)) {
            partition.put(TOPIC, prefix.concat(tableId.tableName()));
        } else {
            partition.put(TOPIC, tableId.tableName());
        }
        return partition;
    }

    /**
     * source partitions
     *
     * @param offsetSuffix
     * @return
     */
    public static Map<String, String> sourceQueryPartitions(String prefix, String offsetSuffix) {
        Map<String, String> partition = Collections.singletonMap(JdbcSourceConfigConstants.QUERY_NAME_KEY(offsetSuffix),
                JdbcSourceConfigConstants.QUERY_NAME_VALUE);
        partition.put(TOPIC, prefix);
        return partition;
    }


    /**
     * init and compute offset
     *
     * @return
     */
    public static Map<String, Map<String, Object>> initOffset(
            JdbcSourceTaskConfig config,
            SourceTaskContext context,
            DatabaseDialect dialect,
            CachedConnectionProvider cachedConnectionProvider
    ) {

        List<String> tables = config.getTables();
        String query = config.getQuery();
        TableLoadMode mode = TableLoadMode.findTableLoadModeByName(config.getMode());
        QueryMode queryMode = !StringUtils.isEmpty(query) ? QueryMode.QUERY : QueryMode.TABLE;

        // step 1 -——-- compute partitions
        Map<String, RecordPartition> partitionsByTableFqn = buildTablePartitions(mode, queryMode, tables, dialect, config.getOffsetSuffix(), config.getTopicPrefix());
        // step 2 ----- get last time offset
        Map<RecordPartition, RecordOffset> offsets = null;
        if (partitionsByTableFqn != null) {
            offsets = context.offsetStorageReader().readOffsets(partitionsByTableFqn.values());
        }
        // step 3 ----- compute offset init value
        List<String> tablesOrQuery = queryMode == QueryMode.QUERY ? Collections.singletonList(query) : tables;
        return initOffsetValues(
                cachedConnectionProvider,
                dialect, queryMode,
                partitionsByTableFqn,
                offsets,
                config,
                tablesOrQuery
        );
    }

    private static Map<String, Map<String, Object>> initOffsetValues(
        CachedConnectionProvider cachedConnectionProvider,
        DatabaseDialect dialect,
        QueryMode queryMode,
        Map<String, RecordPartition> partitionsByTableFqn,
        Map<RecordPartition, RecordOffset> offsets,
        JdbcSourceTaskConfig config,
        List<String> tablesOrQuery) {

        Map<String, Map<String, Object>> offsetsValues = Maps.newHashMap();

        String incrementingColumn = config.getIncrementingColumnName();
        List<String> timestampColumns = config.getTimestampColumnNames();
        boolean validateNonNulls = config.isValidateNonNull();
        TimeZone timeZone = config.getTimeZone();

        for (String tableOrQuery : tablesOrQuery) {
            final RecordPartition tablePartitionsToCheck;
            final Map<String, String> partition;
            switch (queryMode) {
                case TABLE:
                    if (validateNonNulls) {
                        validateNonNullable(
                                config.getMode(),
                                tableOrQuery,
                                incrementingColumn,
                                timestampColumns,
                                dialect,
                                cachedConnectionProvider
                        );
                    }
                    tablePartitionsToCheck = partitionsByTableFqn.get(tableOrQuery);
                    break;
                case QUERY:
                    partition = sourceQueryPartitions(config.getTopicPrefix(), config.getOffsetSuffix());
                    tablePartitionsToCheck = new RecordPartition(partition);
                    break;
                default:
                    throw new ConnectException("Unexpected query mode: " + queryMode);
            }
            Map<String, Object> offset = null;
            if (offsets != null && tablePartitionsToCheck != null && offsets.containsKey(tablePartitionsToCheck)) {
                offset = (Map<String, Object>) offsets.get(tablePartitionsToCheck).getOffset();
            }
            offset = computeInitialOffset(
                    cachedConnectionProvider,
                    dialect,
                    queryMode,
                    tableOrQuery,
                    offset,
                    timeZone,
                    config.getTimestampInitial(),
                    config.getTimestampColumnNames()
            );
            offsetsValues.put(tableOrQuery, offset);
        }
        return offsetsValues;
    }

    private static void validateNonNullable(
            String incrementalMode,
            String table,
            String incrementingColumn,
            List<String> timestampColumns,
            DatabaseDialect dialect,
            CachedConnectionProvider connectionProvider
    ) {
        try {
            Set<String> lowercaseTsColumns = new HashSet<>();
            for (String timestampColumn : timestampColumns) {
                lowercaseTsColumns.add(timestampColumn.toLowerCase(Locale.getDefault()));
            }
            boolean incrementingOptional = false;
            boolean atLeastOneTimestampNotOptional = false;
            final Connection conn = connectionProvider.getConnection();
            boolean autoCommit = conn.getAutoCommit();
            try {
                conn.setAutoCommit(true);
                Map<ColumnId, ColumnDefinition> defnsById = dialect.describeColumns(conn, table, null);
                for (ColumnDefinition defn : defnsById.values()) {
                    String columnName = defn.id().name();
                    if (columnName.equalsIgnoreCase(incrementingColumn)) {
                        incrementingOptional = defn.isOptional();
                    } else if (lowercaseTsColumns.contains(columnName.toLowerCase(Locale.getDefault()))) {
                        if (!defn.isOptional()) {
                            atLeastOneTimestampNotOptional = true;
                        }
                    }
                }
            } finally {
                conn.setAutoCommit(autoCommit);
            }
            if ((incrementalMode.equals(TableLoadMode.MODE_INCREMENTING.getName())
                || incrementalMode.equals(TableLoadMode.MODE_TIMESTAMP_INCREMENTING.getName()))
                && incrementingOptional) {
                throw new ConnectException("Cannot make incremental queries using incrementing column "
                    + incrementingColumn + " on " + table + " because this column "
                    + "is nullable.");
            }
            if ((incrementalMode.equals(TableLoadMode.MODE_TIMESTAMP.getName())
                || incrementalMode.equals(TableLoadMode.MODE_TIMESTAMP_INCREMENTING.getName()))
                && !atLeastOneTimestampNotOptional) {
                throw new ConnectException("Cannot make incremental queries using timestamp columns "
                    + timestampColumns + " on " + table + " because all of these "
                    + "columns "
                    + "nullable.");
            }
        } catch (SQLException e) {
            throw new ConnectException("Failed trying to validate that columns used for offsets are NOT"
                    + " NULL", e);
        }
    }


    private static Map<String, Object> computeInitialOffset(
        CachedConnectionProvider cachedConnectionProvider,
        DatabaseDialect dialect,
        QueryMode queryMode,
        String tableOrQuery,
        Map<String, Object> partitionOffset,
        TimeZone timezone,
        Long timestampInitial,
        List<String> timestampColumns
    ) {
        if (Objects.nonNull(partitionOffset)) {
            return partitionOffset;
        } else {
            Map<String, Object> initialPartitionOffset = null;
            // no offsets found
            if (timestampInitial != null) {
                // start at the specified timestamp
                if (timestampInitial == TIMESTAMP_INITIAL_CURRENT) {
                    // use the current time
                    try {
                        final Connection con = cachedConnectionProvider.getConnection();
                        Calendar cal = Calendar.getInstance(timezone);
                        timestampInitial = dialect.currentTimeOnDB(con, cal).getTime();
                    } catch (SQLException e) {
                        throw new ConnectException("Error while getting initial timestamp from database", e);
                    }
                }
                initialPartitionOffset = new HashMap<String, Object>();
                initialPartitionOffset.put(TimestampIncrementingOffset.TIMESTAMP_FIELD, timestampInitial);
                log.info("No offsets found for '{}', so using configured timestamp {}", tableOrQuery,
                        timestampInitial);
            } else {
                if (queryMode != QueryMode.TABLE || timestampColumns == null || timestampColumns.isEmpty()) {
                    return initialPartitionOffset;
                }
                try {
                    final Connection con = cachedConnectionProvider.getConnection();
                    timestampInitial = dialect.getMinTimestampValue(con, tableOrQuery, timestampColumns);
                    initialPartitionOffset = new HashMap<>();
                    initialPartitionOffset.put(TimestampIncrementingOffset.TIMESTAMP_FIELD, timestampInitial);
                    log.info("get min timestamp value from table success, table={},  min ts ={}", tableOrQuery, timestampInitial);
                } catch (SQLException e) {
                    log.error("get min timestamp value from table failed, for ", e);
                }
            }
            return initialPartitionOffset;
        }
    }


    /**
     * build table partitions
     *
     * @param tableLoadMode
     * @param queryMode
     * @param tables
     * @param dialect
     * @return
     */
    private static Map<String, RecordPartition> buildTablePartitions(
        TableLoadMode tableLoadMode,
        QueryMode queryMode,
        List<String> tables,
        DatabaseDialect dialect,
        String offsetSuffix, String topicPrefix) {

        Map<String, RecordPartition> partitionsByTableFqn = new HashMap<>();
        if (tableLoadMode == TableLoadMode.MODE_INCREMENTING
            || tableLoadMode == TableLoadMode.MODE_TIMESTAMP
            || tableLoadMode == TableLoadMode.MODE_TIMESTAMP_INCREMENTING) {
            switch (queryMode) {
                case TABLE:
                    for (String table : tables) {
                        // Find possible partition maps for different offset protocols
                        // We need to search by all offset protocol partition keys to support compatibility
                        TableId tableId = dialect.parseTableNameToTableId(table);
                        RecordPartition tablePartition = new RecordPartition(SourceOffsetCompute.sourcePartitions(topicPrefix, tableId, offsetSuffix));
                        partitionsByTableFqn.put(table, tablePartition);
                    }
                    break;
                case QUERY:
                    partitionsByTableFqn.put(JdbcSourceConfigConstants.QUERY_NAME_VALUE,
                            new RecordPartition(sourceQueryPartitions(topicPrefix, offsetSuffix))
                    );
                    break;
                default:
                    throw new ConnectException("Unknown query mode: " + queryMode);
            }
        }
        return partitionsByTableFqn;
    }
}
