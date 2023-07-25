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

package org.apache.rocketmq.connect.jdbc.source;

import io.openmessaging.KeyValue;
import io.openmessaging.connector.api.component.task.source.SourceTask;
import io.openmessaging.connector.api.data.ConnectRecord;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.connect.jdbc.connection.CachedConnectionProvider;
import org.apache.rocketmq.connect.jdbc.dialect.DatabaseDialect;
import org.apache.rocketmq.connect.jdbc.dialect.DatabaseDialectLoader;
import org.apache.rocketmq.connect.jdbc.source.common.IncrementContext;
import org.apache.rocketmq.connect.jdbc.source.common.QueryContext;
import org.apache.rocketmq.connect.jdbc.source.common.QueryMode;
import org.apache.rocketmq.connect.jdbc.source.common.TableLoadMode;
import org.apache.rocketmq.connect.jdbc.source.offset.SourceOffsetCompute;
import org.apache.rocketmq.connect.jdbc.source.querier.BulkQuerier;
import org.apache.rocketmq.connect.jdbc.source.querier.Querier;
import org.apache.rocketmq.connect.jdbc.source.querier.TimestampIncrementingQuerier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * jdbc source task
 */
public class JdbcSourceTask extends SourceTask {
    private static final Logger log = LoggerFactory.getLogger(JdbcSourceTask.class);
    private static final int CONSECUTIVE_EMPTY_RESULTS_BEFORE_RETURN = 3;
    private JdbcSourceTaskConfig config;
    private DatabaseDialect dialect;
    private CachedConnectionProvider cachedConnectionProvider;
    BlockingQueue<Querier> tableQueue = new LinkedBlockingQueue<Querier>();
    private final AtomicBoolean running = new AtomicBoolean(false);

    @Override
    public List<ConnectRecord> poll() {
        log.trace(" Polling for new data");
        Map<Querier, Integer> consecutiveEmptyResults = tableQueue.stream().collect(Collectors.toMap(Function.identity(), (q) -> 0));
        while (running.get()) {
            final Querier querier = tableQueue.peek();
            if (sleepIfNeed(querier))
                continue;

            // poll data
            final List<ConnectRecord> results = new ArrayList<>();
            try {
                log.debug("Checking for next block of results from {}", querier);
                querier.maybeStartQuery(cachedConnectionProvider);
                boolean hasNext = true;
                while (results.size() < config.getBatchMaxRows() && (hasNext = querier.hasNext())) {
                    results.add(querier.extractRecord());
                }
                if (!hasNext) {
                    resetAndRequeueHead(querier);
                }
                if (results.isEmpty()) {
                    consecutiveEmptyResults.compute(querier, (k, v) -> v + 1);
                    log.trace("No updates for {}", querier);
                    if (Collections.min(consecutiveEmptyResults.values()) >= CONSECUTIVE_EMPTY_RESULTS_BEFORE_RETURN) {
                        log.warn("More than " + CONSECUTIVE_EMPTY_RESULTS_BEFORE_RETURN + " consecutive empty results for all queriers, returning");
                        return null;
                    } else {
                        continue;
                    }
                } else {
                    consecutiveEmptyResults.put(querier, 0);
                }
                log.debug("Returning {} records for {}", results.size(), querier.toString());
                return results;
            } catch (SQLException sqle) {
                log.error("Failed to run query for table {}: {}", querier.toString(), sqle);
                resetAndRequeueHead(querier);
                throw new RuntimeException(sqle);
            } catch (Throwable t) {
                resetAndRequeueHead(querier);
                // This task has failed, so close any resources (may be reopened if needed) before throwing
                closeResources();
                throw t;
            }
        }

        final Querier querier = tableQueue.peek();
        if (querier != null) {
            resetAndRequeueHead(querier);
        }
        closeResources();
        return null;
    }

    /**
     * Sleep if need
     *
     * @param querier
     * @return
     */
    private boolean sleepIfNeed(Querier querier) {
        if (!querier.querying()) {
            final long nextUpdate = querier.getLastUpdate() + config.getPollIntervalMs();
            final long now = System.currentTimeMillis();
            final long sleepMs = Math.min(nextUpdate - now, 100);
            if (sleepMs > 0) {
                log.trace("Waiting {} ms to poll {} next", nextUpdate - now, querier);
                try {
                    Thread.sleep(sleepMs);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                return true;
            }
        }
        return false;
    }

    private void resetAndRequeueHead(Querier querier) {
        log.debug("Resetting querier {}", querier.toString());
        tableQueue.poll();
        if (running.get()) {
            querier.reset(System.currentTimeMillis());
        } else {
            querier.reset(0);
        }
        tableQueue.add(querier);
    }


    /**
     * Should invoke before start the connector.
     *
     * @param config
     * @return error message
     */
    @Override
    public void validate(KeyValue config) {
    }

    /**
     * start jdbc task
     */
    @Override
    public void start(KeyValue props) {
        // init config
        config = new JdbcSourceTaskConfig(props);
        this.dialect = DatabaseDialectLoader.getDatabaseDialect(config);
        cachedConnectionProvider = connectionProvider(config.getAttempts(), config.getBackoffMs());
        log.info("Using JDBC dialect {}", dialect.name());
        // compute table offset
        Map<String, Map<String, Object>> offsetValues = SourceOffsetCompute.initOffset(config, sourceTaskContext, dialect, cachedConnectionProvider);
        for (String tableOrQuery : offsetValues.keySet()) {
            this.buildAndAddQuerier(
                TableLoadMode.findTableLoadModeByName(this.config.getMode()),
                this.config.getQuerySuffix(),
                this.config.getIncrementingColumnName(),
                this.config.getTimestampColumnNames(),
                this.config.getTimestampDelayIntervalMs(),
                this.config.getTimeZone(), tableOrQuery,
                offsetValues.get(tableOrQuery)
            );
        }
        running.set(true);
        log.info("Started JDBC source task");
    }

    /**
     * build and add querier
     *
     * @param loadMode
     * @param querySuffix
     * @param incrementingColumn
     * @param timestampColumns
     * @param timestampDelayInterval
     * @param timeZone
     * @param tableOrQuery
     * @param offset
     */
    private void buildAndAddQuerier(TableLoadMode loadMode, String querySuffix, String incrementingColumn,
        List<String> timestampColumns, Long timestampDelayInterval, TimeZone timeZone, String tableOrQuery,
        Map<String, Object> offset) {
        String topicPrefix = config.getTopicPrefix();
        QueryMode queryMode = !StringUtils.isEmpty(config.getQuery()) ? QueryMode.QUERY : QueryMode.TABLE;
        Querier querier;
        switch (loadMode) {
            case MODE_BULK:
                querier = new BulkQuerier(
                    dialect,
                    getContext(querySuffix, tableOrQuery, topicPrefix, queryMode)
                );
                tableQueue.add(querier);
                break;
            case MODE_INCREMENTING:
                querier = new TimestampIncrementingQuerier(
                    dialect,
                    this.getIncrementContext(querySuffix, tableOrQuery, topicPrefix, queryMode, null, incrementingColumn, offset, timestampDelayInterval, timeZone)
                );
                tableQueue.add(querier);
                break;
            case MODE_TIMESTAMP:
                querier = new TimestampIncrementingQuerier(
                    dialect,
                    this.getIncrementContext(querySuffix, tableOrQuery, topicPrefix, queryMode, timestampColumns, null, offset, timestampDelayInterval, timeZone)
                );
                tableQueue.add(querier);
                break;
            case MODE_TIMESTAMP_INCREMENTING:
                querier = new TimestampIncrementingQuerier(
                    dialect,
                    this.getIncrementContext(querySuffix, tableOrQuery, topicPrefix, queryMode, timestampColumns, incrementingColumn, offset, timestampDelayInterval, timeZone)
                );
                tableQueue.add(querier);
                break;
        }
    }

    // Common context
    private QueryContext getContext(String querySuffix, String tableOrQuery, String topicPrefix, QueryMode queryMode) {
        QueryContext context = new QueryContext(
            queryMode,
            queryMode == QueryMode.TABLE ? dialect.parseTableNameToTableId(tableOrQuery) : null,
            queryMode == QueryMode.QUERY ? tableOrQuery : null,
            topicPrefix,
            this.config.getOffsetSuffix(),
            querySuffix,
            config.getBatchMaxRows()
        );
        return context;
    }

    // Increment context
    private IncrementContext getIncrementContext(
        String querySuffix,
        String tableOrQuery,
        String topicPrefix,
        QueryMode queryMode,
        List<String> timestampColumnNames,
        String incrementingColumnName,
        Map<String, Object> offsetMap,
        Long timestampDelay,
        TimeZone timeZone

    ) {
        IncrementContext context = new IncrementContext(
            queryMode,
            queryMode == QueryMode.TABLE ? dialect.parseTableNameToTableId(tableOrQuery) : null,
            queryMode == QueryMode.QUERY ? tableOrQuery : null,
            topicPrefix,
            this.config.getOffsetSuffix(),
            querySuffix,
            config.getBatchMaxRows(),
            timestampColumnNames != null ? timestampColumnNames : Collections.emptyList(),
            incrementingColumnName,
            offsetMap,
            timestampDelay,
            timeZone
        );
        return context;
    }

    protected CachedConnectionProvider connectionProvider(int maxConnAttempts, long retryBackoff) {
        return new CachedConnectionProvider(dialect, maxConnAttempts, retryBackoff) {
            @Override
            protected void onConnect(final Connection connection) throws SQLException {
                super.onConnect(connection);
                connection.setAutoCommit(false);
            }
        };
    }


    @Override
    public void stop() {
        running.set(true);
    }


    protected void closeResources() {
        log.info("Closing resources for JDBC source task");
        try {
            if (cachedConnectionProvider != null) {
                cachedConnectionProvider.close();
            }
        } catch (Throwable t) {
            log.warn("Error while closing the connections", t);
        } finally {
            cachedConnectionProvider = null;
            try {
                if (dialect != null) {
                    dialect.close();
                }
            } catch (Throwable t) {
                log.warn("Error while closing the {} dialect: ", dialect.name(), t);
            } finally {
                dialect = null;
            }
        }
    }
}
