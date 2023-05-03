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

import io.openmessaging.KeyValue;
import io.openmessaging.connector.api.component.task.sink.SinkTask;
import io.openmessaging.connector.api.data.ConnectRecord;
import io.openmessaging.connector.api.errors.ConnectException;
import io.openmessaging.connector.api.errors.RetriableException;
import java.sql.SQLException;
import java.util.List;
import org.apache.rocketmq.connect.jdbc.dialect.DatabaseDialect;
import org.apache.rocketmq.connect.jdbc.dialect.DatabaseDialectLoader;
import org.apache.rocketmq.connect.jdbc.exception.TableAlterOrCreateException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * jdbc sink task
 */
public class JdbcSinkTask extends SinkTask {
    private static final Logger log = LoggerFactory.getLogger(JdbcSinkTask.class);
    private KeyValue originalConfig;
    private JdbcSinkConfig config;
    private DatabaseDialect dialect;
    private int remainingRetries;
    private JdbcWriter jdbcWriter;

    /**
     * Start the component
     * @param keyValue
     */
    @Override
    public void start(KeyValue keyValue) {
        originalConfig = keyValue;
        config = new JdbcSinkConfig(keyValue);
        remainingRetries = config.getMaxRetries();
        this.dialect = DatabaseDialectLoader.getDatabaseDialect(config);
        log.info("Initializing writer using SQL dialect: {}", dialect.getClass().getSimpleName());
        this.jdbcWriter = new JdbcWriter(config, dialect);
    }

    /**
     * Put the records to the sink
     *
     * @param records
     */
    @Override
    public void put(List<ConnectRecord> records) throws ConnectException {
        if (records.isEmpty()) {
            return;
        }
        final int recordsCount = records.size();
        log.debug("Received {} records.", recordsCount);
        try {
            jdbcWriter.write(records);
        } catch (TableAlterOrCreateException tace) {
            throw tace;
        } catch (SQLException sqle) {
            SQLException sqlAllMessagesException = getAllMessagesException(sqle);
            if (remainingRetries > 0) {
                jdbcWriter.closeQuietly();
                start(originalConfig);
                remainingRetries--;
                throw new RetriableException(sqlAllMessagesException);
            }

        }
        remainingRetries = config.getMaxRetries();
    }

    private SQLException getAllMessagesException(SQLException sqle) {
        String sqleAllMessages = "Exception chain:" + System.lineSeparator();
        for (Throwable e : sqle) {
            sqleAllMessages += e + System.lineSeparator();
        }
        SQLException sqlAllMessagesException = new SQLException(sqleAllMessages);
        sqlAllMessagesException.setNextException(sqle);
        return sqlAllMessagesException;
    }

    @Override
    public void stop() {
        log.info("Stopping task");
        try {
            jdbcWriter.closeQuietly();
        } finally {
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
