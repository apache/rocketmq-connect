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

package org.apache.rocketmq.connect.jdbc.connector;


import io.openmessaging.KeyValue;
import io.openmessaging.connector.api.component.task.sink.ErrorRecordReporter;
import io.openmessaging.connector.api.component.task.sink.SinkTask;
import io.openmessaging.connector.api.component.task.sink.SinkTaskContext;
import io.openmessaging.connector.api.data.ConnectRecord;
import io.openmessaging.connector.api.errors.ConnectException;
import io.openmessaging.connector.api.errors.RetriableException;
import org.apache.rocketmq.connect.jdbc.dialect.DatabaseDialect;
import org.apache.rocketmq.connect.jdbc.dialect.DatabaseDialectFactory;
import org.apache.rocketmq.connect.jdbc.schema.db.DbStructure;
import org.apache.rocketmq.connect.jdbc.sink.Updater;
import org.apache.rocketmq.connect.jdbc.exception.TableAlterOrCreateException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.List;

/**
 * jdbc sink task
 */
public class JdbcSinkTask extends SinkTask {

    private static final Logger log = LoggerFactory.getLogger(JdbcSinkTask.class);
    private SinkTaskContext context;
    private ErrorRecordReporter errorRecordReporter;
    private KeyValue originalConfig;
    private JdbcSinkConfig config;
    private DatabaseDialect dialect;
    int remainingRetries;
    private Updater updater;


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
            updater.write(records);
        } catch (TableAlterOrCreateException tace) {
            throw tace;
        } catch (SQLException sqle) {
            SQLException sqlAllMessagesException = getAllMessagesException(sqle);
            if (remainingRetries > 0) {
                updater.closeQuietly();
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
    
    /**
     * Start the component
     *
     * @param keyValue
     */
    @Override
    public void start(KeyValue keyValue) {
        originalConfig = keyValue;
        config = new JdbcSinkConfig(keyValue);
        remainingRetries = config.getMaxRetries();
        if (config.getDialectName() != null && !config.getDialectName().trim().isEmpty()) {
            dialect = DatabaseDialectFactory.create(config.getDialectName(), config);
        } else {
            dialect = DatabaseDialectFactory.findDialectFor(config.getConnectionDbUrl(), config);
        }
        final DbStructure dbStructure = new DbStructure(dialect);
        log.info("Initializing writer using SQL dialect: {}", dialect.getClass().getSimpleName());
        this.updater = new Updater(config, dialect, dbStructure);
    }

    @Override
    public void stop() {
        log.info("Stopping task");
        try {
            updater.closeQuietly();
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
