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

package org.apache.rocketmq.connect.doris.connector;

import io.openmessaging.KeyValue;
import io.openmessaging.connector.api.component.task.sink.ErrorRecordReporter;
import io.openmessaging.connector.api.component.task.sink.SinkTask;
import io.openmessaging.connector.api.component.task.sink.SinkTaskContext;
import io.openmessaging.connector.api.data.ConnectRecord;
import io.openmessaging.connector.api.errors.ConnectException;
import io.openmessaging.connector.api.errors.RetriableException;
import org.apache.rocketmq.connect.doris.exception.TableAlterOrCreateException;
import org.apache.rocketmq.connect.doris.sink.Updater;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.List;

/**
 * jdbc sink task
 */
public class DorisSinkTask extends SinkTask {

    private static final Logger log = LoggerFactory.getLogger(DorisSinkTask.class);
    private SinkTaskContext context;
    private ErrorRecordReporter errorRecordReporter;
    private KeyValue originalConfig;
    private DorisSinkConfig config;
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
//                updater.closeQuietly();
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
        config = new DorisSinkConfig(keyValue);
        remainingRetries = config.getMaxRetries();
        log.info("Initializing doris writer");
        this.updater = new Updater(config);
    }

    @Override
    public void stop() {
        log.info("Stopping task");
    }
}

