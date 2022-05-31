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
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.rocketmq.connect.debezium;

import io.debezium.relational.HistorizedRelationalDatabaseConnectorConfig;
import io.openmessaging.KeyValue;
import org.apache.rocketmq.connect.kafka.connect.adaptor.KafkaConnectAdaptorSource;

/**
 * debezium source
 */
public abstract class DebeziumSource extends KafkaConnectAdaptorSource {
    private static final String DEFAULT_HISTORY = "org.apache.rocketmq.connect.debezium.RocketMqDatabaseHistory";
    private static final String DEFAULT_OFFSET_TOPIC = "debezium-offset-topic";
    private static final String DEFAULT_HISTORY_TOPIC = "debezium-history-topic";


    /**
     * set source task class
     *
     * @param config
     * @throws Exception
     */
    public abstract void setSourceTask(KeyValue config);


    @Override
    public void init(KeyValue config) {
        setSourceTask(config);
        // database.history : implementation class for database history.
        config.put(HistorizedRelationalDatabaseConnectorConfig.DATABASE_HISTORY.name(), DEFAULT_HISTORY);
        // history config detail
        super.init(config);
    }
}
