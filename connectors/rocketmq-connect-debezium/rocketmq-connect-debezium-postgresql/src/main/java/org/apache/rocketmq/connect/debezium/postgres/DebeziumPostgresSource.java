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

package org.apache.rocketmq.connect.debezium.postgres;

import io.openmessaging.KeyValue;
import org.apache.kafka.connect.runtime.TaskConfig;
import org.apache.rocketmq.connect.debezium.DebeziumSource;

/**
 * A rocketmq connect source that runs debezium postgresql source.
 */
public class DebeziumPostgresSource extends DebeziumSource {
    private static final String DEFAULT_TASK = "io.debezium.connector.postgresql.PostgresConnectorTask";


    /**
     * set source task class
     *
     * @param config
     * @throws Exception
     */
    @Override
    public void setSourceTask(KeyValue config) {
        config.put(TaskConfig.TASK_CLASS_CONFIG, DEFAULT_TASK);
    }
}
