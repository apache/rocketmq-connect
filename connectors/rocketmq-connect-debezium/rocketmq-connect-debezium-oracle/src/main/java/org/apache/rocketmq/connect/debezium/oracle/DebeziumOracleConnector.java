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

package org.apache.rocketmq.connect.debezium.oracle;

import io.openmessaging.connector.api.component.task.Task;
import org.apache.rocketmq.connect.debezium.DebeziumConnector;


/**
 * debezium oracle connector
 */
public class DebeziumOracleConnector extends DebeziumConnector {
    private static final String DEFAULT_CONNECTOR = "io.debezium.connector.oracle.OracleConnector";

    /**
     * Return the current connector class
     *
     * @return task implement class
     */
    @Override
    public Class<? extends Task> taskClass() {
        return DebeziumOracleSource.class;
    }

    /**
     * get connector class
     */
    @Override
    public String getConnectorClass() {
        return DEFAULT_CONNECTOR;
    }
}
