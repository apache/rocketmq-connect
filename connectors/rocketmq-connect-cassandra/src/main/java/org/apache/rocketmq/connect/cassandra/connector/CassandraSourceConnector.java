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

package org.apache.rocketmq.connect.cassandra.connector;

import io.openmessaging.KeyValue;
import io.openmessaging.connector.api.component.task.Task;
import io.openmessaging.connector.api.component.task.source.SourceConnector;
import java.util.List;
import org.apache.rocketmq.connect.cassandra.common.DataType;
import org.apache.rocketmq.connect.cassandra.config.Config;
import org.apache.rocketmq.connect.cassandra.config.ConfigUtil;
import org.apache.rocketmq.connect.cassandra.config.DbConnectorConfig;
import org.apache.rocketmq.connect.cassandra.config.SourceDbConnectorConfig;
import org.apache.rocketmq.connect.cassandra.config.TaskDivideConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CassandraSourceConnector extends SourceConnector {
    private static final Logger log = LoggerFactory.getLogger(CassandraSourceConnector.class);
    private DbConnectorConfig dbConnectorConfig;

    private KeyValue keyValue;

    private volatile boolean configValid = false;

    public CassandraSourceConnector() {
        dbConnectorConfig = new SourceDbConnectorConfig();
    }

    @Override
    public void validate(KeyValue config) {
        this.keyValue = config;
        for (String requestKey : Config.REQUEST_CONFIG) {
            if (!keyValue.containsKey(requestKey)) {
                throw new IllegalArgumentException("Request config key: " + requestKey);
            }
        }
        ConfigUtil.load(config, dbConnectorConfig);
        dbConnectorConfig.validate(config);
        this.configValid = true;
    }


    @Override public void start(KeyValue keyValue) {
        log.info("CassandraSourceConnector start");
        this.keyValue = keyValue;
    }

    @Override
    public void stop() {
        this.keyValue = null;
    }

    @Override
    public Class<? extends Task> taskClass() {
        return CassandraSourceTask.class;
    }

    @Override
    public List<KeyValue> taskConfigs(int maxTasks) {
        this.dbConnectorConfig.setTaskParallelism(maxTasks);
        TaskDivideConfig tdc = new TaskDivideConfig(
                this.dbConnectorConfig.getDbUrl(),
                this.dbConnectorConfig.getDbPort(),
                this.dbConnectorConfig.getDbUserName(),
                this.dbConnectorConfig.getDbPassword(),
                this.dbConnectorConfig.getLocalDataCenter(),
                this.dbConnectorConfig.getConverter(),
                DataType.COMMON_MESSAGE.ordinal(),
                this.dbConnectorConfig.getTaskParallelism(),
                this.dbConnectorConfig.getMode()
        );
        return this.dbConnectorConfig.getTaskDivideStrategy().divide(this.dbConnectorConfig, tdc, keyValue);
    }

}
