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

import com.google.common.collect.Lists;
import io.openmessaging.KeyValue;
import io.openmessaging.connector.api.component.task.Task;
import io.openmessaging.connector.api.component.task.source.SourceConnector;
import io.openmessaging.internal.DefaultKeyValue;
import java.util.List;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.connect.jdbc.util.ConnectorGroupUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * jdbc source connector
 */
public class JdbcSourceConnector extends SourceConnector {
    private static final Logger log = LoggerFactory.getLogger(JdbcSourceConnector.class);
    private JdbcSourceConfig jdbcSourceConfig;
    private KeyValue originalConfig;

    /**
     * Should invoke before start the connector.
     *
     * @param config
     * @return error message
     */
    @Override
    public void validate(KeyValue config) {
        jdbcSourceConfig = new JdbcSourceConfig(config);
        // validate config
    }

    /**
     * Start the component
     *
     * @param config component context
     */
    @Override
    public void start(KeyValue config) {
        originalConfig = config;
    }


    @Override
    public void stop() {
        this.originalConfig = null;
        this.jdbcSourceConfig = null;
    }

    /**
     * Returns a set of configurations for Tasks based on the current configuration,
     * producing at most count configurations.
     *
     * @param maxTasks maximum number of configurations to generate
     * @return configurations for Tasks
     */
    @Override
    public List<KeyValue> taskConfigs(int maxTasks) {
        log.info("Connector task config divide[" + maxTasks + "]");
        List<KeyValue> keyValues = Lists.newArrayList();
        List<String> tables = Lists.newArrayList();
        log.info("Connector table white list[" + jdbcSourceConfig.getTableWhitelist() + "]");
        jdbcSourceConfig.getTableWhitelist().forEach(table -> {
            tables.add(table);
        });
        maxTasks = tables.size() > maxTasks ? maxTasks : tables.size();
        List<List<String>> tablesGrouped =
                ConnectorGroupUtils.groupPartitions(tables, maxTasks);
        for (List<String> tableGroup : tablesGrouped) {
            KeyValue keyValue = new DefaultKeyValue();
            for (String key : originalConfig.keySet()) {
                keyValue.put(key, originalConfig.getString(key));
            }
            keyValue.put(JdbcSourceTaskConfig.TABLES_CONFIG, StringUtils.join(tableGroup, ","));
            keyValues.add(keyValue);
        }
        return keyValues;
    }

    @Override
    public Class<? extends Task> taskClass() {
        return JdbcSourceTask.class;
    }
}
