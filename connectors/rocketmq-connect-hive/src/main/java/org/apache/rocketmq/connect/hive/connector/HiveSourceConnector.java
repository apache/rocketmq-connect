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

package org.apache.rocketmq.connect.hive.connector;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import io.openmessaging.KeyValue;
import io.openmessaging.connector.api.component.task.Task;
import io.openmessaging.connector.api.component.task.source.SourceConnector;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.rocketmq.connect.hive.config.HiveJdbcDriverManager;
import org.apache.rocketmq.connect.hive.config.HiveConfig;
import org.apache.rocketmq.connect.hive.config.HiveConstant;
import org.apache.rocketmq.connect.hive.replicator.source.HiveQuery;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HiveSourceConnector extends SourceConnector {

    private static final Logger log = LoggerFactory.getLogger(HiveSourceConnector.class);

    private KeyValue keyValue;
    private HiveConfig config;

    private transient Statement stmt;

    @Override
    public List<KeyValue> taskConfigs(int maxTasks) {
        List<KeyValue> configs = new ArrayList<>();
        JSONObject jsonObject = JSON.parseObject(this.config.getTables());
        for (Map.Entry<String, Object> entry : jsonObject.entrySet()) {
            final String tableName = entry.getKey();
            final JSONObject value = (JSONObject) entry.getValue();
            final Map.Entry<String, Object> increment = value.entrySet().stream().findFirst().get();
            final String incrementField = increment.getKey();
            String initIncrementValue = increment.getValue().toString();
            this.keyValue.put(HiveConstant.TABLE_NAME, tableName);
            this.keyValue.put(HiveConstant.INCREMENT_FIELD, incrementField);
            this.keyValue.put(HiveConstant.INCREMENT_VALUE, initIncrementValue);
            this.keyValue.put(tableName, incrementField);
            if (maxTasks < 2) {
                configs.add(this.keyValue);
                continue;
            }
            try {
                final Integer count = HiveQuery.countData(stmt, tableName, incrementField, initIncrementValue);
                if (count < 100000) {
                    configs.add(this.keyValue);
                    continue;
                }
                final int eachNum = count / maxTasks;
                for (int i = 0; i < maxTasks; i++) {
                    String incrementValue = HiveQuery.selectSpecifiedIndexData(stmt, tableName, incrementField, initIncrementValue, eachNum * i);
                    this.keyValue.put(HiveConstant.INCREMENT_VALUE, incrementValue);
                    configs.add(this.keyValue);
                }
            } catch (SQLException e) {
                log.error("create task configs failed", e);
            }

        }
        return configs;
    }

    @Override
    public Class<? extends Task> taskClass() {
        return HiveSourceTask.class;
    }

    @Override
    public void start(KeyValue keyValue) {
        for (String requestKey : HiveConfig.REQUEST_CONFIG_SOURCE) {
            if (!keyValue.containsKey(requestKey)) {
                throw new RuntimeException("Request config key: " + requestKey);
            }
        }
        this.keyValue = keyValue;
        this.config = new HiveConfig();
        this.config.load(keyValue);
        HiveJdbcDriverManager.init(config);
        this.stmt = HiveJdbcDriverManager.getStatement();

    }

    @Override
    public void stop() {
        this.keyValue = null;
        HiveJdbcDriverManager.destroy();

    }
}
