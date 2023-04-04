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
package org.apache.rocketmq.connect.cassandra.strategy;

import com.alibaba.fastjson.JSONObject;
import io.openmessaging.KeyValue;
import io.openmessaging.internal.DefaultKeyValue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.rocketmq.connect.cassandra.config.Config;
import org.apache.rocketmq.connect.cassandra.config.DbConnectorConfig;
import org.apache.rocketmq.connect.cassandra.config.SinkDbConnectorConfig;
import org.apache.rocketmq.connect.cassandra.config.SourceDbConnectorConfig;
import org.apache.rocketmq.connect.cassandra.config.TaskDivideConfig;

public class DivideTaskByTopic extends TaskDivideStrategy {
    @Override
    public List<KeyValue> divide(DbConnectorConfig dbConnectorConfig, TaskDivideConfig tdc, KeyValue keyValue) {
        if (dbConnectorConfig instanceof SourceDbConnectorConfig) {
            return divideSourceTaskByTopic(dbConnectorConfig, tdc, keyValue);
        } else {
            return divideSinkTaskByTopic(dbConnectorConfig, tdc, keyValue);
        }
    }

    private List<KeyValue> divideSinkTaskByTopic(DbConnectorConfig dbConnectorConfig, TaskDivideConfig tdc, KeyValue keyValue) {
        List<KeyValue> config = new ArrayList<KeyValue>();
        int parallelism = tdc.getTaskParallelism();
        int id = -1;
        Set<String> topicRouteSet = ((SinkDbConnectorConfig) dbConnectorConfig).getWhiteTopics();
        Map<Integer, StringBuilder> taskTopicList = new HashMap<>();
        for (String topicName : topicRouteSet) {
            int ind = ++id % parallelism;
            if (!taskTopicList.containsKey(ind)) {
                taskTopicList.put(ind, new StringBuilder(topicName));
            } else {
                taskTopicList.get(ind).append(",").append(topicName);
            }
        }
        for (int i = 0; i < taskTopicList.size(); i++) {
            KeyValue defaultKeyValue = new DefaultKeyValue();
            defaultKeyValue.put(Config.CONN_DB_IP, tdc.getDbUrl());
            defaultKeyValue.put(Config.CONN_DB_PORT, tdc.getDbPort());
            defaultKeyValue.put(Config.CONN_DB_USERNAME, tdc.getDbUserName());
            defaultKeyValue.put(Config.CONN_DB_PASSWORD, tdc.getDbPassword());
            defaultKeyValue.put(Config.CONN_DB_DATACENTER, tdc.getLocalDataCenter());
            defaultKeyValue.put(Config.CONN_TOPIC_NAMES, taskTopicList.get(i).toString());
            defaultKeyValue.put(Config.CONN_DATA_TYPE, tdc.getDataType());
            defaultKeyValue.put(Config.CONN_SOURCE_RECORD_CONVERTER, tdc.getSrcRecordConverter());
            defaultKeyValue.put(Config.CONN_DB_MODE, tdc.getMode());
            defaultKeyValue.put(Config.CONNECTOR_CLASS, keyValue.getString(Config.CONNECTOR_CLASS));
            defaultKeyValue.put(Config.VALUE_CONVERTER, keyValue.getString(Config.VALUE_CONVERTER));
            config.add(defaultKeyValue);
        }

        return config;
    }

    private List<KeyValue> divideSourceTaskByTopic(DbConnectorConfig dbConnectorConfig, TaskDivideConfig tdc, KeyValue originalKeyValue) {
        List<KeyValue> config = new ArrayList<KeyValue>();
        int parallelism = tdc.getTaskParallelism();
        int id = -1;
        Map<String, String> topicRouteMap = ((SourceDbConnectorConfig) dbConnectorConfig).getWhiteTopics();
        Map<Integer, Map<String, Map<String, String>>> taskTopicList = new HashMap<>();
        for (Map.Entry<String, String> entry : topicRouteMap.entrySet()) {
            int ind = ++id % parallelism;
            if (!taskTopicList.containsKey(ind)) {
                taskTopicList.put(ind, new HashMap<>());
            }
            String dbKey = entry.getKey().split("-")[0];
            String tableKey = entry.getKey().split("-")[1];
            String filter = entry.getValue();
            Map<String, String> tableMap = new HashMap<>();
            tableMap.put(tableKey, filter);
            if (!taskTopicList.get(ind).containsKey(dbKey)) {
                taskTopicList.get(ind).put(dbKey, tableMap);
            } else {
                taskTopicList.get(ind).get(dbKey).putAll(tableMap);
            }
        }

        for (int i = 0; i < parallelism; i++) {
            KeyValue keyValue = new DefaultKeyValue();

            keyValue.put(Config.CONN_DB_IP, tdc.getDbUrl());
            keyValue.put(Config.CONN_DB_PORT, tdc.getDbPort());
            keyValue.put(Config.CONN_DB_USERNAME, tdc.getDbUserName());
            keyValue.put(Config.CONN_DB_PASSWORD, tdc.getDbPassword());
            keyValue.put(Config.CONN_DB_DATACENTER, tdc.getLocalDataCenter());
            keyValue.put(Config.CONN_WHITE_LIST, JSONObject.toJSONString(taskTopicList.get(i)));
            keyValue.put(Config.CONN_DATA_TYPE, tdc.getDataType());
            keyValue.put(Config.CONN_SOURCE_RECORD_CONVERTER, tdc.getSrcRecordConverter());
            keyValue.put(Config.CONN_DB_MODE, tdc.getMode());
            keyValue.put(Config.CONNECTOR_CLASS, originalKeyValue.getString(Config.CONNECTOR_CLASS));
            keyValue.put(Config.VALUE_CONVERTER, originalKeyValue.getString(Config.VALUE_CONVERTER));
            config.add(keyValue);
        }

        return config;
    }
}
