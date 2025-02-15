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

package org.apache.rocketmq.connect.neo4j.source;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.rocketmq.connect.neo4j.config.Neo4jConstants;
import org.apache.rocketmq.connect.neo4j.config.Neo4jSourceConfig;
import org.apache.rocketmq.connect.neo4j.helper.LabelTypeEnum;
import org.apache.rocketmq.connect.neo4j.helper.Neo4jClient;

import io.openmessaging.KeyValue;
import io.openmessaging.connector.api.component.task.Task;
import io.openmessaging.connector.api.component.task.source.SourceConnector;
import io.openmessaging.internal.DefaultKeyValue;

public class Neo4jSourceConnector extends SourceConnector {
    private KeyValue keyValue;
    private Neo4jClient neo4jClient;

    @Override
    public List<KeyValue> taskConfigs(int maxTasks) {
        final String type = keyValue.getString(Neo4jConstants.LABEL_TYPE);
        final String labels = keyValue.getString(Neo4jConstants.LABELS);
        List<String> labelList = null;
        if (labels != null && !labels.equals("")) {
            final String[] split = labels.split(",");
            labelList = Arrays.asList(split);
        }
        if (labelList == null) {
            if (LabelTypeEnum.node.name().equals(type)) {
                labelList = neo4jClient.getAllLabels();
            } else {
                labelList = neo4jClient.getAllType();
            }
        }
        // 按标签分配任务
        List<KeyValue> configs = new ArrayList<>();
        for (int i = 0; i < labelList.size(); i++) {
            final String label = labelList.get(i);
            KeyValue config = new DefaultKeyValue();
            for (String key : keyValue.keySet()) {
                config.put(key, keyValue.getString(key));
            }
            config.put(Neo4jConstants.LABEL, label);
            configs.add(config);
        }
        return configs;
    }

    @Override
    public Class<? extends Task> taskClass() {
        return Neo4jSourceTask.class;
    }

    @Override
    public void start(KeyValue keyValue) {
        for (String requestKey : Neo4jSourceConfig.REQUEST_CONFIG) {
            if (!keyValue.containsKey(requestKey)) {
                throw new RuntimeException("Request config key: " + requestKey);
            }
        }
        Neo4jSourceConfig config = new Neo4jSourceConfig();
        config.load(keyValue);
        final LabelTypeEnum labelTypeEnum = LabelTypeEnum.nameOf(config.getLabelType());
        if (labelTypeEnum == null) {
            throw new RuntimeException("labelType is only support node or relationship!");
        }
        neo4jClient = new Neo4jClient(config);
        if (!neo4jClient.ping()) {
            throw new RuntimeException("Cannot connect to neo4j server!");
        }
        this.keyValue = keyValue;
    }

    @Override
    public void stop() {
        this.keyValue = null;
    }
}