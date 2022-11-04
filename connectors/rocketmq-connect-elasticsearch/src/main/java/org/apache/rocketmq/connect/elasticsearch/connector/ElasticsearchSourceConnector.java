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

package org.apache.rocketmq.connect.elasticsearch.connector;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import io.openmessaging.KeyValue;
import io.openmessaging.connector.api.component.task.Task;
import io.openmessaging.connector.api.component.task.source.SourceConnector;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.rocketmq.connect.elasticsearch.config.ElasticsearchConfig;
import org.apache.rocketmq.connect.elasticsearch.config.ElasticsearchConstant;

public class ElasticsearchSourceConnector extends SourceConnector {

    private KeyValue keyValue;

    private ElasticsearchConfig config;

    @Override
    public List<KeyValue> taskConfigs(int maxTasks) {
        this.config = new ElasticsearchConfig();
        this.config.load(keyValue);
        List<KeyValue> configs = new ArrayList<>();
        JSONObject jsonObject = JSON.parseObject(this.config.getIndex());
        for (Map.Entry<String, Object> entry : jsonObject.entrySet()) {
            final String indexName = entry.getKey();
            final JSONObject value = (JSONObject) entry.getValue();
            Integer primaryShards = value.getInteger(ElasticsearchConstant.PRIMARY_SHARDS);
            primaryShards = primaryShards > maxTasks ? maxTasks : primaryShards;
            for (int i = 0; i < primaryShards; i++) {
                this.keyValue.put(ElasticsearchConstant.INDEX, indexName);
                this.keyValue.put(ElasticsearchConstant.INCREMENT_FIELD, value.keySet().stream()
                    .filter(item -> !ElasticsearchConstant.PRIMARY_SHARDS.equals(item)).collect(Collectors.toList()).get(0));
                final String id = value.getString(this.keyValue.getString(ElasticsearchConstant.INCREMENT_FIELD));
                this.keyValue.put(ElasticsearchConstant.INCREMENT, id);
                this.keyValue.put(ElasticsearchConstant.PRIMARY_SHARD, i + "");
                configs.add(this.keyValue);
            }
        }
        return configs;
    }

    @Override
    public Class<? extends Task> taskClass() {
        return ElasticsearchSourceTask.class;
    }

    @Override
    public void start(KeyValue config) {

        for (String requestKey : ElasticsearchConfig.REQUEST_CONFIG) {
            if (!config.containsKey(requestKey)) {
                throw new RuntimeException("Request config key: " + requestKey);
            }
        }
        this.keyValue = config;

    }

    @Override
    public void stop() {
        this.keyValue = null;
    }
}
