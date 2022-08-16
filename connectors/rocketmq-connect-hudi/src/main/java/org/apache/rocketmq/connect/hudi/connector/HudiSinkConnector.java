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

package org.apache.rocketmq.connect.hudi.connector;


import com.alibaba.fastjson.JSONObject;
import io.openmessaging.KeyValue;
import io.openmessaging.connector.api.component.task.Task;
import io.openmessaging.connector.api.component.task.sink.SinkConnector;
import io.openmessaging.internal.DefaultKeyValue;
import org.apache.rocketmq.connect.hudi.config.HudiConnectConfig;
import org.apache.rocketmq.connect.hudi.config.SinkConnectConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.ArrayList;


public class HudiSinkConnector extends SinkConnector {
    private static final Logger log = LoggerFactory.getLogger(HudiSinkConnector.class);

    private SinkConnectConfig sinkConnectConfig;

    public HudiSinkConnector() {
        sinkConnectConfig = new SinkConnectConfig();
    }

    @Override
    public void validate(KeyValue config) {
        super.validate(config);
        for (String requestKey : HudiConnectConfig.REQUEST_CONFIG) {
            if (!config.containsKey(requestKey)) {
                throw new RuntimeException("Request config key: " + requestKey);
            }
        }
        try {
            this.sinkConnectConfig.validate(config);
        } catch (IllegalArgumentException e) {
            throw e;
        }
    }

    @Override
    public List<KeyValue> taskConfigs(int maxTask) {
        List<KeyValue> taskConfigs = new ArrayList<>(maxTask);
        for(int i = 0; i < maxTask;i++) {
            DefaultKeyValue defaultKeyValue = new DefaultKeyValue();
            defaultKeyValue.put(HudiConnectConfig.CONN_CONNECTOR_CLASS, sinkConnectConfig.getConnectorClass());
            defaultKeyValue.put(HudiConnectConfig.CONN_TASK_CLASS, sinkConnectConfig.getTaskClass());
            defaultKeyValue.put(HudiConnectConfig.CONN_HUDI_TABLE_PATH, sinkConnectConfig.getTablePath());
            defaultKeyValue.put(HudiConnectConfig.CONN_HUDI_TABLE_NAME, sinkConnectConfig.getTableName());
            defaultKeyValue.put(HudiConnectConfig.CONN_HUDI_INSERT_SHUFFLE_PARALLELISM, sinkConnectConfig.getInsertShuffleParallelism());
            defaultKeyValue.put(HudiConnectConfig.CONN_HUDI_UPSERT_SHUFFLE_PARALLELISM, sinkConnectConfig.getUpsertShuffleParallelism());
            defaultKeyValue.put(HudiConnectConfig.CONN_HUDI_DELETE_PARALLELISM, sinkConnectConfig.getDeleteParallelism());
            defaultKeyValue.put(HudiConnectConfig.CONN_SOURCE_RECORD_CONVERTER, sinkConnectConfig.getSrcRecordConverter());
            defaultKeyValue.put(HudiConnectConfig.CONN_TOPIC_NAMES, sinkConnectConfig.getTopicNames());
            defaultKeyValue.put(HudiConnectConfig.CONN_SCHEMA_PATH, sinkConnectConfig.getSchemaPath());
            defaultKeyValue.put(HudiConnectConfig.CONN_TASK_PARALLELISM, sinkConnectConfig.getTaskParallelism());
            defaultKeyValue.put(HudiConnectConfig.CONN_TASK_DIVIDE_STRATEGY, sinkConnectConfig.getTaskDivideStrategy());
            defaultKeyValue.put(HudiConnectConfig.CONN_WHITE_LIST, JSONObject.toJSONString(sinkConnectConfig.getWhiteList()));
            defaultKeyValue.put(HudiConnectConfig.CONN_SCHEMA_PATH, sinkConnectConfig.getSchemaPath());
            taskConfigs.add(defaultKeyValue);
        }
        return taskConfigs;
    }

    @Override
    public void start(KeyValue keyValue) {

    }

    /**
     * We need to reason why we don't call srcMQAdminExt.shutdown() here, and why
     * it can be applied to srcMQAdminExt
     */
    @Override
    public void stop() {

    }

    @Override
    public Class<? extends Task> taskClass() {
        return HudiSinkTask.class;
    }


}
