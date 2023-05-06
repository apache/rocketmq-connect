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

package org.apache.rocketmq.connect.clickhouse.sink;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import io.openmessaging.KeyValue;
import io.openmessaging.connector.api.component.task.sink.SinkTask;
import io.openmessaging.connector.api.data.ConnectRecord;
import io.openmessaging.connector.api.data.Field;
import io.openmessaging.connector.api.data.Struct;
import io.openmessaging.connector.api.errors.ConnectException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.rocketmq.connect.clickhouse.ClickHouseHelperClient;
import org.apache.rocketmq.connect.clickhouse.config.ClickHouseSinkConfig;

public class ClickHouseSinkTask extends SinkTask {

    public ClickHouseSinkConfig config;

    private ClickHouseHelperClient helperClient;

    @Override public void put(List<ConnectRecord> sinkRecords) throws ConnectException {
        if (sinkRecords == null || sinkRecords.size() < 1) {
            return;
        }
        Map<String, JSONArray> valueMap = new HashMap<>();
        for (ConnectRecord record : sinkRecords) {
            String table = record.getSchema().getName();
            JSONArray jsonArray = valueMap.getOrDefault(table, new JSONArray());

            final List<Field> fields = record.getSchema().getFields();
            final Struct structData = (Struct) record.getData();

            JSONObject object = new JSONObject();
            for (Field field : fields) {
                object.put(field.getName(), structData.get(field));
            }

            jsonArray.add(object);
            valueMap.put(table, jsonArray);
        }

        for (Map.Entry<String, JSONArray> entry : valueMap.entrySet()) {
            String jsonString = entry.getValue().toString();
            helperClient.insertJson(jsonString, entry.getKey());
        }

    }

    @Override public void start(KeyValue keyValue) {
        this.config = new ClickHouseSinkConfig();
        this.config.load(keyValue);
        this.helperClient = new ClickHouseHelperClient(this.config);
        if (!helperClient.ping()) {
            throw new RuntimeException("Cannot connect to clickhouse server!");
        }
    }

    @Override public void stop() {
        this.helperClient = null;
    }
}
