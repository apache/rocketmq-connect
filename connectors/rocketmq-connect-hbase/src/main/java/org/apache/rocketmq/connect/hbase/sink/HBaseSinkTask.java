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

package org.apache.rocketmq.connect.hbase.sink;

import io.openmessaging.KeyValue;
import io.openmessaging.connector.api.component.task.sink.SinkTask;
import io.openmessaging.connector.api.data.ConnectRecord;
import io.openmessaging.connector.api.data.Field;
import io.openmessaging.connector.api.data.Struct;
import io.openmessaging.connector.api.errors.ConnectException;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.rocketmq.connect.hbase.config.HBaseSinkConfig;
import org.apache.rocketmq.connect.hbase.helper.HBaseHelperClient;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class HBaseSinkTask extends SinkTask {
    public HBaseSinkConfig config;

    private HBaseHelperClient helperClient;

    @Override
    public void put(List<ConnectRecord> sinkRecords) throws ConnectException {
        if (sinkRecords == null || sinkRecords.size() < 1) {
            return;
        }
        Map<String, List<Put>> valueMap = new HashMap<>();
        for (ConnectRecord connectRecord : sinkRecords) {
            String tableName = connectRecord.getSchema().getName();
            List<Put> puts = valueMap.getOrDefault(tableName, new ArrayList<>());
            final List<Field> fields = connectRecord.getSchema().getFields();
            final Struct structData = (Struct) connectRecord.getData();
            String index = structData.get(fields.get(0)).toString();
            Put put = new Put(Bytes.toBytes(index));
            fields.stream().filter(Objects::nonNull)
                    .forEach(field -> put.addColumn(Bytes.toBytes(this.config.getColumnFamily()),
                            Bytes.toBytes(field.getName()),
                            Bytes.toBytes(structData.get(field).toString())));
            puts.add(put);
            valueMap.put(tableName, puts);
        }

        valueMap.forEach((tableName, puts) -> {
            if (!this.helperClient.tableExists(tableName)) {
                this.helperClient.createTable(tableName, Collections.singletonList(this.config.getColumnFamily()));
            }
            this.helperClient.batchInsert(tableName, puts);
        });
    }

    @Override
    public void start(KeyValue keyValue) {
        this.config = new HBaseSinkConfig();
        this.config.load(keyValue);
        this.helperClient = new HBaseHelperClient(this.config);
    }

    @Override
    public void stop() {
        this.helperClient.close();
    }
}
