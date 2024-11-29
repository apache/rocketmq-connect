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

package org.apache.rocketmq.connect.hologres.connector;

import com.alibaba.hologres.client.HoloClient;
import com.alibaba.hologres.client.HoloConfig;
import com.alibaba.hologres.client.Put;
import com.alibaba.hologres.client.exception.HoloClientException;
import com.alibaba.hologres.client.model.TableSchema;
import com.alibaba.hologres.client.model.WriteMode;
import io.openmessaging.KeyValue;
import io.openmessaging.connector.api.component.task.sink.SinkTask;
import io.openmessaging.connector.api.data.ConnectRecord;
import io.openmessaging.connector.api.data.Field;
import io.openmessaging.connector.api.data.Struct;
import io.openmessaging.connector.api.errors.ConnectException;
import io.openmessaging.connector.api.errors.RetriableException;
import org.apache.rocketmq.connect.hologres.config.HologresSinkConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class HologresSinkTask extends SinkTask {
    private static final Logger log = LoggerFactory.getLogger(HologresSinkTask.class);

    private KeyValue keyValue;
    private HologresSinkConfig sinkConfig;
    private HoloConfig holoClientConfig;
    private HoloClient holoClient;
    private final Map<String, TableSchema> tableSchemaCache = new HashMap<>();

    @Override
    public void put(List<ConnectRecord> records) throws ConnectException {
        if (records == null || records.isEmpty()) {
            return;
        }

        log.info("Received {} sink records.", records.size());

        try {
            if (!tableSchemaCache.containsKey(sinkConfig.getTable())) {
                tableSchemaCache.put(sinkConfig.getTable(), holoClient.getTableSchema(sinkConfig.getTable()));
            }
            TableSchema tableSchema = tableSchemaCache.get(sinkConfig.getTable());
            // TODO: check record match table schema
            log.info("get table schema {}", tableSchema);

            List<Put> puts = new ArrayList<>();
            for (ConnectRecord record : records) {
                log.info("Convert record: {}", record);
                Put put = new Put(tableSchema);
                List<Field> fields = record.getSchema().getFields();
                Struct structData = (Struct) record.getData();
                fields.forEach(field -> put.setObject(field.getName(), structData.get(field)));
                puts.add(put);
            }

            holoClient.put(puts);
            log.info("Put {} record to hologres success", puts.size());
        } catch (HoloClientException e) {
            log.error("Put record to hologres failed", e);
            throw new RetriableException(e);
        }
    }

    @Override
    public void start(KeyValue keyValue) {
        this.keyValue = keyValue;
        this.sinkConfig = new HologresSinkConfig(keyValue);
        this.holoClientConfig = buildHoloConfig(sinkConfig);
        log.info("Initializing hologres client");
        try {
            this.holoClient = new HoloClient(holoClientConfig);
            this.holoClient.setAsyncCommit(false);
        } catch (HoloClientException e) {
            log.error("Init hologres client failed", e);
            throw new RuntimeException(e);
        }
        log.info("Hologres client started.");
    }

    private HoloConfig buildHoloConfig(HologresSinkConfig sinkConfig) {
        HoloConfig holoConfig = new HoloConfig();
        holoConfig.setJdbcUrl(sinkConfig.getJdbcUrl());
        holoConfig.setUsername(sinkConfig.getUsername());
        holoConfig.setPassword(sinkConfig.getPassword());
        // TODO: support more configuration
        holoConfig.setDynamicPartition(sinkConfig.isDynamicPartition());
        holoConfig.setWriteMode(WriteMode.valueOf(sinkConfig.getWriteMode()));
        return holoConfig;
    }

    @Override
    public void stop() {
        log.info("Stopping hologres client");
        this.holoClient.close();
    }
}
