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

import com.alibaba.fastjson.JSONObject;
import com.clickhouse.client.ClickHouseClient;
import com.clickhouse.client.ClickHouseConfig;
import com.clickhouse.client.ClickHouseCredentials;
import com.clickhouse.client.ClickHouseNode;
import com.clickhouse.client.ClickHouseProtocol;
import com.clickhouse.client.ClickHouseRequest;
import com.clickhouse.client.ClickHouseResponse;
import com.clickhouse.client.ClickHouseResponseSummary;
import com.clickhouse.client.config.ClickHouseClientOption;

import com.clickhouse.data.ClickHouseDataStreamFactory;
import com.clickhouse.data.ClickHouseFormat;
import com.clickhouse.data.ClickHousePipedOutputStream;
import com.clickhouse.data.format.BinaryStreamUtils;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import io.openmessaging.KeyValue;
import io.openmessaging.connector.api.component.task.sink.SinkTask;
import io.openmessaging.connector.api.data.ConnectRecord;
import io.openmessaging.connector.api.data.Field;
import io.openmessaging.connector.api.data.Schema;
import io.openmessaging.connector.api.data.SchemaBuilder;
import io.openmessaging.connector.api.data.Struct;
import io.openmessaging.connector.api.errors.ConnectException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import org.apache.rocketmq.connect.clickhouse.config.ClickHouseBaseConfig;

public class ClickHouseSinkTask extends SinkTask {

    public ClickHouseBaseConfig config;

    private ClickHouseNode server;

    @Override public void put(List<ConnectRecord> sinkRecords) throws ConnectException {
        if (sinkRecords == null || sinkRecords.size() < 1) {
            return;
        }

        try (ClickHouseClient client = ClickHouseClient.newInstance(server.getProtocol())) {

            boolean pingOK = client.ping(server, 30000);
            if (!pingOK) {
                throw new RuntimeException("Cannot connect to clickhouse server!");
            }

            for (ConnectRecord record : sinkRecords) {

                String table = record.getSchema().getName();
                ClickHouseRequest.Mutation request = client.connect(server)
                    .write()
                    .table(table)
                    .format(ClickHouseFormat.JSONEachRow);

                ClickHouseConfig config = request.getConfig();
                request.option(ClickHouseClientOption.WRITE_BUFFER_SIZE, 8192);
                try (ClickHousePipedOutputStream stream = ClickHouseDataStreamFactory.getInstance()
                    .createPipedOutputStream(config, (Runnable) null)) {
                    CompletableFuture<ClickHouseResponse> future = request.data(stream.getInputStream()).execute();

                    final List<Field> fields = record.getSchema().getFields();
                    final Struct structData = (Struct) record.getData();
                    Gson gson = new Gson();
                    Map<String, Object> data = new HashMap<>();
                    java.lang.reflect.Type gsonType = new TypeToken<HashMap>() {
                    }.getType();

                    JSONObject object = new JSONObject();
                    for (Field field : fields) {
                        object.put(field.getName(), structData.get(field));
                        data.put(field.getName(), structData.get(field));
                    }
                    Schema NESTED_SCHEMA = SchemaBuilder.struct().build();

                    String gsonString = gson.toJson(data, gsonType);
//                    BinaryStreamUtils.writeBytes(stream, object.toJSONString().getBytes(StandardCharsets.UTF_8));
                    BinaryStreamUtils.writeBytes(stream, gsonString.getBytes(StandardCharsets.UTF_8));
                    try (ClickHouseResponse response = future.get()) {
                        ClickHouseResponseSummary summary = response.getSummary();

                    }
                }

            }

//                    ClickHouseRequest.Mutation request = client.connect(server).write().table("table")
//                        .format(ClickHouseFormat.RowBinary);
//                ClickHouseConfig config = request.getConfig();
//                CompletableFuture<ClickHouseResponse> future;
//                // back-pressuring is not supported, you can adjust the first two arguments
//                try (ClickHousePipedOutputStream stream = ClickHouseDataStreamFactory.getInstance()
//                    .createPipedOutputStream(config, (Runnable) null)) {
//                    // in async mode, which is default, execution happens in a worker thread
//                    future = request.data(stream.getInputStream()).execute();
//
//                    // writing happens in main thread
//                    for (int i = 0; i < 10_000; i++) {
//                        BinaryStreamUtils.writeString(stream, String.valueOf(i % 16));
//                        BinaryStreamUtils.writeNonNull(stream);
//                        BinaryStreamUtils.writeString(stream, UUID.randomUUID().toString());
//                    }
//                }
//
//                // response should be always closed
//                try (ClickHouseResponse response = future.get()) {
//                    ClickHouseResponseSummary summary = response.getSummary();
////                return summary.getWrittenRows();
//                }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }

//    static int query(ClickHouseNode server, String table) throws ClickHouseException {
//        try (ClickHouseClient client = ClickHouseClient.newInstance(server.getProtocol());
//             ClickHouseResponse response = client.read(server)
//                 // prefer to use RowBinaryWithNamesAndTypes as it's fully supported
//                 // see details at https://github.com/ClickHouse/clickhouse-java/issues/928
//                 .format(ClickHouseFormat.RowBinaryWithNamesAndTypes)
//                 .query("select * from " + table).execute().get()) {
//            int count = 0;
//            // or use stream API via response.stream()
//            for (ClickHouseRecord r : response.records()) {
//                count++;
//            }
//            return count;
//        } catch (InterruptedException e) {
//            Thread.currentThread().interrupt();
//            throw ClickHouseException.forCancellation(e, server);
//        } catch (ExecutionException e) {
//            throw ClickHouseException.of(e, server);
//        }
//    }

    @Override public void start(KeyValue keyValue) {
        this.config = new ClickHouseBaseConfig();
        this.config.load(keyValue);

        this.server = ClickHouseNode.builder()
            .host(config.getClickHouseHost())
            .port(ClickHouseProtocol.HTTP, config.getClickHousePort())
            .database(config.getDatabase()).credentials(getCredentials(config))
            .build();

//            ClickHouseClient clientPing = ClickHouseClient.newInstance(ClickHouseProtocol.HTTP);
//            boolean pingOK = clientPing.ping(server, 30000);
//            if (!pingOK) {
//                throw new RuntimeException("Cannot connect to clickhouse server!");
//            }
//            try {
//                dropAndCreateTable(server, "tableName");
//            } catch (ClickHouseException e) {
//                e.printStackTrace();
//            }

    }

    void dropAndCreateTable(ClickHouseNode server, String table) throws Exception {
        try (ClickHouseClient client = ClickHouseClient.newInstance(server.getProtocol())) {
            ClickHouseRequest<?> request = client.connect(server);
            // or use future chaining
            request.query("drop table if exists " + table).execute().get();
            request.query("create table " + table + "(a String, b Nullable(String)) engine=MergeTree() order by a")
                .execute().get();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private ClickHouseCredentials getCredentials(ClickHouseBaseConfig config) {
        if (config.getUserName() != null && config.getPassWord() != null) {
            return ClickHouseCredentials.fromUserAndPassword(config.getUserName(), config.getPassWord());
        }
        if (config.getAccessToken() != null) {
            return ClickHouseCredentials.fromAccessToken(config.getAccessToken());
        }
        throw new RuntimeException("Credentials cannot be empty!");

    }

    @Override public void stop() {
        this.server = null;
    }
}
