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
package org.apache.rocketmq.connect.neo4j.sink;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.rocketmq.connect.neo4j.config.Neo4jConstants;

import io.openmessaging.KeyValue;
import io.openmessaging.connector.api.data.*;
import io.openmessaging.internal.DefaultKeyValue;

public class Neo4jSinkTaskTest {

//    private static final String host= "localhost";
//    private static final Integer port = 7687;
//    private static final String db = "tmp";
//    private static final String user = "test";
//    private static final String password = "root123456";
//
//    public static void main(String[] args) {
//        List<ConnectRecord> records = new ArrayList<>();
//        // build schema
//        Schema schema = SchemaBuilder.struct()
//            .name("tableName")
//            .field("label",SchemaBuilder.string().build())
//            .field("recordId", SchemaBuilder.string().build())
//            .build();
//
//
//        for (int i = 0; i < 4; i++) {
//            // build record
//            String param0 = "Record";
//            Struct struct= new Struct(schema);
//            struct.put("label", param0);
//            struct.put("recordId",String.valueOf(i));
//            ConnectRecord record = new ConnectRecord(
//                // offset partition
//                // offset partition"
//                new RecordPartition(new ConcurrentHashMap<>()),
//                new RecordOffset(new HashMap<>()),
//                System.currentTimeMillis(),
//                schema,
//                struct
//            );
//            records.add(record);
//
//        }
//        Neo4jSinkTask task = new Neo4jSinkTask();
//        KeyValue config = new DefaultKeyValue();
//        config.put(Neo4jConstants.NEO4J_HOST, host);
//        config.put(Neo4jConstants.NEO4J_PORT, port);
//        config.put(Neo4jConstants.NEO4J_USER, user);
//        config.put(Neo4jConstants.NEO4J_PASSWORD, password);
//        config.put(Neo4jConstants.NEO4J_DB, db);
//        config.put(Neo4jConstants.LABEL_TYPE, "node");
//        config.put(Neo4jConstants.COLUMN, "[\n" + "        {\n" + "            \"name\":\"recordId\",\n"
//            + "            \"type\":\"long\",\n" + "            \"columnType\":\"primaryKey\",\n"
//            + "            \"valueExtract\":\"#{recordId}\"\n" + "        },\n" + "        {\n"
//            + "            \"name\":\"Goods\",\n" + "            \"type\":\"string\",\n"
//            + "            \"columnType\":\"primaryLabel\",\n" + "            \"valueExtract\":\"#{label}\"\n"
//            + "        }\n" + "    ]");
//
//        task.start(config);
//        task.put(records);
//
//    }

}