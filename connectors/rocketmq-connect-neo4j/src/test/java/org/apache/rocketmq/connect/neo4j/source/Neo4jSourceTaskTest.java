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

import java.util.List;

import org.apache.rocketmq.connect.neo4j.config.Neo4jBaseConfig;
import org.apache.rocketmq.connect.neo4j.config.Neo4jConstants;
import org.apache.rocketmq.connect.neo4j.config.Neo4jSourceConfig;
import org.apache.rocketmq.connect.neo4j.helper.Neo4jClient;

import io.openmessaging.KeyValue;
import io.openmessaging.connector.api.data.ConnectRecord;
import io.openmessaging.internal.DefaultKeyValue;

public class Neo4jSourceTaskTest {
//    private static final String host= "localhost";
//    private static final Integer port = 7687;
//    private static final String db = "test";
//    private static final String user = "test";
//    private static final String password = "root123456";
//
//    public void testClient() {
//        Neo4jBaseConfig neo4jBaseConfig = new Neo4jSourceConfig();
//        KeyValue config = new DefaultKeyValue();
//        config.put(Neo4jConstants.NEO4J_HOST, host);
//        config.put(Neo4jConstants.NEO4J_PORT, port);
//        config.put(Neo4jConstants.NEO4J_USER, user);
//        config.put(Neo4jConstants.NEO4J_PASSWORD, password);
//        config.put(Neo4jConstants.NEO4J_DB, db);
//        neo4jBaseConfig.load(config);
//        Neo4jClient client = new Neo4jClient(neo4jBaseConfig);
//        final boolean ping = client.ping();
//        System.out.println(ping);
//    }
//
//    public void testPoll() throws InterruptedException {
//        Neo4jSourceTask task = new Neo4jSourceTask();
//        KeyValue config = new DefaultKeyValue();
//        config.put(Neo4jConstants.NEO4J_HOST, host);
//        config.put(Neo4jConstants.NEO4J_PORT, port);
//        config.put(Neo4jConstants.NEO4J_USER, user);
//        config.put(Neo4jConstants.NEO4J_PASSWORD, password);
//        config.put(Neo4jConstants.NEO4J_DB, db);
//        config.put(Neo4jConstants.NEO4J_TOPIC, "nodeNeo4jTopic");
//        config.put(Neo4jConstants.LABEL_TYPE, "node");
//        config.put(Neo4jConstants.LABELS, "Goods");
//        config.put(Neo4jConstants.COLUMN, "[\n" + "        {\n" + "            \"name\":\"goodsId\",\n"
//            + "            \"type\":\"long\",\n" + "            \"columnType\":\"primaryKey\",\n"
//            + "            \"valueExtract\":\"#{goodsId}\"\n" + "        },\n" + "        {\n"
//            + "            \"name\":\"label\",\n" + "            \"type\":\"string\",\n"
//            + "            \"columnType\":\"primaryLabel\"\n" + "        },\n" + "        {\n"
//            + "            \"name\":\"goodsName\",\n" + "            \"type\":\"string\",\n"
//            + "            \"columnType\":\"nodeProperty\",\n" + "            \"valueExtract\":\"#{goodsName}\"\n"
//            + "        }\n" + "    ]");
//        task.start(config);
//        while (true) {
//            final List<ConnectRecord> connectRecordList = task.poll();
//            for(ConnectRecord record : connectRecordList) {
//                System.out.println(record);
//            }
//            Thread.sleep(3000);
//        }
//    }
//
//    public static void main(String[] args) throws InterruptedException {
//        Neo4jSourceTaskTest neo4jSourceTaskTest = new Neo4jSourceTaskTest();
//        neo4jSourceTaskTest.testClient();
//        neo4jSourceTaskTest.testPoll();
//    }
}