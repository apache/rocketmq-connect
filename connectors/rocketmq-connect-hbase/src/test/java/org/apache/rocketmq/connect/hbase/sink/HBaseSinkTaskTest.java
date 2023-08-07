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
import io.openmessaging.connector.api.data.*;
import io.openmessaging.internal.DefaultKeyValue;
import junit.framework.TestCase;
import org.apache.rocketmq.connect.hbase.config.HBaseConstants;
import org.apache.rocketmq.connect.hbase.config.HBaseSinkConfig;
import org.apache.rocketmq.connect.hbase.helper.HBaseHelperClient;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

public class HBaseSinkTaskTest extends TestCase {

    private static final String zkHost = "ld-m5ejp1wxrnytqnmz1-proxy-lindorm-pub.lindorm.rds.aliyuncs.com:30020";
    private static final String columnFamily = "cf";

    private static final String userName = "root";

    private static final String password = "xxxxxx";

    public static void main(String[] args) {
        List<ConnectRecord> records = new ArrayList<>();
        // build schema
        Schema schema = SchemaBuilder.struct()
                .name("tableName")
                .field("c1",SchemaBuilder.string().build())
                .field("c2", SchemaBuilder.string().build())
                .build();
        // build record
        String param0 = "1001";
        Struct struct= new Struct(schema);
        struct.put("c1",param0);
        struct.put("c2",String.format("test-data-%s", param0));

        Schema schema2 = SchemaBuilder.struct()
            .name("t1")
            .field("c1",SchemaBuilder.string().build())
            .field("c2", SchemaBuilder.string().build())
            .build();
        // build record
        Struct struct2= new Struct(schema2);
        struct.put("c1",param0);
        struct.put("c2",String.format("test-data-%s", param0));

        for (int i = 0; i < 4; i++) {
            ConnectRecord record = new ConnectRecord(
                // offset partition
                // offset partition"
                new RecordPartition(new ConcurrentHashMap<>()),
                new RecordOffset(new HashMap<>()),
                System.currentTimeMillis(),
                schema,
                struct
            );
            records.add(record);

            ConnectRecord record2 = new ConnectRecord(
                // offset partition
                // offset partition"
                new RecordPartition(new ConcurrentHashMap<>()),
                new RecordOffset(new HashMap<>()),
                System.currentTimeMillis(),
                schema2,
                struct
            );
            records.add(record2);

        }

        HBaseSinkTask task = new HBaseSinkTask();
        KeyValue config = new DefaultKeyValue();
        config.put(HBaseConstants.COLUMN_FAMILY, columnFamily);
        config.put(HBaseConstants.HBASE_ZK_QUORUM, zkHost);
        config.put(HBaseConstants.HBASE_USERNAME_CONFIG, userName);
        config.put(HBaseConstants.HBASE_PASSWORD_CONFIG, password);

        task.start(config);
        task.put(records);
    }

    @Test
    public void testClient() {
        String tableName = "test";
        HBaseSinkConfig config = new HBaseSinkConfig();
        config.setColumnFamily(columnFamily);
        config.setZkQuorum(zkHost);
        config.setUserName(userName);
        config.setPassWord(password);
        HBaseHelperClient helperClient = new HBaseHelperClient(config);
        boolean flag = helperClient.tableExists(tableName);
        Assert.assertFalse(flag);
    }

    @Test
    public void testConn() {
        Socket connect = new Socket();
        try {
            connect.connect(new InetSocketAddress("localhost", 2181),100);//建立连接
            boolean res = connect.isConnected();//通过现有方法查看连通状态
            System.out.println(res);//true为连通
        } catch (IOException e) {
            System.out.println("false");//当连不通时，直接抛异常，异常捕获即可
        }finally{
            try {
                connect.close();
            } catch (IOException e) {
                System.out.println("false");
            }
        }
    }



}