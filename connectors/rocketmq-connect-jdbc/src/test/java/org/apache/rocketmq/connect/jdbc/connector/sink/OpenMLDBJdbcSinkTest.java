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
package org.apache.rocketmq.connect.jdbc.connector.sink;

import static org.junit.Assert.assertNotNull;

/**
 * Unit test
 */
public class OpenMLDBJdbcSinkTest {

//    private static BaseSinkTask openJDBCSinkTask = new BaseSinkTask();
//    private String dbName = "openmldb_db_test_01";
//    private String tableName = "openmldb_table_test_02";
//    // local
//    private String zk = "localhost:2181";
//    private String zkPath = "/openmldb_cluster";
//    private String jdbcUrl = String.format("jdbc:openmldb:///?zk=%s&zkPath=%s", zk, zkPath);
//    // set db name
//    private String jdbcUrlDB = String.format("jdbc:openmldb:///%s?zk=%s&zkPath=%s", dbName, zk, zkPath);
//
//    private Connection connection = null;
//
//    @Before
//    public void testOpenMLDBJdbcSinkTest() {
//        try {
//            Class.forName("com._4paradigm.openmldb.jdbc.SQLDriver");
//            connection = DriverManager.getConnection(jdbcUrl);
//            Statement stmt = connection.createStatement();
//            stmt.execute("create database if not exists " + dbName);
//            stmt.execute(String.format("use %s", dbName));
//            stmt.execute(String.format("create table if not exists %s(c1 int, c2 string)", tableName));
//        } catch (SQLException | ClassNotFoundException e) {
//            e.printStackTrace();
//        }
//        assertNotNull(connection);
//        KeyValue config = buildConfig(jdbcUrlDB);
//        openJDBCSinkTask.start(config);
//    }
//
//    @After
//    public void close() {
//        try {
//            connection.close();
//        } catch (SQLException e) {
//            e.printStackTrace();
//        }
//    }
//
//    private KeyValue buildConfig(String jdbcUrl) {
//        KeyValue config = new DefaultKeyValue();
//        config.put("connector-class", "org.apache.rocketmq.connect.jdbc.sink.JdbcSinkConnector");
//        config.put("max-task", "1");
//        config.put("connection.url", jdbcUrl);
////        config.put("pk.fields","c1");
////        config.put("pk.mode","record_value");
//        config.put("insert.mode", "INSERT");
//        config.put("db.timezone", "UTC");
//        config.put("table.types", "TABLE");
//        config.put("auto.create", "true");
//        config.put("source-record-converter", "org.apache.rocketmq.connect.runtime.converter.JsonConverter");
//        return config;
//    }
//
//    /**
//     * sink writer test
//     */
//    @Test
//    public void testOpenMLDBJdbcSinkWriterTest() throws SQLException {
//        List<ConnectRecord> records = new ArrayList<>();
//        // build schema
//        Schema schema = SchemaBuilder.struct()
//                .name(tableName)
//                .field("c1",SchemaBuilder.int32().build())
//                .field("c2", SchemaBuilder.string().build())
//                .build();
//        // build record
//        int param0 = 1001;
//        Struct struct= new Struct(schema);
//        struct.put("c1",param0);
//        struct.put("c2",String.format("test-data-%s", param0));
//
//        ConnectRecord record = new ConnectRecord(
//                // offset partition
//                // offset partition"
//                new RecordPartition(new ConcurrentHashMap<>()),
//                new RecordOffset(new HashMap<>()),
//                System.currentTimeMillis(),
//                schema,
//                struct
//        );
//        records.add(record);
//        openJDBCSinkTask.put(records);
//        testOpenMLDBSelect();
//    }
//
//    public void testOpenMLDBSelect() throws SQLException {
//        Statement stmt = this.connection.createStatement();
//        ResultSet rs = stmt.executeQuery(String.format("SELECT * from %s", tableName));
//        while (rs.next()) {
//            System.out.println(rs.getInt(1) + "-------" + rs.getString(2));
//        }
//    }

}
