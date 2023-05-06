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

package org.apache.rocketmq.connect.clickhouse;

import com.clickhouse.client.ClickHouseClient;
import com.clickhouse.client.ClickHouseCredentials;
import com.clickhouse.client.ClickHouseNode;
import com.clickhouse.client.ClickHouseProtocol;
import com.clickhouse.client.ClickHouseResponse;
import com.clickhouse.data.ClickHouseFormat;
import com.clickhouse.data.ClickHouseRecord;
import java.util.ArrayList;
import java.util.List;
import org.apache.rocketmq.connect.clickhouse.config.ClickHouseConstants;
import org.apache.rocketmq.connect.clickhouse.config.ClickHouseBaseConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClickHouseHelperClient {

    private static final Logger LOGGER = LoggerFactory.getLogger(ClickHouseHelperClient.class);

    private ClickHouseBaseConfig config;
    private int timeout = ClickHouseConstants.timeoutSecondsDefault * ClickHouseConstants.MILLI_IN_A_SEC;
    private ClickHouseNode server = null;
    private int retry = ClickHouseConstants.retryCountDefault;

    public ClickHouseHelperClient(ClickHouseBaseConfig config) {
        this.config = config;
        this.server = create(config);
    }

    private ClickHouseNode create(ClickHouseBaseConfig config) {
        this.server = ClickHouseNode.builder()
            .host(config.getClickHouseHost())
            .port(ClickHouseProtocol.HTTP, config.getClickHousePort())
            .database(config.getDatabase()).credentials(getCredentials(config))
            .build();

        return this.server;
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

    public boolean ping() {
        ClickHouseClient clientPing = ClickHouseClient.newInstance(ClickHouseProtocol.HTTP);
        LOGGER.debug(String.format("server [%s] , timeout [%d]", server, timeout));
        int retryCount = 0;

        while (retryCount < retry) {
            if (clientPing.ping(server, timeout)) {
                clientPing.close();
                return true;
            }
            retryCount++;
            LOGGER.warn(String.format("Ping retry %d out of %d", retryCount, retry));
        }
        LOGGER.error("unable to ping to clickhouse server. ");
        clientPing.close();
        return false;
    }

    public ClickHouseNode getServer() {
        return this.server;
    }

    public List<ClickHouseRecord> query(String query) {
        return query(query, ClickHouseFormat.RowBinaryWithNamesAndTypes);
    }

    public List<ClickHouseRecord> query(String query, ClickHouseFormat clickHouseFormat) {
        int retryCount = 0;
        Exception ce = null;
        while (retryCount < retry) {
            try (ClickHouseClient client = ClickHouseClient.newInstance(ClickHouseProtocol.HTTP);
                 ClickHouseResponse response = client.read(server)
                     .format(clickHouseFormat)
                     .query(query)
                     .execute().get()) {

                List<ClickHouseRecord> recordList = new ArrayList<>();
                for (ClickHouseRecord r : response.records()) {
                    recordList.add(r);
                }
                return recordList;

            } catch (Exception e) {
                retryCount++;
                LOGGER.warn(String.format("Query retry %d out of %d", retryCount, retry), e);
                ce = e;
            }
        }
        throw new RuntimeException(ce);

    }

//    public List<String> showTables() {
//        List<String> tablesNames = new ArrayList<>();
//        try (ClickHouseClient client = ClickHouseClient.newInstance(ClickHouseProtocol.HTTP);
//             ClickHouseResponse response = client.connect(server) // or client.connect(endpoints)
//                 // you'll have to parse response manually if using a different format
//
//                 .query("SHOW TABLES")
//                 .executeAndWait()) {
//            for (ClickHouseRecord r : response.records()) {
//                ClickHouseValue v = r.getValue(0);
//                String tableName = v.asString();
//                tablesNames.add(tableName);
//            }
//
//        } catch (ClickHouseException e) {
//            LOGGER.error("Failed in show tables", e);
//        }
//        return tablesNames;
//    }

//    public Table describeTable(String tableName) {
//        if (tableName.startsWith(".inner"))
//            return null;
//        String describeQuery = String.format("DESCRIBE TABLE `%s`.`%s`", this.database, tableName);
//
//        try (ClickHouseClient client = ClickHouseClient.newInstance(ClickHouseProtocol.HTTP);
//             ClickHouseResponse response = client.connect(server) // or client.connect(endpoints)
//                 .query(describeQuery)
//                 .executeAndWait()) {
//            Table table = new Table(tableName);
//            for (ClickHouseRecord r : response.records()) {
//                ClickHouseValue v = r.getValue(0);
//                String value = v.asString();
//                String[] cols = value.split("\t");
//                if (cols.length > 2) {
//                    String defaultKind = cols[2];
//                    if ("ALIAS".equals(defaultKind) || "MATERIALIZED".equals(defaultKind)) {
//                        // Only insert into "real" columns
//                        continue;
//                    }
//                }
//                String name = cols[0];
//                String type = cols[1];
//                table.addColumn(Column.extractColumn(name, type, false));
//            }
//            return table;
//        } catch (ClickHouseException e) {
//            LOGGER.error(String.format("Got exception when running %s", describeQuery), e);
//            return null;
//        }
//
//    }
//    public List<Table> extractTablesMapping() {
//        List<Table> tableList =  new ArrayList<>();
//        for (String tableName : showTables() ) {
//            Table table = describeTable(tableName);
//            if (table != null )
//                tableList.add(table);
//        }
//        return tableList;
//    }

//    public static class ClickHouseClientBuilder{
//        private String hostname = null;
//        private int port = -1;
//        private String username = "default";
//        private String database = "default";
//        private String password = "";
//        private boolean sslEnabled = false;
//        private int timeout = ClickHouseSinkConfig.timeoutSecondsDefault * ClickHouseSinkConfig.MILLI_IN_A_SEC;
//        private int retry = ClickHouseSinkConfig.retryCountDefault;
//        public ClickHouseClientBuilder(String hostname, int port) {
//            this.hostname = hostname;
//            this.port = port;
//        }
//
//
//        public ClickHouseClientBuilder setUsername(String username) {
//            this.username = username;
//            return this;
//        }
//
//        public ClickHouseClientBuilder setPassword(String password) {
//            this.password = password;
//            return this;
//        }
//
//        public ClickHouseClientBuilder setDatabase(String database) {
//            this.database = database;
//            return this;
//        }
//
//        public ClickHouseClientBuilder sslEnable(boolean sslEnabled) {
//            this.sslEnabled = sslEnabled;
//            return this;
//        }
//
//        public ClickHouseClientBuilder setTimeout(int timeout) {
//            this.timeout = timeout;
//            return this;
//        }
//
//        public ClickHouseClientBuilder setRetry(int retry) {
//            this.retry = retry;
//            return this;
//        }
//
//        public ClickHouseHelperClient build(){
//            return new ClickHouseHelperClient(this);
//        }
//
//    }
}
