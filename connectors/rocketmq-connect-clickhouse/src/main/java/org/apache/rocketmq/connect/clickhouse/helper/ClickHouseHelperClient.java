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

package org.apache.rocketmq.connect.clickhouse.helper;

import com.clickhouse.client.ClickHouseClient;
import com.clickhouse.client.ClickHouseCredentials;
import com.clickhouse.client.ClickHouseNode;
import com.clickhouse.client.ClickHouseProtocol;
import com.clickhouse.client.ClickHouseResponse;
import com.clickhouse.data.ClickHouseFormat;
import com.clickhouse.data.ClickHouseOutputStream;
import com.clickhouse.data.ClickHouseRecord;
import com.clickhouse.data.ClickHouseWriter;
import com.clickhouse.jdbc.ClickHouseDataSource;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
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

    private Connection getConnection(String url, Properties properties) throws SQLException {
        ClickHouseDataSource dataSource = new ClickHouseDataSource(url, properties);
        Connection conn = dataSource.getConnection(config.getUserName(), config.getPassWord());

        System.out.println("Connected to: " + conn.getMetaData().getURL());
        return conn;
    }

    private boolean insertJson(String jsonString, String table, String sql, String url) {

        try (Connection connection = getConnection(url, new Properties());
             PreparedStatement ps = connection.prepareStatement(sql)) {
            ps.setObject(1, new ClickHouseWriter() {
                @Override
                public void write(ClickHouseOutputStream output) throws IOException {
                    output.writeBytes(jsonString.getBytes());
                }
            });
            ps.executeUpdate();

        } catch (Exception e) {
            return false;
        }
        return true;
    }

    public void insertJson(String jsonString, String table) {
        String url = String.format("jdbc:clickhouse://%s:%s/%s", config.getClickHouseHost(), config.getClickHousePort(), config.getDatabase());
        String sql = String.format("INSERT INTO %s FORMAT JSONEachRow", table);
        int retryCount = 0;

        while (retryCount < this.retry) {
            if (insertJson(jsonString, table, sql, url)) {
                return;
            }
        }
        LOGGER.error(String.format("Insert into table %s error", table));
    }

}
