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

package org.apache.rocketmq.connect.hive.config;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HiveJdbcDriverManager {

    private static final Logger log = LoggerFactory.getLogger(HiveJdbcDriverManager.class);

    private static Connection conn = null;
    private static Statement stmt = null;

    public synchronized static void init(HiveConfig config) {
        if (stmt != null) {
            return;
        }
        try {
            Class.forName(HiveConstant.DRIVER_NAME);
        } catch (ClassNotFoundException e) {
            log.error("load Hive Driver failed", e);
        }
        String jdbcUrl = HiveConstant.URL_PREFIX + config.getHost() + ":" + config.getPort() + "/" + config.getDatabase();
        try {
            if (StringUtils.isEmpty(config.getUsername())) {
                conn = java.sql.DriverManager.getConnection(jdbcUrl, null, null);
            } else {
                conn = java.sql.DriverManager.getConnection(jdbcUrl, config.getUsername(), config.getPassword());
            }
            stmt = conn.createStatement();
        } catch (SQLException e) {
            log.error("get hive connection failed, Please check parameters", e);
        }
    }

    public static void destroy() {
        try {
            if (stmt != null) {
                stmt.close();
            }
            if (conn != null) {
                conn.close();
            }
        } catch (SQLException e) {
            log.error("stop HiveSourceConnector failed", e);
        }
    }

    public static Statement getStatement() {
        return stmt;
    }
}
