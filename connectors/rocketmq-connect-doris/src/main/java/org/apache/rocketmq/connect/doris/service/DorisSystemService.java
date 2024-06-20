/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.rocketmq.connect.doris.service;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.Predicate;
import org.apache.rocketmq.connect.doris.cfg.DorisOptions;
import org.apache.rocketmq.connect.doris.connection.JdbcConnectionProvider;
import org.apache.rocketmq.connect.doris.exception.DorisException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DorisSystemService {

    private static final Logger LOG = LoggerFactory.getLogger(DorisSystemService.class);
    private static final String GET_COLUMN_EXISTS_TEMPLATE =
        "SELECT COLUMN_NAME FROM information_schema.`COLUMNS` WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND COLUMN_NAME = ?";
    private final JdbcConnectionProvider jdbcConnectionProvider;

    public DorisSystemService(DorisOptions dorisOptions) {
        this.jdbcConnectionProvider = new JdbcConnectionProvider(dorisOptions);
    }

    private static final List<String> builtinDatabases =
        Collections.singletonList("information_schema");

    public boolean tableExists(String database, String table) {
        return listTables(database).contains(table);
    }

    public boolean databaseExists(String database) {
        return listDatabases().contains(database);
    }

    public Set<String> listDatabases() {
        return new HashSet<>(
            extractColumnValuesBySQL(
                "SELECT `SCHEMA_NAME` FROM `INFORMATION_SCHEMA`.`SCHEMATA`;",
                1,
                dbName -> !builtinDatabases.contains(dbName)));
    }

    public Set<String> listTables(String databaseName) {
        if (!databaseExists(databaseName)) {
            throw new DorisException("database" + databaseName + " is not exists");
        }
        return new HashSet<>(
            extractColumnValuesBySQL(
                "SELECT TABLE_NAME FROM information_schema.`TABLES` WHERE TABLE_SCHEMA = ?",
                1,
                null,
                databaseName));
    }

    public boolean isColumnExist(String database, String tableName, String columnName) {
        List<String> columnList =
            extractColumnValuesBySQL(
                GET_COLUMN_EXISTS_TEMPLATE, 1, null, database, tableName, columnName);
        return !columnList.isEmpty();
    }

    public List<String> extractColumnValuesBySQL(
        String sql, int columnIndex, Predicate<String> filterFunc, Object... params) {

        List<String> columnValues = new ArrayList<>();
        try (PreparedStatement ps =
                 jdbcConnectionProvider.getOrEstablishConnection().prepareStatement(sql)) {
            if (Objects.nonNull(params) && params.length > 0) {
                for (int i = 0; i < params.length; i++) {
                    ps.setObject(i + 1, params[i]);
                }
            }
            ResultSet rs = ps.executeQuery();
            while (rs.next()) {
                String columnValue = rs.getString(columnIndex);
                if (Objects.isNull(filterFunc) || filterFunc.test(columnValue)) {
                    columnValues.add(columnValue);
                }
            }
            return columnValues;
        } catch (Exception e) {
            LOG.error("The following SQL query could not be executed: {}", sql, e);
            throw new DorisException(
                String.format("The following SQL query could not be executed: %s", sql), e);
        }
    }
}
