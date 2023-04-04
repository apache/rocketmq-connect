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

package org.apache.rocketmq.connect.hive.replicator.source;

import io.openmessaging.KeyValue;
import io.openmessaging.connector.api.data.RecordOffset;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.hive.jdbc.HiveQueryResultSet;
import org.apache.rocketmq.connect.hive.config.HiveConstant;
import org.apache.rocketmq.connect.hive.config.HiveJdbcDriverManager;
import org.apache.rocketmq.connect.hive.config.HiveColumn;
import org.apache.rocketmq.connect.hive.config.HiveRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HiveQuery {

    private Logger log = LoggerFactory.getLogger(this.getClass());

    private HiveReplicator replicator;

    private transient Statement statement;

    public HiveQuery(HiveReplicator replicator) {
        this.replicator = replicator;
    }

    public void start(RecordOffset recordOffset, KeyValue keyValue) {
        if (statement == null) {
            statement = HiveJdbcDriverManager.getStatement();
        }
        final String tableName = keyValue.getString(HiveConstant.TABLE_NAME);
        final String incrementField = keyValue.getString(HiveConstant.INCREMENT_FIELD);
        String incrementValue = keyValue.getString(HiveConstant.INCREMENT_VALUE);
        if (recordOffset != null && recordOffset.getOffset() != null && recordOffset.getOffset().size() > 0) {
            final Object offsetValue = recordOffset.getOffset().get(keyValue.getString(HiveConstant.TABLE_NAME) + ":" + HiveConstant.HIVE_POSITION);
            incrementValue = offsetValue + "";

        }
        int startPosition = 0;
        try {
            while (true) {
                String sql = "select * from " + tableName + " where " + incrementField + " > " + incrementValue
                    + " order by " + incrementField + " limit " + startPosition + "," + (startPosition + 200) + "";
                final ResultSet rs = statement.executeQuery(sql);
                if (!rs.next()) {
                    TimeUnit.SECONDS.sleep(5);
                    continue;
                }
                while (rs.next()) {
                    final int count = rs.getMetaData().getColumnCount();
                    HiveRecord record = new HiveRecord();
                    List<HiveColumn> entityList = new ArrayList<>();
                    for (int i = 1; i <= count; i++) {
                        final Object columnValue = rs.getObject(i);
                        final String columnName = rs.getMetaData().getColumnName(i);
                        HiveColumn entity = new HiveColumn(columnName.substring(columnName.indexOf(".") + 1), columnValue);
                        entityList.add(entity);
                    }
                    record.setRecord(entityList);
                    record.setTableName(tableName);
                    replicator.commit(record);
                }
                startPosition += 200;
            }

        } catch (SQLException | InterruptedException e) {
            log.error("execute hive query failed", e);
        }
    }

    public static int countData(Statement stmt, String tableName, String incrementField, String incrementValue)
        throws SQLException {
        String sql = "select count(1) from " + tableName + " where " + incrementField + " > " + incrementValue;
        HiveQueryResultSet rs = (HiveQueryResultSet) stmt.executeQuery(sql);
        while (rs.next()) {
            return rs.getInt(1);
        }
        return 0;
    }

    public static String selectSpecifiedIndexData(Statement stmt, String tableName, String incrementField,
        String incrementValue, Integer startPosition) throws SQLException {
        String sql = "select * from " + tableName +  " where " + incrementField + " > " + incrementValue + " order by "
            + incrementField + " limit " + startPosition + ",1";
        ResultSet rs = stmt.executeQuery(sql);
        while (rs.next()) {
            final int count = rs.getMetaData().getColumnCount();
            for (int i = 1; i <= count; i++) {
                final Object object = rs.getObject(i);
                final String columnName = rs.getMetaData().getColumnName(i);
                if (columnName.substring(columnName.indexOf(".") + 1).equals(incrementField)) {
                    return object.toString();
                }
            }
        }
        return "";
    }
}
