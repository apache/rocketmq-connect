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

package org.apache.rocketmq.connect.hbase.helper;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;

import java.io.IOException;
import java.util.List;
import java.util.Map;

@Slf4j
public class HBaseHelperClient {

    private Configuration configuration;

    private Connection connection;

    private HBaseAdmin admin;


    public boolean tableExists(String tableName) throws IOException {
        if (admin == null) {
            return false;
        }
        return admin.tableExists(TableName.valueOf(tableName));
    }

    public void createTable(String tableName, List<String> columnFamilies) throws IOException {
        if (tableExists(tableName)) {
            log.warn("table already exist, {}", tableName);
            return;
        }

        HTableDescriptor hTableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
        //添加列族
        if (CollectionUtils.isNotEmpty(columnFamilies)) {
            columnFamilies.forEach(columnFamily -> {
                HColumnDescriptor f = new HColumnDescriptor(columnFamily);
                hTableDescriptor.addFamily(f);
            });
            //创建表
            admin.createTable(hTableDescriptor);
        }
    }

    public void batchInsert(String tableName, List<Put> puts) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));
        table.put(puts);
    }

    public HBaseHelperClient(Map<String, String> confMap) {
        getConnection(confMap);
        initAdmin();
    }

    private void getConnection(Map<String, String> confMap) {
        configuration = HBaseConfiguration.create();
        if (!confMap.isEmpty()) {
            confMap.forEach((confKey, confValue) -> {
                configuration.set(confKey, confValue);
            });
        }
        try {
            connection = ConnectionFactory.createConnection(configuration);
        } catch (Exception e) {
            log.error("get connection with hbase error ", e);
        }
    }

    private void initAdmin() {
        if (connection == null) {
            log.error("cannot get connection with hbase");
            return;
        }
        try {
            admin = (HBaseAdmin) connection.getAdmin();
        } catch (Exception e) {
            log.error("cannot get hbase admin ", e);
        }
    }

    private void close() {
        try {
            admin.close();
            connection.close();
        } catch (Exception e) {
            log.error("error when closing", e);
        }

    }
}
