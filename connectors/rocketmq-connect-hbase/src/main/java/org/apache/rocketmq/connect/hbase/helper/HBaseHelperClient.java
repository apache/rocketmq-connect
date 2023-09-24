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
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.rocketmq.connect.hbase.config.HBaseConstants;
import org.apache.rocketmq.connect.hbase.config.HBaseSinkConfig;

import java.util.List;

@Slf4j
public class HBaseHelperClient {

    private Configuration configuration;

    private Connection connection;

    private HBaseAdmin admin;


    public boolean tableExists(String tableName) {
        try {
            if (admin == null) {
                return false;
            }
            return admin.tableExists(TableName.valueOf(tableName));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void createTable(String tableName, List<String> columnFamilies) {
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
        }
        try {
            //创建表
            admin.createTable(hTableDescriptor);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void batchInsert(String tableName, List<Put> puts) {
        try {
            Table table = connection.getTable(TableName.valueOf(tableName));
            table.put(puts);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public HBaseHelperClient(HBaseSinkConfig hBaseSinkConfig) {
        getConnection(hBaseSinkConfig);
        initAdmin();
    }

    private void getConnection(HBaseSinkConfig hBaseSinkConfig) {
        configuration = HBaseConfiguration.create();
        configuration.set(HBaseConstants.HBASE_ZOOKEEPER_QUORUM, hBaseSinkConfig.getZkQuorum());

        if (StringUtils.isNotBlank(hBaseSinkConfig.getHbaseMaster())) {
            configuration.set(HBaseConstants.HBASE_MASTER, hBaseSinkConfig.getHbaseMaster());
        }
        if (StringUtils.isNotBlank(hBaseSinkConfig.getUserName())) {
            configuration.set(HBaseConstants.HBASE_CLIENT_USERNAME, hBaseSinkConfig.getUserName());
        }
        if (StringUtils.isNotBlank(hBaseSinkConfig.getPassWord())) {
            configuration.set(HBaseConstants.HBASE_CLIENT_PASSWORD, hBaseSinkConfig.getPassWord());
        }
        try {
            connection = ConnectionFactory.createConnection(configuration);
        } catch (Exception e) {
            throw new RuntimeException("Cannot connect to hbase server! ", e);
        }
    }

    private void initAdmin() {
        if (connection == null) {
            throw new RuntimeException("Cannot connect to hbase server!");
        }
        try {
            admin = (HBaseAdmin) connection.getAdmin();
        } catch (Exception e) {
            throw new RuntimeException("Cannot get admin from hbase server! ", e);
        }
    }

    public void close() {
        try {
            admin.close();
            connection.close();
        } catch (Exception e) {
            log.error("error when closing", e);
        }

    }
}
