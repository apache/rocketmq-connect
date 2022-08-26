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
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.rocketmq.connect.deltalake.rolling;

import io.openmessaging.connector.api.data.RecordPartition;
import io.openmessaging.connector.api.data.RecordPosition;
import org.apache.rocketmq.connect.deltalake.config.DeltalakeConnectConfig;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.UUID;

/**
 * @author osgoo
 * @date 2022/8/23
 */
public class DailyRolling implements StoreFileRolling {
    private DeltalakeConnectConfig deltalakeConnectConfig;
    // todo support partition columns
    private String additionalPartitionColumns;
    private final static String FILE_SPLITER = "/";
    private final static String FILE_CONCATOR = "_";
    public DailyRolling(DeltalakeConnectConfig deltalakeConnectConfig) {
        this.deltalakeConnectConfig = deltalakeConnectConfig;
        this.additionalPartitionColumns = deltalakeConnectConfig.getAdditionalPartitionColumns();
    }

    @Override
    public String generateTableDir(RecordPosition recordPosition) {
        RecordPartition recordPartition = recordPosition.getPartition();
        String topic = (String) recordPartition.getPartition().get("topic");
        return FILE_SPLITER + topic;
    }

    @Override
    public String generateStoreDir(RecordPosition partition, long timestamp) {
        RecordPartition recordPartition = partition.getPartition();
        String topic = (String) recordPartition.getPartition().get("topic");
        String brokerName = (String) recordPartition.getPartition().get("brokerName");
        String queue = (String) recordPartition.getPartition().get("queueId");
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
        Date now = new Date(timestamp);
        String currentStr = simpleDateFormat.format(now);
        return FILE_SPLITER + topic + FILE_SPLITER + brokerName + FILE_CONCATOR + queue + FILE_SPLITER + currentStr;
    }

    @Override
    public String generateStoreFileName(RecordPosition partition, long timestamp) {
        RecordPartition recordPartition = partition.getPartition();
        String topic = (String) recordPartition.getPartition().get("topic");
        String brokerName = (String) recordPartition.getPartition().get("brokerName");
        String queue = (String) recordPartition.getPartition().get("queueId");
        String uuid = UUID.randomUUID().toString();
        return FILE_SPLITER + topic + FILE_CONCATOR + brokerName + FILE_CONCATOR + queue + FILE_CONCATOR + uuid + FILE_CONCATOR + deltalakeConnectConfig.getCompressType() + ".parquet";
    }
}
