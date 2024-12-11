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

package org.apache.rocketmq.connect.doris;

import io.openmessaging.KeyValue;
import io.openmessaging.connector.api.data.ConnectRecord;
import io.openmessaging.connector.api.data.RecordOffset;
import io.openmessaging.connector.api.data.RecordPartition;
import io.openmessaging.internal.DefaultKeyValue;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.rocketmq.connect.doris.cfg.DorisSinkConnectorConfig;
import org.apache.rocketmq.connect.doris.utils.ConnectRecordUtil;
import org.apache.rocketmq.connect.doris.writer.load.LoadModel;
import org.apache.rocketmq.connect.runtime.converter.record.json.JsonConverter;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class DorisSinkTaskTest {

    private final DorisSinkTask dorisSinkTask = new DorisSinkTask();
    private final JsonConverter jsonConverter = new JsonConverter();
    private KeyValue keyValue;
    private RecordPartition recordPartition;
    private RecordOffset offset;

    @Before
    public void init() {
        keyValue = new DefaultKeyValue();
        keyValue.put("task_id", "0");
        keyValue.put(DorisSinkConnectorConfig.DORIS_URLS, "127.0.0.1");
        keyValue.put(DorisSinkConnectorConfig.DORIS_USER, "root");
        keyValue.put(DorisSinkConnectorConfig.DORIS_PASSWORD, "");
        keyValue.put(DorisSinkConnectorConfig.DORIS_HTTP_PORT, "8030");
        keyValue.put(DorisSinkConnectorConfig.DORIS_QUERY_PORT, "9030");
        keyValue.put(DorisSinkConnectorConfig.DORIS_DATABASE, "test");
        keyValue.put(DorisSinkConnectorConfig.TOPICS, "rmq_test");
        keyValue.put(DorisSinkConnectorConfig.TOPICS_TABLES_MAP, "rmq_test:doris_tab");
        keyValue.put(DorisSinkConnectorConfig.ENABLE_2PC, "false");
        keyValue.put(DorisSinkConnectorConfig.BUFFER_COUNT_RECORDS, "1");
        DorisSinkConnectorConfig.setDefaultValues(keyValue);

        Map<String, String> partition = new HashMap<>();
        partition.put(ConnectRecordUtil.TOPIC, "rmq_test");
        partition.put(ConnectRecordUtil.BROKER_NAME, "broker_test");
        partition.put(ConnectRecordUtil.QUEUE_ID, "111");
        recordPartition = new RecordPartition(partition);

        Map<String, String> queueOffset = new HashMap<>();
        queueOffset.put("queueOffset", "1");
        offset = new RecordOffset(queueOffset);
        jsonConverter.configure(new HashMap<>());
    }

    @Test
    public void testPut() {
        keyValue.put(DorisSinkConnectorConfig.ENABLE_2PC, "false");
        dorisSinkTask.start(keyValue);
        List<ConnectRecord> connectRecordList = new ArrayList<>();
        ConnectRecord connectRecord = new ConnectRecord(recordPartition, offset, System.currentTimeMillis());
        connectRecord.setData("{\"id\":1,\"name\":\"lisi\",\"age\":12}");
        connectRecordList.add(connectRecord);
        connectRecordList.add(connectRecord);
        dorisSinkTask.put(connectRecordList);
    }

    @Test
    public void testPutAndFlush() {
        dorisSinkTask.start(keyValue);

        List<ConnectRecord> connectRecordList = new ArrayList<>();
        ConnectRecord connectRecord = new ConnectRecord(recordPartition, offset, System.currentTimeMillis());
        connectRecord.setData("{\"id\":2,\"name\":\"zhangsan\",\"age\":13}");
        connectRecordList.add(connectRecord);
        connectRecordList.add(connectRecord);
        dorisSinkTask.put(connectRecordList);
        Map<RecordPartition, RecordOffset> currentOffsets = new HashMap<>();
        currentOffsets.put(recordPartition, offset);
        dorisSinkTask.flush(currentOffsets);
    }

    @Test
    public void testCustomClusterProxy() {
        keyValue.put(DorisSinkConnectorConfig.DORIS_CUSTOM_CLUSTER, "true");
        keyValue.put(DorisSinkConnectorConfig.SOCKS5_ENDPOINT, "");
        keyValue.put(DorisSinkConnectorConfig.SOCKS5_USERNAME, "");
        keyValue.put(DorisSinkConnectorConfig.SOCKET5_PASSWORD, "");
        dorisSinkTask.start(keyValue);

        List<ConnectRecord> connectRecordList = new ArrayList<>();
        ConnectRecord connectRecord = new ConnectRecord(recordPartition, offset, System.currentTimeMillis());
        connectRecord.setData("{\"id\":2,\"name\":\"zhangsan\",\"age\":13}");
        connectRecordList.add(connectRecord);
        connectRecordList.add(connectRecord);
        dorisSinkTask.put(connectRecordList);
        Map<RecordPartition, RecordOffset> currentOffsets = new HashMap<>();
        currentOffsets.put(recordPartition, offset);
        dorisSinkTask.flush(currentOffsets);
    }

    //    @Test
    //    public void testDebeziumConverterPut() {
    //        keyValue.put(DorisSinkConnectorConfig.CONVERTER_MODE, ConverterMode.DEBEZIUM_INGESTION.getName());
    //        dorisSinkTask.start(keyValue);
    //
    //        List<ConnectRecord> connectRecordList = new ArrayList<>();
    //        ConnectRecord connectRecord = new ConnectRecord(recordPartition, offset, System.currentTimeMillis());
    //        String msg = "";
    //        SchemaAndValue schemaAndValue = jsonConverter.toConnectData("a", msg.getBytes(StandardCharsets.UTF_8));
    //
    //        connectRecord.setData(schemaAndValue.value());
    //        connectRecord.setSchema(schemaAndValue.schema());
    //        connectRecordList.add(connectRecord);
    //        connectRecordList.add(connectRecord);
    //        dorisSinkTask.put(connectRecordList);
    //    }

    @Test
    public void testCopyIntoLoad() {
        keyValue.put(DorisSinkConnectorConfig.LOAD_MODEL, LoadModel.COPY_INTO.getName());
        dorisSinkTask.start(keyValue);

        List<ConnectRecord> connectRecordList = new ArrayList<>();
        ConnectRecord connectRecord = new ConnectRecord(recordPartition, offset, System.currentTimeMillis());
        connectRecord.setData("{\"id\":4,\"name\":\"zhaoliu\",\"age\":14}");
        connectRecordList.add(connectRecord);
        connectRecordList.add(connectRecord);
        dorisSinkTask.put(connectRecordList);
        Map<RecordPartition, RecordOffset> currentOffsets = new HashMap<>();
        currentOffsets.put(recordPartition, offset);
        dorisSinkTask.flush(currentOffsets);
    }

}
