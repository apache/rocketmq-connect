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

package org.apache.rocketmq.connect.hive.connector;

import com.alibaba.fastjson.JSON;
import io.openmessaging.KeyValue;
import io.openmessaging.connector.api.component.task.source.SourceTask;
import io.openmessaging.connector.api.data.ConnectRecord;
import io.openmessaging.connector.api.data.Field;
import io.openmessaging.connector.api.data.RecordOffset;
import io.openmessaging.connector.api.data.RecordPartition;
import io.openmessaging.connector.api.data.Schema;
import io.openmessaging.connector.api.data.SchemaBuilder;
import io.openmessaging.connector.api.data.Struct;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.connect.hive.config.HiveColumn;
import org.apache.rocketmq.connect.hive.config.HiveConfig;
import org.apache.rocketmq.connect.hive.config.HiveConstant;
import org.apache.rocketmq.connect.hive.config.HiveJdbcDriverManager;
import org.apache.rocketmq.connect.hive.config.HiveRecord;
import org.apache.rocketmq.connect.hive.config.SchemaManger;
import org.apache.rocketmq.connect.hive.replicator.source.HiveReplicator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HiveSourceTask extends SourceTask {

    private static final Logger log = LoggerFactory.getLogger(HiveSourceTask.class);

    private HiveConfig config;

    private HiveReplicator replicator;

    private KeyValue keyValue;

    @Override
    public List<ConnectRecord> poll() {
        List<ConnectRecord> res = new ArrayList<>();
        try {
            HiveRecord record = replicator.getQueue().poll(1000, TimeUnit.MILLISECONDS);
            if (record != null) {
                res.add(hiveRecord2ConnectRecord(record));
            }
        } catch (Exception e) {
            log.error("hive sourceTask poll error, current config:" + JSON.toJSONString(config), e);
        }
        return res;
    }

    @Override
    public void start(KeyValue keyValue) {
        this.keyValue = keyValue;
        final RecordOffset recordOffset = this.sourceTaskContext.offsetStorageReader().readOffset(buildRecordPartition());
        this.config = new HiveConfig();
        this.config.load(keyValue);
        this.replicator = new HiveReplicator(config);
        this.replicator.start(recordOffset, keyValue);
        log.info("hive source task start success");
    }

    @Override
    public void stop() {
        replicator.stop();
        HiveJdbcDriverManager.destroy();
        log.info("hive source task stop success");
    }

    private ConnectRecord hiveRecord2ConnectRecord(HiveRecord record) {
        Schema schema = SchemaBuilder.struct().name(record.getTableName()).build();
        final List<Field> fields = buildFields(record);
        schema.setFields(fields);
        return new ConnectRecord(buildRecordPartition(),
            buildRecordOffset(record),
            System.currentTimeMillis(),
            schema,
            this.buildPayLoad(fields, schema, record));
    }

    private List<Field> buildFields(HiveRecord record) {
        List<Field> fields = new ArrayList<>();
        final List<HiveColumn> columnList = record.getRecord();
        for (int i = 0; i < columnList.size(); i++) {
            final HiveColumn entity = columnList.get(i);
            fields.add(new Field(i, entity.getColumnName(), SchemaManger.getSchema(entity.getColumnValue())));
        }

        return fields;
    }

    private RecordOffset buildRecordOffset(HiveRecord record) {
        Map<String, Object> offsetMap = new HashMap<>();
        RecordOffset recordOffset = new RecordOffset(offsetMap);
        final List<HiveColumn> columnList = record.getRecord();
        final Optional<HiveColumn> column = columnList.stream().filter(item -> item.getColumnName().equals(this.keyValue.getString(record.getTableName()))).findFirst();
        if (column.isPresent()) {
            offsetMap.put(record.getTableName() + ":" + HiveConstant.HIVE_POSITION, column.get().getColumnValue());
        }

        return recordOffset;
    }

    private Struct buildPayLoad(List<Field> fields, Schema schema, HiveRecord record) {
        Struct payLoad = new Struct(schema);
        final List<HiveColumn> columnList = record.getRecord();
        for (int i = 0; i < fields.size(); i++) {
            payLoad.put(fields.get(i), columnList.get(i).getColumnValue());
        }
        return payLoad;
    }

    private RecordPartition buildRecordPartition() {
        Map<String, String> partitionMap = new HashMap<>();
        partitionMap.put(HiveConstant.HIVE_PARTITION, HiveConstant.HIVE_PARTITION);
        RecordPartition  recordPartition = new RecordPartition(partitionMap);
        return recordPartition;
    }

}
