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

package org.apache.rocketmq.connect.clickhouse.source;

import com.clickhouse.data.ClickHouseColumn;
import com.clickhouse.data.ClickHouseRecord;
import com.clickhouse.data.value.UnsignedByte;
import com.clickhouse.data.value.UnsignedInteger;
import com.clickhouse.data.value.UnsignedShort;
import io.openmessaging.KeyValue;
import io.openmessaging.connector.api.component.task.source.SourceTask;
import io.openmessaging.connector.api.data.ConnectRecord;
import io.openmessaging.connector.api.data.Field;
import io.openmessaging.connector.api.data.RecordOffset;
import io.openmessaging.connector.api.data.RecordPartition;
import io.openmessaging.connector.api.data.Schema;
import io.openmessaging.connector.api.data.SchemaBuilder;
import io.openmessaging.connector.api.data.Struct;
import io.openmessaging.internal.DefaultKeyValue;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.rocketmq.connect.clickhouse.ClickHouseHelperClient;
import org.apache.rocketmq.connect.clickhouse.config.ClickHouseConstants;
import org.apache.rocketmq.connect.clickhouse.config.ClickHouseSourceConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClickHouseSourceTask extends SourceTask {

    private static final Logger log = LoggerFactory.getLogger(ClickHouseSourceTask.class);

    private ClickHouseSourceConfig config;

    private ClickHouseHelperClient helperClient;

    @Override public List<ConnectRecord> poll() {
        List<ConnectRecord> res = new ArrayList<>();
        long offset = readRecordOffset();
        String sql = buildSql(config.getTable(), ClickHouseConstants.MAX_NUMBER_SEND_CONNECT_RECORD_EACH_TIME, offset);

        try {
            List<ClickHouseRecord> recordList = helperClient.query(sql);
            for (ClickHouseRecord clickHouseRecord : recordList) {
                res.add(clickHouseRecord2ConnectRecord(clickHouseRecord, ++offset));
            }
        } catch (Exception e) {
            log.error(String.format("Fail to poll data from clickhouse! Table=%s offset=%d", config.getTable(), offset));
        }
        return res;
    }

    private long readRecordOffset() {
        final RecordOffset positionInfo = this.sourceTaskContext.offsetStorageReader().readOffset(buildRecordPartition(config.getTable()));
        if (positionInfo == null) {
            return 0;
        }
        Object offset = positionInfo.getOffset().get(config.getTable() + "_" + ClickHouseConstants.CLICKHOUSE_OFFSET);
        return offset == null ? 0 : Long.parseLong(offset.toString());
    }

    private String buildSql(String table, int maxNum, long offset) {
        return String.format("SELECT * FROM `%s` LIMIT %d OFFSET %d;", table, maxNum, offset);
    }

    private ConnectRecord clickHouseRecord2ConnectRecord(ClickHouseRecord clickHouseRecord,
        long offset) throws NoSuchFieldException, IllegalAccessException {
        Schema schema = SchemaBuilder.struct().name(config.getTable()).build();
        final List<Field> fields = buildFields(clickHouseRecord);
        schema.setFields(fields);
        final ConnectRecord connectRecord = new ConnectRecord(buildRecordPartition(config.getTable()),
            buildRecordOffset(offset),
            System.currentTimeMillis(),
            schema,
            this.buildPayLoad(fields, schema, clickHouseRecord));
        connectRecord.setExtensions(this.buildExtensions(clickHouseRecord));
        return connectRecord;
    }

    private List<Field> buildFields(
        ClickHouseRecord clickHouseRecord) throws NoSuchFieldException, IllegalAccessException {
        java.lang.reflect.Field columns = clickHouseRecord.getClass().getDeclaredField("columns");
        columns.setAccessible(true);
        List<Field> fields = new ArrayList<>();
        for (ClickHouseColumn column : (Iterable<? extends ClickHouseColumn>) columns.get(clickHouseRecord)) {
            fields.add(new Field(column.getColumnIndex(), column.getColumnName(), getSchema(column.getDataType().getObjectClass())));
        }
        return fields;
    }

    private RecordPartition buildRecordPartition(String partitionValue) {
        Map<String, String> partitionMap = new HashMap<>();
        partitionMap.put(ClickHouseConstants.CLICKHOUSE_PARTITION, partitionValue);
        return new RecordPartition(partitionMap);
    }

    private Struct buildPayLoad(List<Field> fields, Schema schema, ClickHouseRecord clickHouseRecord) {
        Struct payLoad = new Struct(schema);
        for (int i = 0; i < fields.size(); i++) {
            payLoad.put(fields.get(i), clickHouseRecord.getValue(i).asObject());
        }
        return payLoad;
    }

    private KeyValue buildExtensions(ClickHouseRecord clickHouseRecord) {
        KeyValue keyValue = new DefaultKeyValue();
        String topicName = config.getTopic();
        if (topicName == null || topicName.equals("")) {
            String connectorName = this.sourceTaskContext.getConnectorName();
            topicName = config.getTable() + "_" + connectorName;
        }
        keyValue.put(ClickHouseConstants.TOPIC, topicName);
        return keyValue;
    }

    private RecordOffset buildRecordOffset(long offset) {
        Map<String, Long> offsetMap = new HashMap<>();
        offsetMap.put(config.getTable() + "_" + ClickHouseConstants.CLICKHOUSE_OFFSET, offset);
        return new RecordOffset(offsetMap);
    }

    private static Schema getSchema(Class clazz) {
        if (clazz.equals(Byte.class)) {
            return SchemaBuilder.int8().build();
        } else if (clazz.equals(Short.class) || clazz.equals(UnsignedByte.class)) {
            return SchemaBuilder.int16().build();
        } else if (clazz.equals(Integer.class) || clazz.equals(UnsignedShort.class)) {
            return SchemaBuilder.int32().build();
        } else if (clazz.equals(Long.class) || clazz.equals(UnsignedInteger.class)) {
            return SchemaBuilder.int64().build();
        } else if (clazz.equals(Float.class)) {
            return SchemaBuilder.float32().build();
        } else if (clazz.equals(Double.class)) {
            return SchemaBuilder.float64().build();
        } else if (clazz.equals(String.class)) {
            return SchemaBuilder.string().build();
        } else if (clazz.equals(Date.class) || clazz.equals(LocalDateTime.class) || clazz.equals(LocalDate.class)) {
            return SchemaBuilder.time().build();
        } else if (clazz.equals(Timestamp.class)) {
            return SchemaBuilder.timestamp().build();
        } else if (clazz.equals(Boolean.class)) {
            return SchemaBuilder.bool().build();
        }
        return SchemaBuilder.string().build();
    }

    @Override public void start(KeyValue keyValue) {
        this.config = new ClickHouseSourceConfig();
        this.config.load(keyValue);
        this.helperClient = new ClickHouseHelperClient(this.config);
        if (!helperClient.ping()) {
            throw new RuntimeException("Cannot connect to clickhouse server!");
        }
    }

    @Override public void stop() {
        this.helperClient = null;
    }
}
