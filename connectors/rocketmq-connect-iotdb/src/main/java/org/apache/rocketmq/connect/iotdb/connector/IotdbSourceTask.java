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

package org.apache.rocketmq.connect.iotdb.connector;

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
import java.util.concurrent.TimeUnit;
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.apache.rocketmq.connect.iotdb.config.IotdbConfig;
import org.apache.rocketmq.connect.iotdb.config.IotdbConstant;
import org.apache.rocketmq.connect.iotdb.config.SchemaProcessor;
import org.apache.rocketmq.connect.iotdb.replicator.source.DeviceEntity;
import org.apache.rocketmq.connect.iotdb.replicator.source.IotdbReplicator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IotdbSourceTask extends SourceTask {
    private static final Logger log = LoggerFactory.getLogger(IotdbSourceTask.class);

    private IotdbConfig config;

    private IotdbReplicator replicator;

    @Override
    public List<ConnectRecord> poll() {
        List<ConnectRecord> res = new ArrayList<>();
        try {
            DeviceEntity deviceEntity = replicator.getQueue().poll(1000, TimeUnit.MILLISECONDS);
            if (deviceEntity != null) {
                res.add(deviceEntity2ConnectRecord(deviceEntity));
            }
        } catch (Exception e) {
            log.error("iotdb sourceTask poll error, current config:" + JSON.toJSONString(config), e);
        }
        return res;
    }

    @Override
    public void start(KeyValue keyValue) {
        final RecordOffset recordOffset = this.sourceTaskContext.offsetStorageReader().readOffset(buildRecordPartition());
        this.config = new IotdbConfig();
        this.config.load(keyValue);
        this.replicator = new IotdbReplicator(config);
        this.replicator.start(recordOffset, keyValue);
        log.info("iotdb source task start success");
    }

    public ConnectRecord deviceEntity2ConnectRecord(DeviceEntity deviceEntity) {
        Schema schema = SchemaBuilder.struct().name(deviceEntity.getPath()).build();
        final List<Field> fields = buildFields(deviceEntity);
        schema.setFields(fields);
        return new ConnectRecord(buildRecordPartition(),
            buildRecordOffset(deviceEntity),
            System.currentTimeMillis(),
            schema,
            this.buildPayLoad(fields, schema, deviceEntity));
    }

    private RecordPartition buildRecordPartition() {
        Map<String, String> partitionMap = new HashMap<>();
        partitionMap.put(IotdbConstant.IOTDB_PARTITION, IotdbConstant.IOTDB_PARTITION);
        return new RecordPartition(partitionMap);
    }

    private List<Field> buildFields(DeviceEntity entity) {
        List<Field> fields = new ArrayList<>();
        final List<String> names = entity.getColumnNames();
        final List<String> types = entity.getColumnTypes();
        for (int i = 0; i < names.size(); i++) {
            final String fieldName = names.get(i);
            final String fieldType = types.get(i);
            fields.add(new Field(i, fieldName, SchemaProcessor.getSchema(fieldType)));
        }
        return fields;
    }

    private RecordOffset buildRecordOffset(DeviceEntity deviceEntity) {
        Map<String, Long> offsetMap = new HashMap<>();
        offsetMap.put(IotdbConstant.IOTDB_PARTITION + deviceEntity.getPath(), deviceEntity.getRowRecord().getTimestamp());
        return new RecordOffset(offsetMap);
    }

    private Struct buildPayLoad(List<Field> fields, Schema schema, DeviceEntity deviceEntity) {
        Struct payLoad = new Struct(schema);
        final RowRecord rowRecord = deviceEntity.getRowRecord();
        final List<String> columnTypes = deviceEntity.getColumnTypes();
        final List<org.apache.iotdb.tsfile.read.common.Field> measurementList = rowRecord.getFields();
        for (int i = 0; i < fields.size(); i++) {
            if (i == 0) {
                payLoad.put(fields.get(i), rowRecord.getTimestamp());
            } else {
                payLoad.put(fields.get(i), measurementList.get(i - 1).getObjectValue(SchemaProcessor.getTSDataType(columnTypes.get(i))));
            }
        }
        return payLoad;
    }

    @Override
    public void stop() {
        replicator.stop();
        log.info("iotdb source task stop success");
    }


}
