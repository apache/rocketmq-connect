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

package org.apache.rocketmq.connect.redis.converter;

import com.alibaba.fastjson.JSON;
import io.openmessaging.connector.api.data.*;
import org.apache.rocketmq.connect.redis.common.Options;
import org.apache.rocketmq.connect.redis.pojo.KVEntry;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class RedisEntryConverter implements KVEntryConverter {
    private final int maxValueSize = 500;

    @Override
    public List<ConnectRecord> kVEntryToConnectRecord(KVEntry kvEntry) {
        String partition = kvEntry.getPartition();
        if (partition == null) {
            throw new IllegalStateException("partition info error.");
        }

        List<ConnectRecord> res = new ArrayList<>();
        List<Object> values = splitValue(kvEntry.getValueType(), kvEntry.getValue(), this.maxValueSize);
        for (int i = 0; i < values.size(); i++) {
            final Object data = values.get(i);
            if (data == null || data.toString().equals("")) {
                continue;
            }
            Schema schema = SchemaBuilder.struct().name(Options.REDIS_KEY.name()).build();
            schema.setFields(buildFields());
            res.add(new ConnectRecord(buildRecordPartition(), buildRecordOffset(kvEntry.getOffset()), System.currentTimeMillis(), schema, this.buildPayLoad(schema.getFields(), schema, kvEntry, data)));
        }
        return res;
    }
    
    @Override
    public KVEntry connectRecordsToKVEntry(final List<ConnectRecord> connectRecords) {
        return null;
    }
    
    private RecordOffset buildRecordOffset(Long offset)  {
        Map<String, Long> offsetMap = new HashMap<>();
        offsetMap.put(Options.REDIS_OFFSET.name(), offset);
        return new RecordOffset(offsetMap);
    }

    private RecordPartition buildRecordPartition() {
        Map<String, String> partitionMap = new HashMap<>();
        partitionMap.put(Options.REDIS_PARTITION.name(), Options.REDIS_PARTITION.name());
        return new RecordPartition(partitionMap);
    }

    private List<Field> buildFields() {
        final Schema stringSchema = SchemaBuilder.string().build();
        List<Field> fields = new ArrayList<>();
        fields.add(new Field(0, Options.REDIS_COMMAND.name(), stringSchema));
        fields.add(new Field(1, Options.REDIS_KEY.name(), stringSchema));
        fields.add(new Field(2, Options.REDIS_VALUE.name(), stringSchema));
        fields.add(new Field(3, Options.REDIS_PARAMS.name(), stringSchema));
        return fields;
    }
    
    private Struct buildPayLoad(List<Field> fields, Schema schema, KVEntry kvEntry, Object data) {
        Struct payLoad = new Struct(schema);
        payLoad.put(fields.get(0), kvEntry.getCommand());
        payLoad.put(fields.get(1), kvEntry.getKey());
        payLoad.put(fields.get(2), data instanceof String ? data : JSON.toJSONString(data));
        payLoad.put(fields.get(3), JSON.toJSONString(kvEntry.getParams()));
        return payLoad;
    }

    private List<Object> splitValue(FieldType valueType, Object value, Integer maxValueSize) {
        List<Object> res = new ArrayList<>();
        if (valueType.equals(FieldType.ARRAY) && value instanceof List) {
            List<Object> list = (List) value;
            if (list.size() < maxValueSize) {
                res.add(list);
            } else {
                int num = list.size() / maxValueSize + 1;
                for (int i = 0; i < num; i++) {
                    List<Object> v = new ArrayList<>();
                    for (int j = i * maxValueSize; j < Math.min((i + 1) * maxValueSize, list.size()); j++) {
                        v.add(list.get(j));
                    }
                    if (!v.isEmpty()) {
                        res.add(v);
                    }
                }
            }
            return res;
        }

        if (valueType.equals(FieldType.MAP) && value instanceof Map) {
            Map<Object, Object> map = (Map<Object, Object>) value;
            if (map.size() < maxValueSize) {
                res.add(map);
            } else {
                AtomicInteger num = new AtomicInteger(0);
                Map<Object, Object> v = new HashMap<>();
                for (Object k : map.keySet()) {
                    v.put(k, map.get(k));
                    if (num.incrementAndGet() == maxValueSize) {
                        res.add(v);
                        v = new HashMap<>();
                        num = new AtomicInteger(0);
                    }
                }
                if (!v.isEmpty()) {
                    res.add(v);
                }
            }
            return res;
        }

        res.add(value);
        return res;
    }

}
