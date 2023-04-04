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
import io.openmessaging.connector.api.data.ConnectRecord;
import io.openmessaging.connector.api.data.Field;
import io.openmessaging.connector.api.data.FieldType;
import io.openmessaging.connector.api.data.RecordOffset;
import io.openmessaging.connector.api.data.RecordPartition;
import io.openmessaging.connector.api.data.Schema;
import io.openmessaging.connector.api.data.SchemaBuilder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.rocketmq.connect.redis.common.Options;
import org.apache.rocketmq.connect.redis.pojo.KVEntry;

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
            Schema keySchema = SchemaBuilder.string().name(Options.REDIS_KEY.name()).build();
            keySchema.setFields(buildFields());
            final Object data = values.get(i);
            if (data == null  || data.toString().equals("")) {
                continue;
            }
            res.add(new ConnectRecord(
                buildRecordPartition(),
                buildRecordOffset(kvEntry.getOffset()),
                System.currentTimeMillis(),
                keySchema,
                kvEntry.getKey(),
                buildValueSchema(),
                JSON.toJSONString(kvEntry.getValue())));
        }
        return res;
    }

    private RecordOffset buildRecordOffset(Long offset)  {
        Map<String, Long> offsetMap = new HashMap<>();
        offsetMap.put(Options.REDIS_OFFSET.name(), offset);
        RecordOffset recordOffset = new RecordOffset(offsetMap);
        return recordOffset;
    }

    private RecordPartition buildRecordPartition() {
        Map<String, String> partitionMap = new HashMap<>();
        partitionMap.put(Options.REDIS_PARTITION.name(), Options.REDIS_PARTITION.name());
        RecordPartition  recordPartition = new RecordPartition(partitionMap);
        return recordPartition;
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

    private Schema buildValueSchema() {
        final Schema valueSchema = SchemaBuilder.string().build();
        return valueSchema;
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
