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

package org.apache.rocketmq.connect.elasticsearch.connector;

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
import io.openmessaging.internal.DefaultKeyValue;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.connect.elasticsearch.config.ElasticsearchConfig;
import org.apache.rocketmq.connect.elasticsearch.config.ElasticsearchConstant;
import org.apache.rocketmq.connect.elasticsearch.replicator.source.ElasticsearchReplicator;
import org.elasticsearch.search.SearchHit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ElasticsearchSourceTask extends SourceTask {

    private static final Logger log = LoggerFactory.getLogger(ElasticsearchSourceTask.class);

    private ElasticsearchReplicator replicator;

    private ElasticsearchConfig config;

    @Override
    public List<ConnectRecord> poll() {
        List<ConnectRecord> res = new ArrayList<>();
        try {
            SearchHit searchHit = replicator.getQueue().poll(1000, TimeUnit.MILLISECONDS);
            if (searchHit != null) {
                res.add(searchHit2ConnectRecord(searchHit));
            }
        } catch (Exception e) {
            log.error("elasticsearch sourceTask poll error, current config:" + JSON.toJSONString(config), e);
        }
        return res;
    }

    @Override
    public void start(KeyValue keyValue) {
        final RecordOffset recordOffset = this.sourceTaskContext.offsetStorageReader().readOffset(buildRecordPartition());
        this.config = new ElasticsearchConfig();
        this.config.load(keyValue);
        this.replicator = new ElasticsearchReplicator(config);
        this.replicator.start(recordOffset, keyValue);
        log.info("elasticsearch source task start success");
    }

    @Override
    public void stop() {
        replicator.stop();
        log.info("elasticsearch source task stop success");
    }

    public ConnectRecord searchHit2ConnectRecord(SearchHit hit) {
        Schema schema = SchemaBuilder.struct().name(hit.getIndex()).build();
        final List<Field> fields = buildFields(hit);
        schema.setFields(fields);
        final ConnectRecord connectRecord = new ConnectRecord(buildRecordPartition(),
            buildRecordOffset(hit),
            System.currentTimeMillis(),
            schema,
            this.buildPayLoad(fields, schema, hit.getSourceAsMap()));
        connectRecord.setExtensions(this.buildExtensions(hit));
        return connectRecord;
    }

    private List<Field> buildFields(SearchHit hit) {
        List<Field> fields = new ArrayList<>();
        final Map<String, Object> map = hit.getSourceAsMap();
        int i = 0;
        for (Map.Entry<String, Object> entry : map.entrySet()) {
            fields.add(new Field(i++, entry.getKey(), getSchema(entry.getValue())));
        }
        return fields;
    }

    private RecordPartition buildRecordPartition() {
        Map<String, String> partitionMap = new HashMap<>();
        partitionMap.put(ElasticsearchConstant.ES_PARTITION, ElasticsearchConstant.ES_PARTITION);
        RecordPartition  recordPartition = new RecordPartition(partitionMap);
        return recordPartition;
    }

    private RecordOffset buildRecordOffset(SearchHit hit) {
        Map<String, Long> offsetMap = new HashMap<>();
        Object value = JSON.parseObject(hit.getSourceAsString()).get(config.getIndexMap().get(hit.getIndex()));
        if (value == null) {
            value = 1;
        }
        offsetMap.put(hit.getIndex() + ":" + ElasticsearchConstant.ES_POSITION, Long.parseLong(value.toString()));
        RecordOffset recordOffset = new RecordOffset(offsetMap);
        return recordOffset;
    }

    private KeyValue buildExtensions(SearchHit hit) {
        KeyValue keyValue = new DefaultKeyValue();
        keyValue.put(ElasticsearchConstant.INDEX, hit.getIndex());
        return keyValue;
    }

    private Struct buildPayLoad(List<Field> fields, Schema schema, Map<String, Object> dataMap) {
        Struct payLoad = new Struct(schema);
        for (int i = 0; i < fields.size(); i++) {
            payLoad.put(fields.get(i), dataMap.get(fields.get(i).getName()));
        }
        return payLoad;
    }

    private Schema getSchema(Object obj) {
        if (obj instanceof Integer) {
            return SchemaBuilder.int32().build();
        } else if (obj instanceof Long) {
            return SchemaBuilder.int64().build();
        } else if (obj instanceof String) {
            return SchemaBuilder.string().build();
        } else if (obj instanceof Date) {
            return SchemaBuilder.time().build();
        } else if (obj instanceof Timestamp) {
            return SchemaBuilder.timestamp().build();
        }
        return SchemaBuilder.string().build();
    }

}
