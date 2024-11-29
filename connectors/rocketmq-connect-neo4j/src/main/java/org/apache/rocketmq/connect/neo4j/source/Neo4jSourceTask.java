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

package org.apache.rocketmq.connect.neo4j.source;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.rocketmq.connect.neo4j.config.Neo4jConstants;
import org.apache.rocketmq.connect.neo4j.config.Neo4jSourceConfig;
import org.apache.rocketmq.connect.neo4j.helper.Neo4jClient;
import org.apache.rocketmq.connect.neo4j.source.query.CqlQueryProcessor;
import org.apache.rocketmq.connect.neo4j.source.query.QueryRegistrar;

import org.neo4j.driver.Record;
import org.neo4j.driver.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.openmessaging.KeyValue;
import io.openmessaging.connector.api.component.task.source.SourceTask;
import io.openmessaging.connector.api.data.ConnectRecord;
import io.openmessaging.connector.api.data.RecordOffset;
import io.openmessaging.connector.api.data.RecordPartition;
import io.openmessaging.connector.api.data.Struct;
import io.openmessaging.internal.DefaultKeyValue;

public class Neo4jSourceTask extends SourceTask {
    private static final Logger log = LoggerFactory.getLogger(Neo4jSourceTask.class);

    private Neo4jSourceConfig config;

    private Neo4jClient client;

    @Override
    public List<ConnectRecord> poll() {
        List<ConnectRecord> res = new ArrayList<>();
        long offset = readRecordOffset();
        String cql = buildCql(offset);
        try {
            final List<Record> recordList = client.query(cql);
            for (Record record : recordList) {
                final ConnectRecord connectRecord = neo4jRecord2ConnectRecord(record);
                res.add(connectRecord);
            }
        } catch (Exception e) {
            log.error(String.format("Fail to poll data from neo4j! offset=%d", offset));
        }
        return res;
    }

    @Override
    public void start(KeyValue keyValue) {
        this.config = new Neo4jSourceConfig();
        this.config.load(keyValue);
        this.client = new Neo4jClient(this.config);
        if (!client.ping()) {
            throw new RuntimeException("Cannot connect to neo4j server!");
        }
        QueryRegistrar.register(config);
    }

    @Override
    public void stop() {
        this.client = null;
    }

    // build cypher query
    private String buildCql(long offset) {
        final String queryType = config.getLabelType();
        final CqlQueryProcessor cqlQueryProcessor = QueryRegistrar.querySqlBuilder(queryType);
        return cqlQueryProcessor.buildCql(offset);
    }

    private long readRecordOffset() {
        if (sourceTaskContext == null) {
            return 0L;
        }
        final RecordOffset positionInfo =
            this.sourceTaskContext.offsetStorageReader().readOffset(buildRecordPartition(config.getNeo4jDataBase()));
        if (positionInfo == null) {
            return 0;
        }
        Object offset = positionInfo.getOffset().get(config.getTaskName() + "_" + Neo4jConstants.NEO4J_OFFSET);
        return offset == null ? 0 : Long.parseLong(offset.toString());
    }

    private ConnectRecord neo4jRecord2ConnectRecord(Record record) {
        final String queryType = config.getLabelType();
        final CqlQueryProcessor cqlQueryProcessor = QueryRegistrar.querySqlBuilder(queryType);
        final Pair<Long, Struct> structPair = cqlQueryProcessor.buildStruct(record);
        final Long id = structPair.key();
        final Struct struct = structPair.value();
        final ConnectRecord connectRecord =
            new ConnectRecord(buildRecordPartition(config.getTaskName()), buildRecordOffset(id),
                System.currentTimeMillis(), struct.getSchema(), struct);

        connectRecord.setExtensions(this.buildExtensions());
        return connectRecord;
    }

    private RecordPartition buildRecordPartition(String partitionValue) {
        Map<String, String> partitionMap = new HashMap<>();
        partitionMap.put(Neo4jConstants.NEO4J_PARTITION, partitionValue);
        return new RecordPartition(partitionMap);
    }

    private KeyValue buildExtensions() {
        KeyValue keyValue = new DefaultKeyValue();
        String topicName = config.getTopic();
        if (topicName == null || topicName.equals("")) {
            String connectorName = this.sourceTaskContext.getConnectorName();
            topicName = config.getTaskName() + "_" + connectorName;
        }
        keyValue.put(Neo4jConstants.NEO4J_TOPIC, topicName);
        return keyValue;
    }

    private RecordOffset buildRecordOffset(long offset) {
        Map<String, Long> offsetMap = new HashMap<>();
        offsetMap.put(config.getTaskName() + "_" + Neo4jConstants.NEO4J_OFFSET, offset);
        return new RecordOffset(offsetMap);
    }
}