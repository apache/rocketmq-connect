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

package org.apache.rocketmq.connect.neo4j.source.query;

import java.util.*;

import org.apache.rocketmq.connect.neo4j.config.Neo4jConstants;
import org.apache.rocketmq.connect.neo4j.config.Neo4jSourceConfig;
import org.apache.rocketmq.connect.neo4j.helper.LabelTypeEnum;
import org.apache.rocketmq.connect.neo4j.helper.RecordConverter;
import org.apache.rocketmq.connect.neo4j.source.mapping.ReadMapper;
import org.apache.rocketmq.connect.neo4j.helper.MappingRule;
import org.apache.rocketmq.connect.neo4j.helper.MappingRuleFactory;
import org.neo4j.driver.Record;
import org.neo4j.driver.internal.InternalPair;
import org.neo4j.driver.types.Node;
import org.neo4j.driver.util.Pair;

import io.openmessaging.connector.api.data.Schema;
import io.openmessaging.connector.api.data.Struct;

// match (n:Person) where id(n) > 10000 return n order by id(n) limit 1000;
// match (n:Person) return n order by id(n) skip 10000 limit 1000;
// Performance optimization of the above two query statements with the same function

public class NodeQueryStrategy implements CqlQueryProcessor {
    private final Neo4jSourceConfig neo4jSourceConfig;
    private final MappingRule mappingRule;

    public NodeQueryStrategy(Neo4jSourceConfig neo4jSourceConfig) {
        this.neo4jSourceConfig = neo4jSourceConfig;
        final LabelTypeEnum labelTypeEnum = LabelTypeEnum.nameOf(neo4jSourceConfig.getLabelType());
        this.mappingRule = MappingRuleFactory.getInstance().create(neo4jSourceConfig, labelTypeEnum);
        mappingRule.validateConfig();
    }

    @Override
    public String buildCql(long offset) {
        StringBuilder cql = new StringBuilder();
        cql.append("use ").append(neo4jSourceConfig.getNeo4jDataBase()).append(" ");
        cql.append("match (n:").append(neo4jSourceConfig.getLabel()).append(")");
        cql.append("where id(n) >=").append(offset);
        cql.append(" return n ").append(" limit ").append(Neo4jConstants.MAX_NUMBER_SEND_CONNECT_RECORD_EACH_TIME);
        return cql.toString();
    }

    public Pair<Long, Struct> buildStruct(Record record) {
        final Map<String, Object> nMap = record.asMap();
        final Object value = nMap.get("n");
        if (!(value instanceof Node)) {
            throw new RuntimeException("cypher query Inconsistent");
        }
        Node node = (Node)value;
        final Iterable<String> labels = node.labels();
        final Map<String, Object> nodeMap = RecordConverter.asNodeMap(node);
        Map<String, Object> columnMap = new LinkedHashMap<>();
        ReadMapper.getMapper(mappingRule).accept(nodeMap, columnMap);
        final Schema valueSchema = RecordConverter.map2StructSchema(columnMap);
        StringBuilder labelName = new StringBuilder();
        final Iterator<String> iterator = labels.iterator();
        while (iterator.hasNext()) {
            labelName.append("_").append(iterator.next());
        }
        valueSchema.setName(labelName.substring(1));
        Struct struct = RecordConverter.buildStruct(valueSchema, columnMap);
        return InternalPair.of(node.id(), struct);
    }

    @Override
    public String queryType() {
        return LabelTypeEnum.node.name();
    }
}