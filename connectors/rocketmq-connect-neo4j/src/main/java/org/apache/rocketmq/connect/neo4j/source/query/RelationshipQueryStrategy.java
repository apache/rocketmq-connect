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

import java.util.HashMap;
import java.util.Map;

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
import org.neo4j.driver.types.Relationship;
import org.neo4j.driver.util.Pair;

import io.openmessaging.connector.api.data.Schema;
import io.openmessaging.connector.api.data.Struct;

public class RelationshipQueryStrategy implements CqlQueryProcessor {
    private Neo4jSourceConfig neo4jSourceConfig;
    private MappingRule mappingRule;

    public RelationshipQueryStrategy(Neo4jSourceConfig config) {
        this.neo4jSourceConfig = config;
        final LabelTypeEnum labelTypeEnum = LabelTypeEnum.nameOf(neo4jSourceConfig.getLabelType());
        this.mappingRule = MappingRuleFactory.getInstance().create(neo4jSourceConfig, labelTypeEnum);
        mappingRule.validateConfig();
    }

    @Override
    public String buildCql(long offset) {
        StringBuilder cql = new StringBuilder();
        cql.append("use ").append(neo4jSourceConfig.getNeo4jDataBase()).append(" ");
        cql.append("match  (source)-[r:").append(neo4jSourceConfig.getLabel()).append("]->(target)");
        cql.append(" where id(r) >=").append(offset);
        cql.append(" return source , r, target ").append(" limit ")
            .append(Neo4jConstants.MAX_NUMBER_SEND_CONNECT_RECORD_EACH_TIME);
        return cql.toString();
    }

    @Override
    public Pair<Long, Struct> buildStruct(Record record) {
        final Map<String, Object> recordMap = record.asMap();
        final Object value = recordMap.get("r");
        if (!(value instanceof Relationship)) {
            throw new RuntimeException("cypher query Inconsistent");
        }
        final Node source = (Node)recordMap.get("source");
        final Map<String, Object> sourceMap = RecordConverter.asNodeMap(source);
        final Node target = (Node)recordMap.get("target");
        final Map<String, Object> targetMap = RecordConverter.asNodeMap(target);
        Relationship relationship = (Relationship)value;
        final Map<String, Object> relationshipMap = RecordConverter.asRelationshipMap(relationship);
        relationshipMap.put("<source_map>", sourceMap);
        relationshipMap.put("<target_map>", targetMap);
        Map<String, Object> columnMap = new HashMap<>();
        ReadMapper.getMapper(mappingRule).accept(relationshipMap, columnMap);
        final Schema valueSchema = RecordConverter.map2StructSchema(columnMap);
        valueSchema.setName(relationship.type());
        Struct struct = RecordConverter.buildStruct(valueSchema, columnMap);
        return InternalPair.of(relationship.id(), struct);
    }

    @Override
    public String queryType() {
        return LabelTypeEnum.relationship.name();
    }
}