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

package org.apache.rocketmq.connect.neo4j.sink.write;

import org.apache.rocketmq.connect.neo4j.config.Neo4jSinkConfig;
import org.apache.rocketmq.connect.neo4j.helper.LabelTypeEnum;
import org.apache.rocketmq.connect.neo4j.helper.Neo4jClient;
import org.apache.rocketmq.connect.neo4j.helper.Neo4jElement;
import org.apache.rocketmq.connect.neo4j.sink.mapping.WriteMapper;
import org.apache.rocketmq.connect.neo4j.helper.MappingRule;
import org.apache.rocketmq.connect.neo4j.helper.MappingRuleFactory;

import io.openmessaging.connector.api.data.ConnectRecord;
import io.openmessaging.connector.api.data.Schema;

public abstract class WriteProcessor implements Processor {
    private Neo4jSinkConfig neo4jSinkConfig;
    private Neo4jClient neo4jClient;
    private MappingRule mappingRule;

    public WriteProcessor(Neo4jSinkConfig config, Neo4jClient neo4jClient) {
        this.neo4jSinkConfig = config;
        this.neo4jClient = neo4jClient;
        final LabelTypeEnum labelTypeEnum = LabelTypeEnum.nameOf(neo4jSinkConfig.getLabelType());
        if (labelTypeEnum == null) {
            throw new RuntimeException("label type only support node or relationship");
        }
        mappingRule = MappingRuleFactory.getInstance().create(neo4jSinkConfig, labelTypeEnum);
        mappingRule.validateConfig();
    }

    public void write(ConnectRecord record) {
        final Schema recordSchema = record.getSchema();
        Neo4jElement neo4jElement = new Neo4jElement();
        WriteMapper.getMapper(mappingRule).accept(record, neo4jElement);
        String cql = buildCql(neo4jElement, recordSchema);
        neo4jClient.insert(cql);
    }

    protected abstract String buildCql(Neo4jElement neo4jElement, Schema schema);

}