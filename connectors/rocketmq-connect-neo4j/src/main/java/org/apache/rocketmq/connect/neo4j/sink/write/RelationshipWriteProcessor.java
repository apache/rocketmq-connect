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

import java.util.Map;

import org.apache.rocketmq.connect.neo4j.config.Neo4jSinkConfig;
import org.apache.rocketmq.connect.neo4j.helper.Neo4jClient;
import org.apache.rocketmq.connect.neo4j.helper.Neo4jElement;

import io.openmessaging.connector.api.data.Field;
import io.openmessaging.connector.api.data.FieldType;
import io.openmessaging.connector.api.data.Schema;

public class RelationshipWriteProcessor extends WriteProcessor {
    private Neo4jSinkConfig neo4jSinkConfig;

    public RelationshipWriteProcessor(Neo4jSinkConfig config, Neo4jClient neo4jClient) {
        super(config, neo4jClient);
        this.neo4jSinkConfig = config;
    }

    protected String buildCql(Neo4jElement neo4jElement, Schema schema) {
        StringBuilder cql = new StringBuilder();
        cql.append("use ").append(neo4jSinkConfig.getNeo4jDataBase()).append(" ");
        cql.append("merge (source:").append(neo4jElement.getFromLabel()).append("{")
            .append(neo4jElement.getFromPrimaryKey()).append(":").append(neo4jElement.getFromPrimaryValue())
            .append("}) ");
        cql.append("merge (target:").append(neo4jElement.getToLabel()).append("{")
            .append(neo4jElement.getToPrimaryKey()).append(":").append(neo4jElement.getToPrimaryValue()).append("}) ");
        cql.append("merge (source)-[:").append(neo4jElement.getLabel()).append("{")
            .append(addProperty(schema, neo4jElement)).append("}]").append("->(target)");
        return cql.toString();
    }

    protected String addProperty(Schema schema, Neo4jElement neo4jElement) {
        StringBuilder cql = new StringBuilder();
        cql.append(setValue(neo4jElement.getPrimaryKey(), neo4jElement.getPrimaryValue(), schema)).append(",");
        final Map<String, Object> properties = neo4jElement.getProperties();
        if (!properties.isEmpty()) {
            for (String key : properties.keySet()) {
                final Object o = properties.get(key);
                cql.append(setValue(key, o, schema));
                cql.append(",");
            }
        }
        return cql.substring(0, cql.length() - 1);
    }

    protected String setValue(String key, Object value, Schema schema) {
        final Field field = schema.getField(key);
        if (field == null) {
            return key + ":" + value;
        }
        if (field.getSchema().getFieldType() == FieldType.STRING) {
            return key + ":'" + value + "'";
        } else {
            return key + ":" + value;
        }
    }
}