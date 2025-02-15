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

import io.openmessaging.connector.api.data.*;

public class NodeWriteProcessor extends WriteProcessor {
    private Neo4jSinkConfig neo4jSinkConfig;

    public NodeWriteProcessor(Neo4jSinkConfig config, Neo4jClient neo4jClient) {
        super(config, neo4jClient);
        this.neo4jSinkConfig = config;
    }

    //MERGE (keanu:Person {name: 'Keanu Reeves'})
    //ON CREATE
    //  SET keanu.created = timestamp()
    //ON MATCH
    //  SET keanu.lastSeen = timestamp()
    //RETURN keanu.name, keanu.created, keanu.lastSeen
    protected String buildCql(Neo4jElement neo4jElement, Schema schema) {
        StringBuilder cql = new StringBuilder();
        cql.append("use ").append(neo4jSinkConfig.getNeo4jDataBase()).append(" ");
        cql.append("merge (n:").append(neo4jElement.getLabel()).append("{").append(neo4jElement.getPrimaryKey())
            .append(":").append(neo4jElement.getPrimaryValue()).append("})");
        if (!neo4jElement.getProperties().isEmpty()) {
            cql.append(" on create set ").append(setProperty(neo4jElement.getProperties(), schema));
            cql.append(" on match set ").append(setProperty(neo4jElement.getProperties(), schema));
        }
        return cql.toString();
    }

    protected String setProperty(Map<String, Object> property, Schema recordSchema) {
        StringBuilder cql = new StringBuilder();
        for (String key : property.keySet()) {
            final Object o = property.get(key);
            final Field field = recordSchema.getField(key);
            if (field.getSchema().getFieldType() == FieldType.STRING) {
                cql.append("n.").append(key).append(" = ").append(setStringValue((String)o));
            } else {
                cql.append("n.").append(key).append(" = ").append(o);
            }
            cql.append(",");
        }
        return cql.substring(0, cql.length() - 1);
    }

    protected String setStringValue(String v) {
        return "'" + v + "'";
    }
}