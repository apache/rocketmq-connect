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

import org.apache.rocketmq.connect.neo4j.config.Neo4jSourceConfig;

public class QueryRegistrar {
    private static final Map<String, CqlQueryProcessor> registrar = new HashMap<>();

    public static void register(Neo4jSourceConfig neo4jSourceConfig) {
        doRegister(new NodeQueryStrategy(neo4jSourceConfig));
        doRegister(new RelationshipQueryStrategy(neo4jSourceConfig));
    }

    static void doRegister(CqlQueryProcessor cqlQueryProcessor) {
        registrar.put(cqlQueryProcessor.queryType(), cqlQueryProcessor);
    }

    public static CqlQueryProcessor querySqlBuilder(String queryType) {
        return registrar.get(queryType);
    }
}