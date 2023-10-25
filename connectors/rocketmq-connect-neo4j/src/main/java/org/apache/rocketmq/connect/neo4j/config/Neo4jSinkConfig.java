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

package org.apache.rocketmq.connect.neo4j.config;

import java.util.HashSet;
import java.util.Set;

public class Neo4jSinkConfig extends Neo4jBaseConfig {
    private String labelType;

    public String getLabelType() {
        return labelType;
    }

    public void setLabelType(String labelType) {
        this.labelType = labelType;
    }

    public static final Set<String> SINK_REQUEST_CONFIG = new HashSet<String>() {
        {
            add(Neo4jConstants.NEO4J_HOST);
            add(Neo4jConstants.NEO4J_PORT);
            add(Neo4jConstants.NEO4J_USER);
            add(Neo4jConstants.NEO4J_PASSWORD);
            add(Neo4jConstants.NEO4J_DB);
            add(Neo4jConstants.LABEL_TYPE);
        }
    };
}