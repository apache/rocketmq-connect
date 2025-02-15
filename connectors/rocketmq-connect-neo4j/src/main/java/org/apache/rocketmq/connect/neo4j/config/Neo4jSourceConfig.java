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

import org.apache.rocketmq.connect.neo4j.helper.LabelTypeEnum;

public class Neo4jSourceConfig extends Neo4jBaseConfig {
    private String labelType;
    private String label;

    public static final Set<String> REQUEST_CONFIG = new HashSet<String>() {
        {
            add(Neo4jConstants.NEO4J_HOST);
            add(Neo4jConstants.NEO4J_PORT);
            add(Neo4jConstants.NEO4J_USER);
            add(Neo4jConstants.NEO4J_PASSWORD);
            add(Neo4jConstants.NEO4J_TOPIC);
            add(Neo4jConstants.NEO4J_DB);
            add(Neo4jConstants.LABEL_TYPE);
        }
    };

    public String getTaskName() {
        if (LabelTypeEnum.node.name().equals(labelType) || LabelTypeEnum.relationship.name().equals(labelType)) {
            return getNeo4jDataBase() + "_" + labelType + "_" + label;
        }
        return getNeo4jDataBase();
    }

    public String getLabelType() {
        return labelType;
    }

    public void setLabelType(String labelType) {
        this.labelType = labelType;
    }

    public String getLabel() {
        return label;
    }

    public void setLabel(String label) {
        this.label = label;
    }

}