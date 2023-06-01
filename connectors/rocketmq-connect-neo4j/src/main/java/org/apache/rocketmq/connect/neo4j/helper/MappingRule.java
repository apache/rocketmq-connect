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

package org.apache.rocketmq.connect.neo4j.helper;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.rocketmq.connect.neo4j.config.Neo4jConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.rocketmq.connect.neo4j.config.Neo4jConstants.*;

public class MappingRule {
    private static final Logger LOGGER = LoggerFactory.getLogger(MappingRule.class);
    private boolean hasRelation = false;
    private boolean hasProperty = false;
    private LabelTypeEnum type = LabelTypeEnum.node;

    /**
     * property names for property key-value
     */
    private List<String> propertyNames = new ArrayList<>();

    private List<ColumnMappingRule> columns = new ArrayList<>();

    private Set<ColumnType> columnTypeSet = new HashSet<>();

    public void validateConfig() {
        if (type == LabelTypeEnum.node) {
            if (!columnTypeSet.contains(ColumnType.primaryKey) || !columnTypeSet.contains(ColumnType.primaryLabel)) {
                LOGGER.error("node config need ColumnType primaryKey and primaryLabel");
                throw new RuntimeException("node config need ColumnType primaryKey and primaryLabel");
            }
        } else {
            if (!columnTypeSet.contains(ColumnType.primaryKey) || !columnTypeSet.contains(ColumnType.primaryLabel)
                || !columnTypeSet.contains(ColumnType.srcPrimaryKey) || !columnTypeSet
                .contains(ColumnType.srcPrimaryLabel) || !columnTypeSet.contains(ColumnType.dstPrimaryKey)
                || !columnTypeSet.contains(ColumnType.dstPrimaryLabel)) {
                LOGGER.error("relationship config need ColumnType primaryKey and primaryLabel and srcPrimaryKey and "
                    + "srcPrimaryLabel and dstPrimaryKey and dstPrimaryLabel");
                throw new RuntimeException(
                    "relationship config need ColumnType primaryKey and primaryLabel and srcPrimaryKey and "
                        + "srcPrimaryLabel and dstPrimaryKey and dstPrimaryLabel");
            }
        }
    }

    void addColumn(ColumnType columnType, ValueType type, String name, String valueExtract) {
        ColumnMappingRule rule = new ColumnMappingRule();
        rule.setColumnType(columnType);
        rule.setName(name);
        rule.setValueType(type);
        rule.setValueExtract(valueExtract);
        columnTypeSet.add(columnType);

        if (columnType == ColumnType.nodeProperty || columnType == Neo4jConstants.ColumnType.relationshipProperty) {
            propertyNames.add(name);
            hasProperty = true;
        }

        boolean hasTo = columnType == ColumnType.dstPrimaryKey || columnType == ColumnType.dstPrimaryLabel;
        boolean hasFrom = columnType == ColumnType.srcPrimaryKey || columnType == ColumnType.srcPrimaryLabel;
        if (hasTo || hasFrom) {
            hasRelation = true;
        }

        columns.add(rule);
    }

    void addJsonColumn(ColumnType columnType) {
        ColumnMappingRule rule = new ColumnMappingRule();
        rule.setColumnType(columnType);
        rule.setName("json");
        rule.setValueType(ValueType.STRING);

        if (!propertyNames.isEmpty()) {
            throw new RuntimeException("JsonProperties should be only property");
        }

        columns.add(rule);
        hasProperty = true;
    }

    public boolean isHasRelation() {
        return hasRelation;
    }

    public void setHasRelation(boolean hasRelation) {
        this.hasRelation = hasRelation;
    }

    public boolean isHasProperty() {
        return hasProperty;
    }

    public void setHasProperty(boolean hasProperty) {
        this.hasProperty = hasProperty;
    }

    public LabelTypeEnum getType() {
        return type;
    }

    public void setType(LabelTypeEnum type) {
        this.type = type;
    }

    public List<String> getPropertyNames() {
        return propertyNames;
    }

    public void setPropertyNames(List<String> propertyNames) {
        this.propertyNames = propertyNames;
    }

    public List<ColumnMappingRule> getColumns() {
        return columns;
    }

    public void setColumns(List<ColumnMappingRule> columns) {
        this.columns = columns;
    }

    public static class ColumnMappingRule {
        private String name = null;

        private ValueType valueType = null;

        private ColumnType columnType = null;

        private String valueExtract = null;

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public ValueType getValueType() {
            return valueType;
        }

        public void setValueType(ValueType valueType) {
            this.valueType = valueType;
        }

        public ColumnType getColumnType() {
            return columnType;
        }

        public void setColumnType(ColumnType columnType) {
            this.columnType = columnType;
        }

        public String getValueExtract() {
            return valueExtract;
        }

        public void setValueExtract(String valueExtract) {
            this.valueExtract = valueExtract;
        }
    }
}
