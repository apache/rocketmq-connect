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

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

import org.apache.rocketmq.connect.neo4j.config.Neo4jBaseConfig;
import org.apache.rocketmq.connect.neo4j.config.Neo4jConstants;
import org.apache.rocketmq.connect.neo4j.config.Neo4jConstants.*;

public class MappingRuleFactory {
    private static final MappingRuleFactory instance = new MappingRuleFactory();

    public static MappingRuleFactory getInstance() {
        return instance;
    }

    public MappingRule create(Neo4jBaseConfig config, LabelTypeEnum exportType) {
        MappingRule rule = new MappingRule();

        rule.setType(exportType);
        String columnStr = config.getColumn();
        final JSONArray configObject = JSON.parseArray(columnStr);
        final int size = configObject.size();
        for (int i = 0; i < size; i++) {
            final JSONObject columnValue = configObject.getJSONObject(i);
            final String columnName = columnValue.getString(Neo4jConstants.COLUMN_NAME);
            String type = columnValue.getString(Neo4jConstants.VALUE_TYPE);
            String cType = columnValue.getString(Neo4jConstants.COLUMN_TYPE);
            String valueExtract = columnValue.getString(Neo4jConstants.VALUE_EXTRACT);

            ColumnType columnType;
            try {
                columnType = ColumnType.valueOf(cType);
            } catch (NullPointerException | IllegalArgumentException e) {
                throw new RuntimeException("columnType config error");
            }

            if (exportType == LabelTypeEnum.node) {
                // only id/label/property column allow when node
                if (columnType != ColumnType.primaryKey && columnType != ColumnType.primaryLabel
                    && columnType != ColumnType.nodeProperty && columnType != ColumnType.nodeJsonProperty) {
                    throw new RuntimeException("only id/label/property column allow when node");
                }
            } else if (exportType == LabelTypeEnum.relationship) {
                // relationship
                if (columnType != ColumnType.primaryKey && columnType != ColumnType.primaryLabel
                    && columnType != ColumnType.srcPrimaryKey && columnType != ColumnType.srcPrimaryLabel
                    && columnType != ColumnType.dstPrimaryKey && columnType != ColumnType.dstPrimaryLabel
                    && columnType != ColumnType.relationshipProperty
                    && columnType != ColumnType.relationshipJsonProperty) {
                    throw new RuntimeException("relationship check");
                }
            }
            if (columnType == ColumnType.relationshipProperty || columnType == ColumnType.nodeProperty
                || columnType == ColumnType.primaryKey || columnType == ColumnType.dstPrimaryKey
                || columnType == ColumnType.srcPrimaryKey) {

                ValueType propType = ValueType.fromShortName(type);

                if (propType == null) {
                    throw new RuntimeException("UNSUPPORTED TYPE");
                }
                rule.addColumn(columnType, propType, columnName, valueExtract);
            } else if (columnType == ColumnType.nodeJsonProperty || columnType == ColumnType.relationshipJsonProperty) {
                rule.addColumn(columnType, ValueType.STRING, columnName, valueExtract);
            } else {
                rule.addColumn(columnType, ValueType.STRING, columnName, valueExtract);
            }
        }
        return rule;
    }

    public static boolean isPrimitive(Object value) {
        return value == null || value instanceof Boolean || value instanceof Number || value instanceof String;
    }
}