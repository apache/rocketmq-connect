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
package org.apache.rocketmq.connect.neo4j.source.mapping;

import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Function;

import org.apache.rocketmq.connect.neo4j.helper.LabelTypeEnum;
import org.apache.rocketmq.connect.neo4j.helper.MappingRule;
import org.apache.rocketmq.connect.neo4j.helper.ValueType;

public class ReadMapper {

    public static BiConsumer<Map<String, Object>, Map<String, Object>> getMapper(MappingRule rule) {
        return (neo4jElement, record) -> rule.getColumns().forEach(columnMappingRule -> {
            Object value = null;
            ValueType type = columnMappingRule.getValueType();
            String name = columnMappingRule.getName();
            final String valueExtract = columnMappingRule.getValueExtract();

            switch (columnMappingRule.getColumnType()) {
                case dstPrimaryKey:
                    final Map<String, Object> targetMap = (Map<String, Object>)neo4jElement.get("<target_map>");
                    value = extractValue(valueExtract, "<node_id>", targetMap::get, type);
                    record.put(name, value);
                    break;
                case srcPrimaryKey:
                    final Map<String, Object> sourceMap = (Map<String, Object>)neo4jElement.get("<source_map>");
                    value = extractValue(valueExtract, "<node_id>", sourceMap::get, type);
                    record.put(name, value);
                    break;
                case primaryKey:
                    final LabelTypeEnum labelTypeEnum = rule.getType();
                    if (LabelTypeEnum.node == labelTypeEnum) {
                        value = extractValue(valueExtract, "<node_id>", neo4jElement::get, type);
                    } else {
                        value = extractValue(valueExtract, "<relationship_id>", neo4jElement::get, type);
                    }

                    record.put(name, value);
                    break;
                case primaryLabel:
                    final LabelTypeEnum exportType = rule.getType();
                    if (LabelTypeEnum.node == exportType) {
                        value = neo4jElement.get("<labels>");
                    } else {
                        value = neo4jElement.get("<type>");
                    }
                    record.put(name, value);
                    break;
                case dstPrimaryLabel:
                    final Map<String, Object> dstTargetMap = (Map<String, Object>)neo4jElement.get("<target_map>");
                    value = extractValue(valueExtract, "<labels>", dstTargetMap::get, type);
                    record.put(name, value);
                    break;
                case srcPrimaryLabel:
                    final Map<String, Object> srcSourceMap = (Map<String, Object>)neo4jElement.get("<source_map>");
                    value = extractValue(valueExtract, "<labels>", srcSourceMap::get, type);
                    record.put(name, value);
                    break;
                case nodeProperty:
                case relationshipProperty:
                    value = extractValue(valueExtract, name, neo4jElement::get, type);
                    record.put(name, value);
                    break;
                case relationshipJsonProperty:
                case nodeJsonProperty:
                    value = extractValue(valueExtract, name, neo4jElement::get, type);
                    record.put(name, type.applyObject(value));
                    break;
                default:
                    break;
            }
        });
    }

    private static Object extractValue(String valueExtract, String defaultExtract, Function<String, Object> function,
        ValueType valueType) {
        if (valueExtract == null) {
            return function.apply(defaultExtract);
        } else {
            if (valueExtract.startsWith("#{") && valueExtract.endsWith("}")) {
                final String val = valueExtract.substring(2, valueExtract.length() - 1);
                return function.apply(val);
            } else {
                return valueType.applyObject(valueExtract);
            }
        }
    }

}
