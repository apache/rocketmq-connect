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

package org.apache.rocketmq.connect.neo4j.sink.mapping;

import java.util.function.BiConsumer;

import org.apache.rocketmq.connect.neo4j.helper.Neo4jElement;
import org.apache.rocketmq.connect.neo4j.helper.MappingRule;
import org.apache.rocketmq.connect.neo4j.helper.ValueType;

import io.openmessaging.connector.api.data.ConnectRecord;
import io.openmessaging.connector.api.data.Field;
import io.openmessaging.connector.api.data.Schema;
import io.openmessaging.connector.api.data.Struct;

public class WriteMapper {
    public static BiConsumer<ConnectRecord, Neo4jElement> getMapper(MappingRule rule) {
        return (record, neo4jElement) -> {
            final Schema recordSchema = record.getSchema();
            final Struct struct = (Struct)record.getData();

            rule.getColumns().forEach(columnMappingRule -> {
                Object value = null;
                ValueType type = columnMappingRule.getValueType();
                String name = columnMappingRule.getName();
                boolean extractFixedValue = false;
                final String valueExtract = columnMappingRule.getValueExtract();
                if (valueExtract.startsWith("#{") && valueExtract.endsWith("}")) {
                    final String val = valueExtract.substring(2, valueExtract.length() - 1);
                    final Field field = recordSchema.getField(val);
                    if (field != null) {
                        value = struct.get(field);
                    }
                } else {
                    value = valueExtract;
                    extractFixedValue = true;
                }
                switch (columnMappingRule.getColumnType()) {
                    case primaryKey:
                        neo4jElement.setPrimaryKey(name);
                        neo4jElement.setPrimaryValue(String.valueOf(value));
                        break;
                    case primaryLabel:
                        neo4jElement.setLabel(String.valueOf(value));
                        break;
                    case dstPrimaryKey:
                        neo4jElement.setToPrimaryKey(name);
                        neo4jElement.setToPrimaryValue(String.valueOf(value));
                        break;
                    case srcPrimaryKey:
                        neo4jElement.setFromPrimaryKey(name);
                        neo4jElement.setFromPrimaryValue(String.valueOf(value));
                        break;
                    case srcPrimaryLabel:
                        neo4jElement.setFromLabel(String.valueOf(value));
                        break;
                    case dstPrimaryLabel:
                        neo4jElement.setToLabel(String.valueOf(value));
                        break;
                    case nodeProperty:
                    case relationshipProperty:
                    case nodeJsonProperty:
                    case relationshipJsonProperty:
                        if (value != null) {
                            neo4jElement.getProperties().put(name, type.applyObject(value));
                        }
                    default:
                        break;
                }
            });
        };
    }
}