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

import java.lang.reflect.Array;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.*;
import java.util.stream.Collectors;

import org.neo4j.driver.types.Node;
import org.neo4j.driver.types.Point;
import org.neo4j.driver.types.Relationship;

import io.openmessaging.connector.api.data.*;

public class RecordConverter {

    public static Map<String, Object> asNodeMap(Node node) {
        final long id = node.id();
        final Iterable<String> labels = node.labels();
        List<String> labelList = new ArrayList<>();
        for (String label : labels) {
            labelList.add(label);
        }
        final Map<String, Object> asMap = node.asMap();
        Map<String, Object> valueMap = new HashMap<>(asMap);
        valueMap.put("<node_id>", id);
        valueMap.put("<labels>", labelsMerge(labelList));
        return valueMap;
    }

    public static Map<String, Object> asRelationshipMap(Relationship relationship) {
        final long startNodeId = relationship.startNodeId();
        final long endNodeId = relationship.endNodeId();
        final String type = relationship.type();
        final long id = relationship.id();

        final Map<String, Object> asMap = relationship.asMap();
        Map<String, Object> valueMap = new HashMap<>(asMap);
        valueMap.put("<relationship_id>", id);
        valueMap.put("<type>", type);
        valueMap.put("<source_id>", startNodeId);
        valueMap.put("<target_id>", endNodeId);
        return valueMap;
    }

    public static Schema map2StructSchema(Map<String, Object> map) {
        final SchemaBuilder structBuilder = SchemaBuilder.struct().optional();
        map.forEach((k, v) -> {
            final Schema valueSchema = neo4jValueSchema(v);
            if (valueSchema != null) {
                structBuilder.field(k, valueSchema);
            }
        });
        if (structBuilder.fields().isEmpty()) {
            return null;
        } else {
            return structBuilder.build();
        }
    }

    public static Struct buildStruct(Schema schema, Map<String, Object> columnMap) {
        Struct struct = new Struct(schema);
        final List<Field> fields = schema.getFields();
        for (Field field : fields) {
            struct.put(field, columnMap.get(field.getName()));
        }
        return struct;
    }

    private static Schema neo4jValueSchema(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof String) {
            return SchemaBuilder.string().build();
        }
        if (value instanceof Integer) {
            return SchemaBuilder.int32().build();
        }
        if (value instanceof Long) {
            return SchemaBuilder.int64().build();
        }
        if (value instanceof Float) {
            return SchemaBuilder.float32().build();
        }
        if (value instanceof Double) {
            return SchemaBuilder.float64().build();
        }
        if (value instanceof Boolean) {
            return SchemaBuilder.bool().build();
        }
        if (value instanceof LocalDateTime || value instanceof LocalDate || value instanceof Date) {
            return SchemaBuilder.date().build();
        }
        if (value instanceof Collection) {
            Collection collection = (Collection)value;
            if (collection.size() > 0) {
                final Object v = collection.stream().findFirst().orElse(null);
                Schema valueSchema = neo4jValueSchema(v);
                return Optional.ofNullable(valueSchema).map(x -> SchemaBuilder.array(valueSchema).build()).orElse(null);
            } else {
                return null;
            }
        }

        if (value instanceof Array) {
            Object[] array = (Object[])value;
            if (array.length > 0) {
                final Object v = array[0];
                Schema valueSchema = neo4jValueSchema(v);
                return Optional.ofNullable(valueSchema).map(x -> SchemaBuilder.array(valueSchema).build()).orElse(null);
            } else {
                return null;
            }
        }
        if (value instanceof Map) {
            Map<String, Object> map = (Map<String, Object>)value;
            if (map.isEmpty()) {
                return SchemaBuilder.map(SchemaBuilder.string().build(), SchemaBuilder.string().optional().build())
                    .optional().build();
            } else {
                final Set<String> valueTypes =
                    map.values().stream().map(i -> i.getClass().getName()).collect(Collectors.toSet());
                if (valueTypes.size() == 1) {
                    final Object firstV = map.values().stream().findFirst().orElse(null);
                    final Schema valueSchema = neo4jValueSchema(firstV);
                    return SchemaBuilder.map(SchemaBuilder.string().build(), valueSchema).build();
                } else {
                    map2StructSchema(map);
                }
            }
        }
        if (value instanceof Point) {
            final SchemaBuilder structBuilder = SchemaBuilder.struct().optional();
            structBuilder.field("srid", SchemaBuilder.int32().build());
            structBuilder.field("x", SchemaBuilder.float64().build());
            structBuilder.field("y", SchemaBuilder.float64().build());
            structBuilder.field("z", SchemaBuilder.float64().build());
            return structBuilder.build();
        }
        if (value instanceof Node) {
            final Map<String, Object> nodeMap = asNodeMap((Node)value);
            return map2StructSchema(nodeMap);
        }
        if (value instanceof Relationship) {
            final Map<String, Object> relationshipMap = asRelationshipMap((Relationship)value);
            return map2StructSchema(relationshipMap);
        } else {
            return SchemaBuilder.string().build();
        }
    }

    private static String labelsMerge(List<String> labelList) {
        if (labelList == null || labelList.isEmpty()) {
            return "";
        }
        StringBuilder stringBuilder = new StringBuilder();
        for (String label : labelList) {
            stringBuilder.append(":").append(label);
        }
        return stringBuilder.substring(1);
    }
}