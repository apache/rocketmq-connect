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
package org.apache.rocketmq.schema.json.util;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.BinaryNode;
import com.fasterxml.jackson.databind.node.BooleanNode;
import com.fasterxml.jackson.databind.node.NullNode;
import com.fasterxml.jackson.databind.node.NumericNode;
import com.fasterxml.jackson.databind.node.TextNode;
import org.apache.rocketmq.schema.registry.client.rest.JacksonMapper;
import org.everit.json.schema.Schema;
import org.everit.json.schema.ValidationException;
import org.json.JSONArray;
import org.json.JSONObject;

/**
 * json schema utils
 */
public class JsonSchemaUtils {
    private static final ObjectMapper OBJECT_MAPPER = JacksonMapper.INSTANCE;
    private static final Object NONE_MARKER = new Object();

    /**
     * validate object
     *
     * @param schema
     * @param value
     * @throws JsonProcessingException
     * @throws ValidationException
     */
    public static void validate(Schema schema, Object value) throws JsonProcessingException, ValidationException {
        Object primitiveValue = NONE_MARKER;
        if (isPrimitive(value)) {
            primitiveValue = value;
        } else if (value instanceof BinaryNode) {
            primitiveValue = ((BinaryNode) value).asText();
        } else if (value instanceof BooleanNode) {
            primitiveValue = ((BooleanNode) value).asBoolean();
        } else if (value instanceof NullNode) {
            primitiveValue = null;
        } else if (value instanceof NumericNode) {
            primitiveValue = ((NumericNode) value).numberValue();
        } else if (value instanceof TextNode) {
            primitiveValue = ((TextNode) value).asText();
        }
        if (primitiveValue != NONE_MARKER) {
            schema.validate(primitiveValue);
        } else {
            Object jsonObject;
            if (value instanceof ArrayNode) {
                jsonObject = OBJECT_MAPPER.treeToValue((ArrayNode) value, JSONArray.class);
            } else if (value instanceof JsonNode) {
                jsonObject = new JSONObject(OBJECT_MAPPER.writeValueAsString(value));
            } else if (value.getClass().isArray()) {
                jsonObject = OBJECT_MAPPER.convertValue(value, JSONArray.class);
            } else {
                jsonObject = OBJECT_MAPPER.convertValue(value, JSONObject.class);
            }
            schema.validate(jsonObject);
        }
    }


    private static boolean isPrimitive(Object value) {
        return value == null
                || value instanceof Boolean
                || value instanceof Number
                || value instanceof String;
    }

}
