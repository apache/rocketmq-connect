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
package org.apache.rocketmq.schema.json;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.rocketmq.schema.common.ParsedSchema;
import org.apache.rocketmq.schema.registry.client.rest.JacksonMapper;
import org.apache.rocketmq.schema.registry.common.model.SchemaType;
import org.everit.json.schema.Schema;
import org.everit.json.schema.loader.SchemaLoader;
import org.json.JSONObject;

import java.io.IOException;
import java.util.Objects;

/**
 * Json schema
 */
public class JsonSchema implements ParsedSchema<Schema> {

    private static final ObjectMapper OBJECT_MAPPER = JacksonMapper.INSTANCE;
    private final JsonNode jsonNode;
    private final Integer version;
    private transient org.everit.json.schema.Schema schemaObj;
    private transient String canonicalString;

    public JsonSchema(JsonNode jsonNode) {
        this(jsonNode, null);
    }

    public JsonSchema(JsonNode jsonNode, Integer version) {
        this.jsonNode = jsonNode;
        this.version = version;
    }

    public JsonSchema(String schemaString) {
        this(schemaString, null);
    }

    public JsonSchema(String schemaString, Integer version) {
        try {
            this.jsonNode = OBJECT_MAPPER.readTree(schemaString);
            this.version = version;
        } catch (IOException e) {
            throw new IllegalArgumentException("Invalid JSON " + schemaString, e);
        }
    }

    public JsonSchema(org.everit.json.schema.Schema schemaObj) {
        this(schemaObj, null);
    }

    public JsonSchema(org.everit.json.schema.Schema schemaObj, Integer version) {
        try {
            this.jsonNode = schemaObj != null ? OBJECT_MAPPER.readTree(schemaObj.toString()) : null;
            this.schemaObj = schemaObj;
            this.version = version;
        } catch (IOException e) {
            throw new IllegalArgumentException("Invalid JSON " + schemaObj, e);
        }
    }

    @Override
    public SchemaType schemaType() {
        return SchemaType.JSON;
    }

    @Override
    public Schema rawSchema() {
        if (jsonNode == null) {
            return null;
        }
        if (schemaObj == null) {
            try {
                SchemaLoader.SchemaLoaderBuilder builder = SchemaLoader.builder()
                        .useDefaults(true).draftV7Support();
                JSONObject jsonObject = new JSONObject(OBJECT_MAPPER.writeValueAsString(jsonNode));
                builder.schemaJson(jsonObject);
                SchemaLoader loader = builder.build();
                schemaObj = loader.load().build();
            } catch (IOException e) {
                throw new IllegalArgumentException("Invalid JSON", e);
            }
        }
        return schemaObj;
    }

    @Override
    public Integer version() {
        return version;
    }

    @Override
    public String idl() {
        if (jsonNode == null) {
            return null;
        }
        if (canonicalString == null) {
            try {
                canonicalString = OBJECT_MAPPER.writeValueAsString(jsonNode);
            } catch (IOException e) {
                throw new IllegalArgumentException("Invalid JSON", e);
            }
        }
        return canonicalString;
    }

    @Override
    public String name() {
        return getString("title");
    }

    public String getString(String key) {
        return jsonNode.has(key) ? jsonNode.get(key).asText() : null;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        JsonSchema that = (JsonSchema) o;
        return Objects.equals(jsonNode, that.jsonNode)
                && Objects.equals(schemaObj, that.schemaObj)
                && Objects.equals(version, that.version);
    }

    @Override
    public int hashCode() {
        return Objects.hash(jsonNode, schemaObj, version);
    }

    @Override
    public String toString() {
        return idl();
    }
}
