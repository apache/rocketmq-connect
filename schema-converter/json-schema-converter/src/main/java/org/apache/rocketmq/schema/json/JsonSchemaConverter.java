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
import io.openmessaging.connector.api.data.RecordConverter;
import io.openmessaging.connector.api.data.Schema;
import io.openmessaging.connector.api.data.SchemaAndValue;
import lombok.SneakyThrows;
import org.apache.rocketmq.schema.json.serde.JsonSchemaDeserializer;
import org.apache.rocketmq.schema.json.serde.JsonSchemaSerializer;

import java.util.Map;

/**
 * json schema converter
 */
public class JsonSchemaConverter implements RecordConverter {
    private JsonSchemaSerializer serializer;
    private JsonSchemaDeserializer deserializer;
    /**
     * is key
     */
    private boolean isKey = false;

    /**
     * json schema data
     */
    private JsonSchemaData jsonSchemaData;

    @Override
    public void configure(Map<String, ?> configs) {
        JsonSchemaConverterConfig jsonSchemaConfig = new JsonSchemaConverterConfig(configs);
        // convert key
        this.isKey = jsonSchemaConfig.isKey();
        // serializer
        serializer = new JsonSchemaSerializer();
        serializer.configure(configs);
        // deserializer
        deserializer = new JsonSchemaDeserializer();
        deserializer.configure(configs);
        // json schema data
        jsonSchemaData = new JsonSchemaData(jsonSchemaConfig);
    }

    @SneakyThrows
    @Override
    public byte[] fromConnectData(String topic, Schema schema, Object value) {
        if (schema == null && value == null) {
            return null;
        }
        org.everit.json.schema.Schema jsonSchema = jsonSchemaData.fromJsonSchema(schema);
        JsonNode jsonNode = jsonSchemaData.fromConnectData(schema, value);
        return serializer.serialize(topic, isKey, new JsonSchema(jsonSchema), jsonNode);
    }

    @Override
    public SchemaAndValue toConnectData(String topic, byte[] value) {
        if (value == null) {
            return SchemaAndValue.NULL;
        }
        JsonSchemaAndValue jsonSchemaAndValue = deserializer.deserialize(topic, isKey, value);
        Schema schema = jsonSchemaData.toConnectSchema(jsonSchemaAndValue.getSchema().rawSchema());
        Object result = JsonSchemaData.toConnectData(schema, jsonSchemaAndValue.getValue());
        return new SchemaAndValue(schema, result);
    }

}
