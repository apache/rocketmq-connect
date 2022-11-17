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
package org.apache.rocketmq.schema.json.serde;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.rocketmq.schema.common.Deserializer;
import org.apache.rocketmq.schema.json.JsonSchema;
import org.apache.rocketmq.schema.json.JsonSchemaAndValue;
import org.apache.rocketmq.schema.json.JsonSchemaConverterConfig;
import org.apache.rocketmq.schema.json.JsonSchemaData;
import org.apache.rocketmq.schema.json.JsonSchemaRegistryClient;
import org.apache.rocketmq.schema.json.util.JsonSchemaUtils;
import org.apache.rocketmq.schema.registry.client.rest.JacksonMapper;
import org.apache.rocketmq.schema.registry.common.dto.GetSchemaResponse;
import org.everit.json.schema.Schema;
import org.everit.json.schema.loader.SchemaLoader;
import org.json.JSONObject;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;


/**
 * json schema deserializer
 */
public class JsonSchemaDeserializer implements Deserializer<JsonSchemaAndValue> {

    protected static final int ID_SIZE = 8;
    private static final ObjectMapper OBJECT_MAPPER = JacksonMapper.INSTANCE;
    private JsonSchemaRegistryClient schemaRegistryClient;
    private JsonSchemaConverterConfig jsonSchemaConverterConfig;

    @Override
    public void configure(Map<String, ?> props) {
        this.jsonSchemaConverterConfig = new JsonSchemaConverterConfig(props);
        this.schemaRegistryClient = new JsonSchemaRegistryClient(this.jsonSchemaConverterConfig);
    }

    /**
     * deserialize
     *
     * @param topic
     * @param isKey
     * @param payload
     * @return
     */
    @Override
    public JsonSchemaAndValue deserialize(String topic, boolean isKey, byte[] payload) {
        if (payload == null) {
            return null;
        }

        ByteBuffer buffer = ByteBuffer.wrap(payload);
        long recordId = buffer.getLong();
        GetSchemaResponse response = schemaRegistryClient.getSchemaByRecordId(JsonSchemaData.NAMESPACE, topic, recordId);

        int length = buffer.limit() - ID_SIZE;
        int start = buffer.position() + buffer.arrayOffset();

        // Return JsonNode if type is null
        JsonNode value = null;
        try {
            value = OBJECT_MAPPER.readTree(new ByteArrayInputStream(buffer.array(), start, length));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        // load json schema
        SchemaLoader.SchemaLoaderBuilder schemaLoaderBuilder = SchemaLoader
                .builder()
                .useDefaults(true)
                .draftV7Support();
        JSONObject jsonObject = new JSONObject(response.getIdl());
        schemaLoaderBuilder.schemaJson(jsonObject);
        Schema schema = schemaLoaderBuilder.build().load().build();
        // validate schema
        if (jsonSchemaConverterConfig.validate()) {
            try {
                JsonSchemaUtils.validate(schema, value);
            } catch (JsonProcessingException e) {
                throw new RuntimeException(e);
            }
        }
        return new JsonSchemaAndValue(new JsonSchema(schema), value);
    }
}
