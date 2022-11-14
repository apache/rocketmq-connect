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

package org.apache.rocketmq.schema.avro.serde;

import org.apache.avro.Schema;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.schema.avro.AvroConverterConfig;
import org.apache.rocketmq.schema.avro.AvroData;
import org.apache.rocketmq.schema.avro.AvroSchema;
import org.apache.rocketmq.schema.avro.AvroSchemaRegistryClient;
import org.apache.rocketmq.schema.avro.NonRecordContainer;
import org.apache.rocketmq.schema.common.SchemaResponse;
import org.apache.rocketmq.schema.common.Serializer;
import org.apache.rocketmq.schema.common.TopicNameStrategy;
import org.apache.rocketmq.schema.registry.client.exceptions.SerializationException;
import org.apache.rocketmq.schema.registry.common.dto.RegisterSchemaRequest;
import org.apache.rocketmq.schema.registry.common.model.Compatibility;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;

/**
 * avro serializer
 */
public class AvroSerializer implements Serializer<AvroSchema> {

    private final int idSize = 8;
    private AvroSchemaRegistryClient schemaRegistryClient;
    private AvroDatumWriterFactory avroDatumWriterFactory;


    @Override
    public void configure(Map<String, ?> props) {
        AvroConverterConfig config = new AvroConverterConfig(props);
        this.schemaRegistryClient = new AvroSchemaRegistryClient(config);
        this.avroDatumWriterFactory = AvroDatumWriterFactory.get(
                config.useSchemaReflection(),
                config.avroUseLogicalTypeConverters()
        );
    }

    /**
     * avro serialize
     *
     * @param topic
     * @param isKey
     * @param schema
     * @param data
     * @return
     */
    public byte[] serialize(String topic, boolean isKey, AvroSchema schema, Object data) {
        if (data == null) {
            return null;
        }
        String subjectName = TopicNameStrategy.subjectName(topic, isKey);
        org.apache.avro.Schema avroSchema = schema.rawSchema();
        try {
            RegisterSchemaRequest registerSchemaRequest = RegisterSchemaRequest.builder()
                    .schemaType(schema.schemaType())
                    .compatibility(Compatibility.BACKWARD)
                    .schemaIdl(avroSchema.toString()).build();
            SchemaResponse schemaResponse = schemaRegistryClient.autoRegisterOrGetSchema(AvroData.NAMESPACE, topic, subjectName, registerSchemaRequest, schema);
            long recordId = schemaResponse.getRecordId();
            // parse idl
            if (StringUtils.isNotEmpty(schemaResponse.getIdl())) {
                schema = new AvroSchema(schemaResponse.getIdl());
            }
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            // Add record id in the header
            out.write(ByteBuffer.allocate(idSize).putLong(recordId).array());

            Object value = data instanceof NonRecordContainer
                    ? ((NonRecordContainer) data).getValue()
                    : data;
            Schema rawSchema = schema.rawSchema();
            // bytes
            if (rawSchema.getType().equals(Schema.Type.BYTES)) {
                if (value instanceof byte[]) {
                    out.write((byte[]) value);
                } else if (value instanceof ByteBuffer) {
                    out.write(((ByteBuffer) value).array());
                } else {
                    throw new SerializationException(
                            "Unrecognized bytes object of type: " + value.getClass().getName());
                }
            } else {
                // not bytes
                this.avroDatumWriterFactory.writeDatum(out, value, rawSchema);
            }
            byte[] bytes = out.toByteArray();
            out.close();
            return bytes;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

}
