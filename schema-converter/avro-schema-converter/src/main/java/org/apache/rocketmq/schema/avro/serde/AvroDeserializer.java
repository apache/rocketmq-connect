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
import org.apache.avro.generic.GenericContainer;
import org.apache.rocketmq.schema.avro.AvroConverterConfig;
import org.apache.rocketmq.schema.avro.AvroData;
import org.apache.rocketmq.schema.avro.AvroSchema;
import org.apache.rocketmq.schema.avro.AvroSchemaRegistryClient;
import org.apache.rocketmq.schema.avro.GenericContainerWithVersion;
import org.apache.rocketmq.schema.avro.NonRecordContainer;
import org.apache.rocketmq.schema.common.Deserializer;
import org.apache.rocketmq.schema.common.TopicNameStrategy;
import org.apache.rocketmq.schema.registry.common.dto.GetSchemaResponse;

import java.nio.ByteBuffer;
import java.util.Map;

/**
 * avro serializer
 */
public class AvroDeserializer implements Deserializer<GenericContainerWithVersion> {
    private AvroSchemaRegistryClient schemaRegistryClient;
    private AvroDatumReaderFactory avroDatumReaderFactory;

    @Override
    public void configure(Map<String, ?> props) {
        AvroConverterConfig converterConfig = new AvroConverterConfig(props);
        this.schemaRegistryClient = new AvroSchemaRegistryClient(converterConfig);
        this.avroDatumReaderFactory = AvroDatumReaderFactory.get(
                converterConfig.useSchemaReflection(),
                converterConfig.avroUseLogicalTypeConverters(),
                converterConfig.specificAvroReaderConfig(),
                converterConfig.avroReflectionAllowNullConfig()
        );
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
    public GenericContainerWithVersion deserialize(String topic, boolean isKey, byte[] payload) {
        if (payload == null) {
            return null;
        }
        // get subject name
        String subjectName = TopicNameStrategy.subjectName(topic, isKey);
        ByteBuffer byteBuffer = ByteBuffer.wrap(payload);
        long recordId = byteBuffer.getLong();
        GetSchemaResponse schemaResponse = schemaRegistryClient.getSchemaByRecordId(AvroData.NAMESPACE, topic, recordId);
        String avroSchemaIdl = schemaResponse.getIdl();
        Integer version = Integer.parseInt(String.valueOf(schemaResponse.getVersion()));
        AvroSchema avroSchema = new AvroSchema(avroSchemaIdl);
        Object result = this.avroDatumReaderFactory.read(byteBuffer, avroSchema.rawSchema(), null);
        if (avroSchema.rawSchema().getType().equals(Schema.Type.RECORD)) {
            return new GenericContainerWithVersion((GenericContainer) result, version);
        } else {
            return new GenericContainerWithVersion(
                    new NonRecordContainer(avroSchema.rawSchema(), result),
                    version
            );
        }
    }

}
