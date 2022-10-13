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

package org.apache.rocketmq.schema.avro;

import io.openmessaging.connector.api.data.RecordConverter;
import io.openmessaging.connector.api.data.Schema;
import io.openmessaging.connector.api.data.SchemaAndValue;
import io.openmessaging.connector.api.errors.ConnectException;
import org.apache.avro.generic.GenericContainer;
import org.apache.avro.generic.IndexedRecord;
import org.apache.rocketmq.schema.avro.serde.AvroDeserializer;
import org.apache.rocketmq.schema.avro.serde.AvroSerializer;

import java.util.Map;


/**
 * avro converter
 */
public class AvroConverter implements RecordConverter {

    private AvroData avroData;
    private AvroSerializer serializer;
    private AvroDeserializer deserializer;
    private boolean isKey;

    @Override
    public void configure(Map<String, ?> configs) {
        AvroConverterConfig converterConfig = new AvroConverterConfig(configs);
        isKey = converterConfig.isKey();
        AvroDataConfig avroDataConfig = new AvroDataConfig(configs);
        this.avroData = new AvroData(avroDataConfig);
        serializer = new AvroSerializer();
        serializer.configure(configs);
        deserializer = new AvroDeserializer();
        deserializer.configure(configs);
    }

    /**
     * from connect data
     *
     * @param topic  the topic associated with the data
     * @param schema record schema
     * @param value  record value
     * @return
     */
    @Override
    public byte[] fromConnectData(String topic, Schema schema, Object value) {
        if (value == null) {
            return null;
        }
        org.apache.avro.Schema avroSchema = avroData.fromConnectSchema(schema);
        Object object = avroData.fromConnectData(schema, value);
        return serializer.serialize(topic, isKey, new AvroSchema(avroSchema), object);
    }

    @Override
    public SchemaAndValue toConnectData(String topic, byte[] value) {
        GenericContainerWithVersion genericContainerWithVersion = deserializer.deserialize(topic, isKey, value);
        if (genericContainerWithVersion == null) {
            return SchemaAndValue.NULL;
        }
        GenericContainer deserialized = genericContainerWithVersion.container();
        Integer version = genericContainerWithVersion.version();
        if (deserialized instanceof IndexedRecord) {
            return avroData.toConnectData(deserialized.getSchema(), deserialized, version);
        } else if (deserialized instanceof NonRecordContainer) {
            return avroData.toConnectData(
                    deserialized.getSchema(), ((NonRecordContainer) deserialized).getValue(), version);
        }
        throw new ConnectException(
                String.format("Unsupported type returned during deserialization of topic %s ", topic)
        );
    }
}
