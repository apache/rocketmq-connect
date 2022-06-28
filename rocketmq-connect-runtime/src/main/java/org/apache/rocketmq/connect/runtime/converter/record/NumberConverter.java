/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rocketmq.connect.runtime.converter.record;

import io.openmessaging.connector.api.data.RecordConverter;
import io.openmessaging.connector.api.data.Schema;
import io.openmessaging.connector.api.data.SchemaAndValue;
import io.openmessaging.connector.api.errors.ConnectException;
import org.apache.rocketmq.connect.runtime.serialization.Deserializer;
import org.apache.rocketmq.connect.runtime.serialization.Serializer;

import java.util.Map;


/**
 * number converter
 *
 * @param <T>
 */
abstract class NumberConverter<T extends Number> implements RecordConverter {

    private final Serializer<T> serializer;
    private final Deserializer<T> deserializer;
    private final String typeName;
    private final Schema schema;

    /**
     * Create the converter.
     *
     * @param typeName     the displayable name of the type; may not be null
     * @param schema       the optional schema to be used for all deserialized forms; may not be null
     * @param serializer   the serializer; may not be null
     * @param deserializer the deserializer; may not be null
     */
    protected NumberConverter(String typeName, Schema schema, Serializer<T> serializer, Deserializer<T> deserializer) {
        this.typeName = typeName;
        this.schema = schema;
        this.serializer = serializer;
        this.deserializer = deserializer;
        assert this.serializer != null;
        assert this.deserializer != null;
        assert this.typeName != null;
        assert this.schema != null;
    }

    @Override
    public void configure(Map<String, ?> configs) {
        serializer.configure(configs);
        deserializer.configure(configs);
    }

    @Override
    public byte[] fromConnectData(String topic, Schema schema, Object value) {
        try {
            return serializer.serialize(topic, value == null ? null : cast(value));
        } catch (Exception e) {
            throw new io.openmessaging.connector.api.errors.ConnectException("Failed to serialize to " + typeName + " (was " + value.getClass() + "): ", e);
        }
    }

    @Override
    public SchemaAndValue toConnectData(String topic, byte[] value) {
        try {
            return new SchemaAndValue(schema, deserializer.deserialize(topic, value));
        } catch (RuntimeException e) {
            throw new ConnectException("Failed to deserialize " + typeName + ": ", e);
        }
    }

    protected T cast(Object value) {
        return (T) value;
    }
}
