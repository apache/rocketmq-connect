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

import io.openmessaging.connector.api.data.FieldType;
import io.openmessaging.connector.api.data.RecordConverter;
import io.openmessaging.connector.api.data.Schema;
import io.openmessaging.connector.api.data.SchemaAndValue;
import io.openmessaging.connector.api.data.SchemaBuilder;
import io.openmessaging.connector.api.errors.ConnectException;

import java.util.Map;


/**
 * byte array converter
 */
public class ByteArrayConverter implements RecordConverter {

    /**
     * Configure this class.
     *
     * @param configs configs in key/value pairs
     */
    @Override
    public void configure(Map configs) {
        // config
    }

    @Override
    public byte[] fromConnectData(String topic, Schema schema, Object value) {
        if (schema != null && schema.getFieldType() != FieldType.BYTES) {
            throw new ConnectException("Invalid schema type for ByteArrayConverter: " + schema.getFieldType().toString());
        }
        if (value != null && !(value instanceof byte[])) {
            throw new ConnectException("ByteArrayConverter is not compatible with objects of type " + value.getClass());
        }
        return (byte[]) value;
    }

    @Override
    public SchemaAndValue toConnectData(String topic, byte[] value) {
        return new SchemaAndValue(SchemaBuilder.bytes().build(), value);
    }

}
