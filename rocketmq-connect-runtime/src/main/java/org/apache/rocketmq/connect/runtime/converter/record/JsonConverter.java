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

import io.openmessaging.connector.api.data.Schema;
import java.util.Map;


/**
 * json converter
 */
public class JsonConverter implements RecordConverter {

    /**
     * Configure this class.
     *
     * @param configs configs in key/value pairs
     */
    @Override
    public void configure(Map<String, ?> configs) {

    }

    /**
     * Convert a rocketmq Connect data object to a native object for serialization.
     *
     * @param topic  the topic associated with the data
     * @param schema the schema for the value
     * @param value  the value to convert
     * @return the serialized value
     */
    @Override
    public byte[] fromConnectData(String topic, Schema schema, Object value) {
        return new byte[0];
    }

    /**
     * Convert a native object to a Rocketmq Connect data object.
     *
     * @param topic the topic associated with the data
     * @param value the value to convert
     * @return an object containing the {@link Schema} and the converted value
     */
    @Override
    public SchemaAndValue toConnectData(String topic, byte[] value) {
        return null;
    }
}
