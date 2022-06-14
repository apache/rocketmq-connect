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

import io.openmessaging.KeyValue;
import io.openmessaging.connector.api.data.Schema;
import io.openmessaging.connector.api.data.SchemaAndValue;

import java.util.Map;

/**
 * The topic parameter is mainly a compatible schema registry
 * abstract converter
 */
public interface RecordConverter {


    /**
     * Config is used for parameter passing in the conversion process
     * @param configs
     */
    default void configure(Map<String, ?> configs) {

    }

    /**
     * Convert ConnectRecord  to byte[]
     * @param topic  the topic associated with the data
     * @param schema record schema
     * @param value  record value
     * @return
     */
    byte[] fromConnectData(String topic, Schema schema, Object value);


    /**
     * The provided subject and extension may be used in the record as needed.
     * @param topic the topic associated with the data
     * @param extensions
     * @param schema  rocketmq connect record schema
     * @param value rocketmq connect record value
     * @return
     */
    default byte[] fromConnectData(String topic, KeyValue extensions, Schema schema, Object value) {
        return fromConnectData(topic, schema, value);
    }

    /**
     * Convert a byte[] object to a Rocketmq Connect data object.
     *
     * @param topic the topic associated with the data
     * @param value the value to convert
     * @return an object containing the {@link Schema} and the converted value
     */
    SchemaAndValue toConnectData(String topic, byte[] value);


    /**
     * The provided subject and extension may be used in the record as needed.
     * @param topic  the topic associated with the data
     * @param extensions transform property
     * @param value
     * @return
     */
    default SchemaAndValue toConnectData(String topic, KeyValue extensions, byte[] value) {
        return toConnectData(topic, value);
    }

}
