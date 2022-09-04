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
package org.apache.rocketmq.connect.doris.serialization;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.SerializerFeature;
import io.openmessaging.connector.api.errors.ConnectException;

import java.nio.charset.StandardCharsets;
import java.util.Objects;

/**
 * json serializer
 */
public class JsonSerializer implements Serializer<Object> {


    /**
     * Convert {@code data} into a byte array.
     *
     * @param topic topic associated with data
     * @param data  typed data
     * @return serialized bytes
     */
    @Override
    public byte[] serialize(String topic, Object data) {
        if (Objects.isNull(data)) {
            return null;
        }
        try {
            return JSON.toJSONString(data, SerializerFeature.DisableCircularReferenceDetect,  SerializerFeature.WriteMapNullValue).getBytes(StandardCharsets.UTF_8);
        } catch (Exception e) {
            throw new ConnectException("Error serializing JSON message", e);
        }
    }
}
