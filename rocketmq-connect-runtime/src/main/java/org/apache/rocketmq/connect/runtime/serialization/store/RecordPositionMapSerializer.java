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
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.rocketmq.connect.runtime.serialization.store;

import com.alibaba.fastjson.JSON;
import io.openmessaging.connector.api.data.RecordOffset;
import org.apache.rocketmq.connect.runtime.common.LoggerName;
import org.apache.rocketmq.connect.runtime.serialization.Serializer;
import org.apache.rocketmq.connect.runtime.store.ExtendRecordPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * Byte Map to byte[].
 */
public class RecordPositionMapSerializer implements Serializer<Map<ExtendRecordPartition, RecordOffset>> {

    private static final Logger log = LoggerFactory.getLogger(LoggerName.ROCKETMQ_RUNTIME);

    @Override
    public byte[] serialize(String topic, Map<ExtendRecordPartition, RecordOffset> data) {
        try {
            Map<String, String> resultMap = new HashMap<>();
            for (Map.Entry<ExtendRecordPartition, RecordOffset> entry : data.entrySet()) {
                String jsonKey = JSON.toJSONString(entry.getKey());
                jsonKey.getBytes("UTF-8");
                String jsonValue = JSON.toJSONString(entry.getValue());
                jsonValue.getBytes("UTF-8");
                resultMap.put(jsonKey, jsonValue);
            }
            return JSON.toJSONString(resultMap).getBytes("UTF-8");
        } catch (Exception e) {
            log.error("RecordPositionMapSerializer serialize failed", e);
        }
        return new byte[0];
    }
}
