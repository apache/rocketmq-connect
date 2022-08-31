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
import org.apache.rocketmq.connect.runtime.serialization.Deserializer;
import org.apache.rocketmq.connect.runtime.store.ExtendRecordPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.Map;

/**
 * Byte Map to byte[].
 */
public class RecordPositionMapDeserializer implements Deserializer<Map<ExtendRecordPartition, RecordOffset>> {

    private static final Logger log = LoggerFactory.getLogger(LoggerName.ROCKETMQ_RUNTIME);

    @Override
    public Map<ExtendRecordPartition, RecordOffset> deserialize(String topic, byte[] data) {
        Map<ExtendRecordPartition, RecordOffset> resultMap = new HashMap<>();
        try {
            String rawString = new String(data, "UTF-8");
            Map<String, String> map = JSON.parseObject(rawString, Map.class);
            for (String key : map.keySet()) {
                ExtendRecordPartition recordPartition = JSON.parseObject(key, ExtendRecordPartition.class);
                RecordOffset recordOffset = JSON.parseObject(map.get(key), RecordOffset.class);
                resultMap.put(recordPartition, recordOffset);
            }
            return resultMap;
        } catch (UnsupportedEncodingException e) {
            log.error("RecordPositionMapDeserializer deserialize failed", e);
        }
        return resultMap;
    }
}
