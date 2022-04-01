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

package org.apache.rocketmq.connect.runtime.converter;

import com.alibaba.fastjson.JSON;
import io.openmessaging.connector.api.data.Converter;
import io.openmessaging.connector.api.data.RecordOffset;
import io.openmessaging.connector.api.data.RecordPartition;
import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.Map;
import org.apache.rocketmq.connect.runtime.common.LoggerName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Byte Map to byte[].
 */
public class RecordPositionMapConverter implements Converter<Map<RecordPartition, RecordOffset>> {

    private static final Logger log = LoggerFactory.getLogger(LoggerName.ROCKETMQ_RUNTIME);

    @Override
    public byte[] objectToByte(Map<RecordPartition, RecordOffset> map) {

        try {
            Map<String, String> resultMap = new HashMap<>();

            for (Map.Entry<RecordPartition, RecordOffset> entry : map.entrySet()) {
                String jsonKey = JSON.toJSONString(entry.getKey());
                jsonKey.getBytes("UTF-8");
                String jsonValue = JSON.toJSONString(entry.getValue());
                jsonValue.getBytes("UTF-8");
                resultMap.put(jsonKey, jsonValue);
            }
            return JSON.toJSONString(resultMap).getBytes("UTF-8");
        } catch (Exception e) {
            log.error("ByteMapConverter#objectToByte failed", e);
        }
        return new byte[0];
    }

    @Override
    public Map<RecordPartition, RecordOffset> byteToObject(byte[] bytes) {

        Map<RecordPartition, RecordOffset> resultMap = new HashMap<>();
        try {
            String rawString = new String(bytes, "UTF-8");
            Map<String, String> map = JSON.parseObject(rawString, Map.class);
            for (String key : map.keySet()) {
                RecordPartition recordPartition = JSON.parseObject(key, RecordPartition.class);
                RecordOffset recordOffset = JSON.parseObject(map.get(key), RecordOffset.class);
                resultMap.put(recordPartition, recordOffset);
            }
            return resultMap;
        } catch (UnsupportedEncodingException e) {
            log.error("ByteMapConverter#byteToObject failed", e);
        }
        return resultMap;
    }

}
