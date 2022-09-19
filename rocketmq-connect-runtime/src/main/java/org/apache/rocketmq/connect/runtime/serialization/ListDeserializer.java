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

package org.apache.rocketmq.connect.runtime.serialization;

import com.alibaba.fastjson.JSONArray;
import org.apache.rocketmq.connect.runtime.common.LoggerName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.util.List;

/**
 * Convert between a list and byte[].
 */
public class ListDeserializer implements Deserializer<List> {

    private static final Logger log = LoggerFactory.getLogger(LoggerName.ROCKETMQ_RUNTIME);

    private Class clazz;

    public ListDeserializer(Class clazz) {
        this.clazz = clazz;
    }

    @Override
    public List deserialize(String topic, byte[] data) {
        try {
            String json = new String(data, "UTF-8");
            List list = JSONArray.parseArray(json, clazz);
            return list;
        } catch (UnsupportedEncodingException e) {
            log.error("ListConverter#byteToObject failed", e);
        }
        return null;
    }
}
