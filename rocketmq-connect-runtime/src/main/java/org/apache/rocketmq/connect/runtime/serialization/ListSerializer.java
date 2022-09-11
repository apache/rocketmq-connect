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

import com.alibaba.fastjson.JSON;
import org.apache.rocketmq.connect.runtime.common.LoggerName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Convert between a list and byte[].
 */
public class ListSerializer implements Serializer<List> {

    private static final Logger log = LoggerFactory.getLogger(LoggerName.ROCKETMQ_RUNTIME);

    private Class clazz;

    public ListSerializer(Class clazz) {
        this.clazz = clazz;
    }

    @Override
    public byte[] serialize(String topic, List data) {
        try {
            return JSON.toJSONString(data).getBytes("UTF-8");
        } catch (Exception e) {
            log.error("ListSerializer serialize failed", e);
        }
        return null;
    }
}
