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
import org.apache.rocketmq.connect.runtime.common.ConnectKeyValue;
import org.apache.rocketmq.connect.runtime.common.LoggerName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;

/**
 * Converter data between ConnAndTaskConfigs and byte[].
 */
public class ConnectKeyValueConverter implements Converter<ConnectKeyValue> {

    private static final Logger log = LoggerFactory.getLogger(LoggerName.ROCKETMQ_RUNTIME);

    @Override
    public byte[] objectToByte(ConnectKeyValue config) {
        try {
            return JSON.toJSONString(config).getBytes("UTF-8");
        } catch (Exception e) {
            log.error("ConnectKeyValueConverter#objectToByte failed", e);
        }
        return new byte[0];
    }

    @Override
    public ConnectKeyValue byteToObject(byte[] bytes) {

        try {
            String jsonString = new String(bytes, "UTF-8");
            return JSON.parseObject(jsonString, ConnectKeyValue.class);
        } catch (UnsupportedEncodingException e) {
            log.error("ConnAndTaskConfigConverter#byteToObject failed", e);
        }
        return null;
    }
}
