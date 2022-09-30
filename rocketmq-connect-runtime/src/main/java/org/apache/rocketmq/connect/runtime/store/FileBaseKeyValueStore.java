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

package org.apache.rocketmq.connect.runtime.store;

import com.alibaba.fastjson.JSON;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.rocketmq.connect.runtime.common.LoggerName;
import org.apache.rocketmq.connect.runtime.serialization.Serde;
import org.apache.rocketmq.connect.runtime.utils.Base64Util;
import org.apache.rocketmq.connect.runtime.utils.FileAndPropertyUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * File based Key value store.
 *
 * @param <K>
 * @param <V>
 */
public class FileBaseKeyValueStore<K, V> extends MemoryBasedKeyValueStore<K, V> {

    private static final Logger log = LoggerFactory.getLogger(LoggerName.ROCKETMQ_RUNTIME);

    private String configFilePath;
    private Serde serdeKey;
    private Serde serdeValue;

    public FileBaseKeyValueStore(String configFilePath,
        Serde serdeKey,
        Serde serdeValue) {
        super();
        this.configFilePath = configFilePath;
        this.serdeKey = serdeKey;
        this.serdeValue = serdeValue;
    }

    public String encode() {
        Map<String, String> map = new HashMap<>();
        for (K key : data.keySet()) {
            byte[] keyByte = serdeKey.serializer().serialize("", key);
            byte[] valueByte = serdeValue.serializer().serialize("", data.get(key));
            map.put(Base64Util.base64Encode(keyByte), Base64Util.base64Encode(valueByte));
        }
        return JSON.toJSONString(map);
    }

    public void decode(String jsonString) {
        Map<K, V> resultMap = new HashMap<>();
        Map<String, String> map = JSON.parseObject(jsonString, Map.class);
        for (String key : map.keySet()) {
            K decodeKey = (K) serdeKey.deserializer().deserialize("", Base64Util.base64Decode(key));
            V decodeValue = (V) serdeValue.deserializer().deserialize("", Base64Util.base64Decode(map.get(key)));
            resultMap.put(decodeKey, decodeValue);
        }
        this.data = resultMap;
    }

    @Override
    public boolean load() {
        String fileName = null;
        try {
            fileName = this.configFilePath;
            String jsonString = FileAndPropertyUtil.file2String(fileName);

            if (null == jsonString || jsonString.length() == 0) {
                return this.loadBak();
            } else {
                this.decode(jsonString);
                log.info("load " + fileName + " OK");
                return true;
            }
        } catch (Exception e) {
            log.error("load " + fileName + " failed, and try to load backup file", e);
            return this.loadBak();
        }
    }

    private boolean loadBak() {
        String fileName = null;
        try {
            fileName = this.configFilePath;
            String jsonString = FileAndPropertyUtil.file2String(fileName + ".bak");
            if (jsonString != null && jsonString.length() > 0) {
                this.decode(jsonString);
                log.info("load " + fileName + " OK");
                return true;
            }
        } catch (Exception e) {
            log.error("load " + fileName + " Failed", e);
            return false;
        }

        return true;
    }

    @Override
    public void persist() {

        String jsonString = this.encode();
        if (jsonString != null) {
            String fileName = this.configFilePath;
            try {
                FileAndPropertyUtil.string2File(jsonString, fileName);
            } catch (IOException e) {
                log.error("persist file " + fileName + " exception", e);
            }
        }
    }
}
