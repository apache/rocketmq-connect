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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.rocketmq.connect.runtime.converter;

import io.openmessaging.connector.api.data.RecordOffset;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import org.apache.rocketmq.connect.runtime.store.ExtendRecordPartition;
import org.junit.Assert;
import org.junit.Test;

public class RecordPositionMapConverterTest {

    private RecordPositionMapConverter recordPositionMapConverter = new RecordPositionMapConverter();

    @Test
    public void objectToByteTest() {
        Map<ExtendRecordPartition, RecordOffset> map = new HashMap<>();
        Map<String, Integer> offset  =new HashMap<>();
        offset.put("nextPosition", 123);
        RecordOffset recordOffset = new RecordOffset(offset);
        Map<String, String> partition = new HashMap<>();
        partition.put("ip_port", "127.0.0.1:3306");
        ExtendRecordPartition extendRecordPartition = new ExtendRecordPartition("default_namespace", partition);
        map.put(extendRecordPartition, recordOffset);
        final byte[] bytes = recordPositionMapConverter.objectToByte(map);
        Assert.assertEquals("{\"{\\\"namespace\\\":\\\"default_namespace\\\",\\\"partition\\\":{\\\"ip_port\\\":\\\"127.0.0.1:3306\\\"}}\":\"{\\\"offset\\\":{\\\"nextPosition\\\":123}}\"}", new String(bytes));
    }

    @Test
    public void byteToObjectTest() {
        String str = "{\"{\\\"namespace\\\":\\\"default_namespace\\\",\\\"partition\\\":{\\\"ip_port\\\":\\\"127.0.0.1:3306\\\"}}\":\"{\\\"offset\\\":{\\\"nextPosition\\\":123}}\"}";
        final Map<ExtendRecordPartition, RecordOffset> map = recordPositionMapConverter.byteToObject(str.getBytes(StandardCharsets.UTF_8));
        Map<String, String> partition = new HashMap<>();
        partition.put("ip_port", "127.0.0.1:3306");
        ExtendRecordPartition extendRecordPartition = new ExtendRecordPartition("default_namespace", partition);
        Assert.assertTrue(map.containsKey(extendRecordPartition));
    }
}
