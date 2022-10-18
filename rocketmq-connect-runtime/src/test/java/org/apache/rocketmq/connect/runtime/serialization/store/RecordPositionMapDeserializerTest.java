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

package org.apache.rocketmq.connect.runtime.serialization.store;

import com.alibaba.fastjson.JSON;
import io.openmessaging.connector.api.data.RecordOffset;
import java.util.HashMap;
import java.util.Map;
import org.apache.rocketmq.connect.runtime.store.ExtendRecordPartition;
import org.junit.Assert;
import org.junit.Test;

public class RecordPositionMapDeserializerTest {

    @Test
    public void deserializeTest() {
        Map<String, String> partitionMap = new HashMap<>();
        partitionMap.put("defaultPartition", "defaultPartition");
        ExtendRecordPartition partition = new ExtendRecordPartition("testNamespace", partitionMap);

        Map<String, Integer> offsetMap = new HashMap<>();
        offsetMap.put("testOffset", 1);
        RecordOffset recordOffset = new RecordOffset(offsetMap);
        Map<String, String> map = new HashMap<>();
        map.put(JSON.toJSONString(partition), JSON.toJSONString(recordOffset));

        RecordPositionMapDeserializer deserializer = new RecordPositionMapDeserializer();
        final Map<ExtendRecordPartition, RecordOffset> result = deserializer.deserialize("testTopic", JSON.toJSONBytes(map));
        for (Map.Entry<ExtendRecordPartition, RecordOffset> entry : result.entrySet()) {
            final ExtendRecordPartition key = entry.getKey();
            final RecordOffset value = entry.getValue();
            Assert.assertEquals(partition.toString(), key.toString());
            Assert.assertEquals(recordOffset.toString(), value.toString());
        }
    }
}
