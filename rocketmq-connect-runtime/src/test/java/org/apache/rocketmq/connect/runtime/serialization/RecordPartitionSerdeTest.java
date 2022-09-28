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

package org.apache.rocketmq.connect.runtime.serialization;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

import org.apache.rocketmq.connect.runtime.serialization.store.RecordPartitionSerde;
import org.apache.rocketmq.connect.runtime.store.ExtendRecordPartition;
import org.junit.Assert;
import org.junit.Test;

public class RecordPartitionSerdeTest {
    private RecordPartitionSerde recordPartitionSerde = new RecordPartitionSerde();

    @Test
    public void objectToByteTest() {
        Map<String, String> partition = new HashMap<>();
        partition.put("ip_port", "127.0.0.1:3306");
        ExtendRecordPartition extendRecordPartition = new ExtendRecordPartition("default_namespace", partition);
        final byte[] actual = recordPartitionSerde.serializer().serialize("", extendRecordPartition);
        Assert.assertEquals("{\"namespace\":\"default_namespace\",\"partition\":{\"ip_port\":\"127.0.0.1:3306\"}}", new String(actual));
    }

    @Test
    public void byteToObjectTest() {
        String str = "{\"namespace\":\"default_namespace\",\"partition\":{\"ip_port\":\"127.0.0.1:3306\"}}";
        final ExtendRecordPartition extendRecordPartition = recordPartitionSerde.deserializer().deserialize("", str.getBytes(StandardCharsets.UTF_8));
        Assert.assertEquals("default_namespace", extendRecordPartition.getNamespace());
        Assert.assertEquals("127.0.0.1:3306", extendRecordPartition.getPartition().get("ip_port"));
    }
}
