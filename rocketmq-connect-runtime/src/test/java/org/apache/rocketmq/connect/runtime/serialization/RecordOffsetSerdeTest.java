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

import io.openmessaging.connector.api.data.RecordOffset;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

import org.apache.rocketmq.connect.runtime.serialization.store.RecordOffsetSerde;
import org.junit.Assert;
import org.junit.Test;

public class RecordOffsetSerdeTest {

    private RecordOffsetSerde recordOffsetSerde =new RecordOffsetSerde();

    @Test
    public void objectToByteTest() {
        Map<String, Integer> offset = new HashMap<>();
        offset.put("nextPosition", 123);
        RecordOffset recordOffset = new RecordOffset(offset);
        final byte[] actual = recordOffsetSerde.serializer().serialize("", recordOffset);
        Assert.assertEquals("{\"offset\":{\"nextPosition\":123}}", new String(actual));
    }

    @Test
    public void byteToObjectTest() {
        String str = "{\"offset\":{\"nextPosition\":123}}";
        final RecordOffset recordOffset = recordOffsetSerde.deserializer().deserialize("", str.getBytes(StandardCharsets.UTF_8));
        Assert.assertEquals(123, recordOffset.getOffset().get("nextPosition"));
    }

}
