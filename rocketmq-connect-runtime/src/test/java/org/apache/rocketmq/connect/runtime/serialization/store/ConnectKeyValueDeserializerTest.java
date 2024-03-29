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
import org.apache.rocketmq.connect.runtime.common.ConnectKeyValue;
import org.junit.Assert;
import org.junit.Test;

public class ConnectKeyValueDeserializerTest {

    @Test
    public void deserializeTest() {
        ConnectKeyValueDeserializer deserializer = new ConnectKeyValueDeserializer();
        ConnectKeyValue connectKeyValue = new ConnectKeyValue();
        connectKeyValue.put("connector.topic", "testTopic");
        connectKeyValue.put("max.tasks", 2);
        final ConnectKeyValue result = deserializer.deserialize("testTopic", JSON.toJSONBytes(connectKeyValue));
        Assert.assertEquals(2, result.getInt("max.tasks"));
        Assert.assertEquals("testTopic", result.getString("connector.topic"));
    }
}
