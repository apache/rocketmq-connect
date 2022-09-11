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

import org.apache.rocketmq.connect.runtime.common.ConnectKeyValue;
import org.apache.rocketmq.connect.runtime.connectorwrapper.TargetState;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class JsonSerdeTest {

    private JsonSerde serde = new JsonSerde(ConnectKeyValue.class);
    private  static final Long epoch = Long.valueOf(123456789);

    /**
     * serializer
     */
    @Test
    public void objectToByteTest() {
        ConnectKeyValue connectKeyValue = new ConnectKeyValue();
        connectKeyValue.setEpoch(epoch);
        connectKeyValue.setTargetState(TargetState.STARTED);
        Map<String,String> configs =  new HashMap<>();
        configs.put("connect-name","test");
        connectKeyValue.setProperties(configs);
        byte[] serialize = serde.serializer().serialize("", connectKeyValue);
        ConnectKeyValue connectKeyValue1 = (ConnectKeyValue)serde.deserializer().deserialize("", serialize);
        Assert.assertEquals(connectKeyValue1, connectKeyValue);
    }

    /**
     * deserializer
     */
    @Test
    public void byteToObjectTest() {}

}
