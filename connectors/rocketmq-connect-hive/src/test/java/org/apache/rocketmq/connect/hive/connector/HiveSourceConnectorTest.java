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

package org.apache.rocketmq.connect.hive.connector;

import io.openmessaging.KeyValue;
import io.openmessaging.internal.DefaultKeyValue;
import java.lang.reflect.Field;
import java.util.List;
import org.apache.rocketmq.connect.hive.config.HiveConstant;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class HiveSourceConnectorTest {

    private HiveSourceConnector connector;

    private KeyValue keyValue;

    @Before
    public void before() {
        connector = new HiveSourceConnector();
        keyValue = new DefaultKeyValue();
        keyValue.put(HiveConstant.HOST, "127.0.0.1");
        keyValue.put(HiveConstant.PORT, 10000);
        keyValue.put(HiveConstant.TABLES, "{\n" +
            "\"invites\": {\n" +
            "\"bar\":1\n" +
            "}\n" +
            "}");
        connector.start(keyValue);
    }

    @After
    public void after() {
        connector.stop();
    }

    @Test
    public void taskConfigsTest() throws NoSuchFieldException {
        final Field keyValueField = connector.getClass().getDeclaredField("keyValue");
        keyValueField.setAccessible(true);

        final List<KeyValue> values = connector.taskConfigs(2);
        Assert.assertEquals("127.0.0.1", values.get(0).getString(HiveConstant.HOST));
        Assert.assertEquals(10000, values.get(0).getInt(HiveConstant.PORT));
    }

}
