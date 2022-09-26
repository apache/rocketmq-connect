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

package org.apache.rocketmq.connect.runtime.utils;

import java.util.List;
import org.apache.rocketmq.connect.runtime.common.ConnectKeyValue;
import org.apache.rocketmq.connect.runtime.connectorwrapper.testimpl.TestConnector;
import org.assertj.core.util.Lists;
import org.junit.Assert;
import org.junit.Test;

public class UtilsTest {

    @Test
    public void newInstanceTest() {
        Assert.assertNotNull(Utils.newInstance(TestConnector.class));
    }

    @Test
    public void joinTest() {
        List<String> list = Lists.newArrayList("Hello1", "Hello2", "Hello3");
        final String join = Utils.join(list, ",");
        Assert.assertEquals("Hello1,Hello2,Hello3", join);
    }

    @Test
    public void getClassTest() {
        ConnectKeyValue connectKeyValue = new ConnectKeyValue();
        connectKeyValue.put("testConnector", "org.apache.rocketmq.connect.runtime.connectorwrapper.testimpl.TestConnector");
        final Class<?> connector = Utils.getClass(connectKeyValue, "testConnector");
        Assert.assertEquals("TestConnector", connector.getSimpleName());
    }

}
