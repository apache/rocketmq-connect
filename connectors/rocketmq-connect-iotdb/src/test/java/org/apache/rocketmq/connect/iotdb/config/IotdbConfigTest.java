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

package org.apache.rocketmq.connect.iotdb.config;

import io.openmessaging.KeyValue;
import io.openmessaging.internal.DefaultKeyValue;
import org.junit.Assert;
import org.junit.Test;

public class IotdbConfigTest {

    @Test
    public void loadTest() {
        KeyValue keyValue = new DefaultKeyValue();
        keyValue.put(IotdbConstant.IOTDB_HOST, "localhost");
        keyValue.put(IotdbConstant.IOTDB_PORT, 6667);
        keyValue.put(IotdbConstant.IOTDB_PATHS, "root.ln.wf01.wt01,root.ln.wf01.wt02");
        IotdbConfig config = new IotdbConfig();
        config.load(keyValue);
        Assert.assertEquals("localhost", config.getIotdbHost());
        Assert.assertEquals(6667 + "", config.getIotdbPort().toString());
        Assert.assertEquals("root.ln.wf01.wt01,root.ln.wf01.wt02", config.getIotdbPaths());
    }
}
