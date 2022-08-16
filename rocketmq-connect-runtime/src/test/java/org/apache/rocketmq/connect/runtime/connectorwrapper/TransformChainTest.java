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

package org.apache.rocketmq.connect.runtime.connectorwrapper;

import io.openmessaging.KeyValue;
import io.openmessaging.connector.api.data.ConnectRecord;
import io.openmessaging.internal.DefaultKeyValue;
import org.apache.rocketmq.connect.runtime.config.ConnectorConfig;
import org.apache.rocketmq.connect.runtime.controller.isolation.Plugin;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class TransformChainTest {

    private TransformChain transformChain;

    private KeyValue keyValue = new DefaultKeyValue();

    @Mock
    private Plugin plugin;

    @Mock
    private ConnectRecord connectRecord;

    @Before
    public void before() {
        keyValue.put(ConnectorConfig.TRANSFORMS, "testTransform");
        keyValue.put("transforms-testTransform-class", "org.apache.rocketmq.connect.runtime.connectorwrapper.TestTransform");
        connectRecord.setExtensions(keyValue);
        transformChain = new TransformChain(keyValue, plugin);
    }

    @After
    public void after() throws Exception {
        transformChain.close();
    }

    @Test
    public void doTransformsTest() {
        final ConnectRecord record = transformChain.doTransforms(connectRecord);
        assert record != null;
    }
}
