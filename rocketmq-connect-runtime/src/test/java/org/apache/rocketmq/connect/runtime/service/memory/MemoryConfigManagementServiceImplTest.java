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

package org.apache.rocketmq.connect.runtime.service.memory;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.rocketmq.connect.runtime.common.ConnectKeyValue;
import org.apache.rocketmq.connect.runtime.config.WorkerConfig;
import org.apache.rocketmq.connect.runtime.controller.isolation.DelegatingClassLoader;
import org.apache.rocketmq.connect.runtime.controller.isolation.Plugin;
import org.apache.rocketmq.connect.runtime.controller.isolation.PluginClassLoader;
import org.apache.rocketmq.connect.runtime.converter.record.json.JsonConverter;
import org.apache.rocketmq.connect.runtime.service.ConfigManagementService;
import org.assertj.core.api.Assertions;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class MemoryConfigManagementServiceImplTest {

    private ConfigManagementService memoryConfigManagementService = new MemoryConfigManagementServiceImpl();

    private WorkerConfig workerConfig = new WorkerConfig();

    private Plugin plugin;

    @Mock
    private DelegatingClassLoader delegatingClassLoader;

    private PluginClassLoader pluginClassLoader;

    @Before
    public void before() throws ClassNotFoundException, MalformedURLException {
        List<String> pluginPaths = new ArrayList<>();
        pluginPaths.add("src/test/java/org/apache/rocketmq/connect/runtime");
        plugin = new Plugin(pluginPaths);

        URL url = new URL("file://src/test/java/org/apache/rocketmq/connect/runtime");
        URL[] urls = new URL[]{};
        pluginClassLoader = new PluginClassLoader(url, urls);
        memoryConfigManagementService.initialize(workerConfig, new JsonConverter(), plugin);
        memoryConfigManagementService.start();
    }

    @After
    public void after() {
        memoryConfigManagementService.stop();
    }

    @Test
    public void putConnectorConfigTest() {
        ConnectKeyValue connectKeyValue = new ConnectKeyValue();
        connectKeyValue.put("connect.topicname", "testTopic");
        connectKeyValue.put("connector.class", "org.apache.rocketmq.connect.runtime.connectorwrapper.testimpl.TestConnector");
        final String result = memoryConfigManagementService.putConnectorConfig("testConnector", connectKeyValue);
        Assertions.assertThat("".equals(result));
    }

    @Test
    public void getConnectorConfigsTest() {
        ConnectKeyValue connectKeyValue = new ConnectKeyValue();
        connectKeyValue.put("connect.topicname", "testTopic");
        connectKeyValue.put("connector.class", "org.apache.rocketmq.connect.runtime.connectorwrapper.testimpl.TestConnector");
        memoryConfigManagementService.putConnectorConfig("testConnector", connectKeyValue);

        final Map<String, ConnectKeyValue> configs = memoryConfigManagementService.getConnectorConfigs();
        final ConnectKeyValue resultKeyValue = configs.get("testConnector");
        Assert.assertEquals("testTopic", resultKeyValue.getString("connect.topicname"));
        Assert.assertEquals("org.apache.rocketmq.connect.runtime.connectorwrapper.testimpl.TestConnector", resultKeyValue.getString("connector.class"));
    }

    @Test
    public void getConnectorConfigsIncludeDeletedTest() {
        ConnectKeyValue connectKeyValue = new ConnectKeyValue();
        connectKeyValue.put("connect.topicname", "testTopic");
        connectKeyValue.put("connector.class", "org.apache.rocketmq.connect.runtime.connectorwrapper.testimpl.TestConnector");
        connectKeyValue.put("config-deleted", 1);
        memoryConfigManagementService.putConnectorConfig("testConnector", connectKeyValue);

        final Map<String, ConnectKeyValue> allData = memoryConfigManagementService.getConnectorConfigs();
        final ConnectKeyValue resultKeyValue = allData.get("testConnector");
        Assert.assertEquals("testTopic", resultKeyValue.getString("connect.topicname"));
        Assert.assertEquals("org.apache.rocketmq.connect.runtime.connectorwrapper.testimpl.TestConnector", resultKeyValue.getString("connector.class"));
    }

    @Test
    public void removeConnectorConfigTest() {
        ConnectKeyValue connectKeyValue = new ConnectKeyValue();
        connectKeyValue.put("connect.topicname", "testTopic");
        connectKeyValue.put("connector.class", "org.apache.rocketmq.connect.runtime.connectorwrapper.testimpl.TestConnector");
        memoryConfigManagementService.putConnectorConfig("testConnector", connectKeyValue);
        Assertions.assertThatCode(() -> memoryConfigManagementService.deleteConnectorConfig("testConnector")).doesNotThrowAnyException();
    }

}
