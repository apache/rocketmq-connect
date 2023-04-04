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

package org.apache.rocketmq.connect.runtime.controller.distributed;

import io.openmessaging.connector.api.data.RecordConverter;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.StandardCharsets;

import org.apache.rocketmq.connect.runtime.config.WorkerConfig;
import org.apache.rocketmq.connect.runtime.connectorwrapper.NameServerMocker;
import org.apache.rocketmq.connect.runtime.connectorwrapper.ServerResponseMocker;
import org.apache.rocketmq.connect.runtime.connectorwrapper.testimpl.TestPositionManageServiceImpl;
import org.apache.rocketmq.connect.runtime.controller.isolation.Plugin;

import org.apache.rocketmq.connect.runtime.controller.isolation.PluginClassLoader;
import org.apache.rocketmq.connect.runtime.converter.record.json.JsonConverter;
import org.apache.rocketmq.connect.runtime.service.ClusterManagementService;
import org.apache.rocketmq.connect.runtime.service.ClusterManagementServiceImpl;
import org.apache.rocketmq.connect.runtime.service.ConfigManagementService;
import org.apache.rocketmq.connect.runtime.service.PositionManagementService;
import org.apache.rocketmq.connect.runtime.service.StateManagementService;
import org.apache.rocketmq.connect.runtime.service.local.LocalStateManagementServiceImpl;
import org.assertj.core.api.Assertions;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class DistributedConnectControllerTest {

    private DistributedConnectController distributedConnectController;

    @Mock
    private Plugin plugin;

    private DistributedConfig distributedConfig = new DistributedConfig();

    private ClusterManagementService clusterManagementService = new ClusterManagementServiceImpl();

    private ConfigManagementService configManagementService = new TestConfigManagementService();

    private PositionManagementService positionManagementService = new TestPositionManageServiceImpl();

    private final StateManagementService stateManagementService = new TestStateManagementService();

    private WorkerConfig workerConfig = new WorkerConfig();

    private ServerResponseMocker nameServerMocker;

    private ServerResponseMocker brokerMocker;

    private RecordConverter recordConverter;

    private PluginClassLoader pluginClassLoader;

    @Before
    public void before() throws InterruptedException, MalformedURLException {
        nameServerMocker = NameServerMocker.startByDefaultConf(9876, 10911);
        brokerMocker = ServerResponseMocker.startServer(10911, "Hello World".getBytes(StandardCharsets.UTF_8));
        workerConfig.setNamesrvAddr("127.0.0.1:9876");
        recordConverter = new JsonConverter();
        clusterManagementService.initialize(workerConfig);
        stateManagementService.initialize(workerConfig, recordConverter);
        URL url = new URL("file://src/test/java/org/apache/rocketmq/connect/runtime");
        URL[] urls = new URL[]{};
        pluginClassLoader = new PluginClassLoader(url, urls);
        Thread.currentThread().setContextClassLoader(pluginClassLoader);

        distributedConnectController = new DistributedConnectController(
                plugin,
                distributedConfig,
                clusterManagementService,
                configManagementService,
                positionManagementService,
                stateManagementService );
    }

    @After
    public void after() {
        brokerMocker.shutdown();
        nameServerMocker.shutdown();
    }

    @Test
    public void startTest() {
        Assertions.assertThatCode(() -> distributedConnectController.start()).doesNotThrowAnyException();
    }

}
