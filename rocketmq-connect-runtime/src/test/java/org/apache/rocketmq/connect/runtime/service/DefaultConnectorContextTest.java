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

package org.apache.rocketmq.connect.runtime.service;

import io.openmessaging.connector.api.component.connector.ConnectorContext;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.rocketmq.connect.runtime.common.ConnectKeyValue;
import org.apache.rocketmq.connect.runtime.config.WorkerConfig;
import org.apache.rocketmq.connect.runtime.connectorwrapper.NameServerMocker;
import org.apache.rocketmq.connect.runtime.connectorwrapper.ServerResponseMocker;
import org.apache.rocketmq.connect.runtime.connectorwrapper.TargetState;
import org.apache.rocketmq.connect.runtime.connectorwrapper.WorkerConnector;
import org.apache.rocketmq.connect.runtime.connectorwrapper.status.ConnectorStatus;
import org.apache.rocketmq.connect.runtime.connectorwrapper.status.WrapperStatusListener;
import org.apache.rocketmq.connect.runtime.connectorwrapper.testimpl.TestConnector;
import org.apache.rocketmq.connect.runtime.controller.isolation.Plugin;
import org.apache.rocketmq.connect.runtime.controller.standalone.StandaloneConfig;
import org.apache.rocketmq.connect.runtime.controller.standalone.StandaloneConnectController;
import org.apache.rocketmq.connect.runtime.converter.record.json.JsonConverter;
import org.assertj.core.api.Assertions;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class DefaultConnectorContextTest {

    private DefaultConnectorContext defaultConnectorContext;

    private StandaloneConnectController standaloneConnectController;

    private Plugin plugin;
    private StandaloneConfig standaloneConfig = new StandaloneConfig();

    private ClusterManagementService clusterManagementService = new ClusterManagementServiceImpl();

    private ConfigManagementService configManagementService;

    private PositionManagementService positionManagementService = new PositionManagementServiceImpl();

    private WorkerConfig workerConfig;

    private ServerResponseMocker nameServerMocker;

    private ServerResponseMocker brokerMocker;

    private StateManagementService stateManagementService;

    private ConnectorStatus.Listener statusListene;

    @Before
    public void before() {
        nameServerMocker = NameServerMocker.startByDefaultConf(9876, 10911);
        brokerMocker = ServerResponseMocker.startServer(10911, "Hello World".getBytes(StandardCharsets.UTF_8));
        List<String> pluginPaths = new ArrayList<>();
        pluginPaths.add("src/test/java/org/apache/rocketmq/connect/runtime");
        plugin = new Plugin(pluginPaths);
        workerConfig = new WorkerConfig();
        workerConfig.setNamesrvAddr("localhost:9876");

        configManagementService = new ConfigManagementServiceImpl();
        configManagementService.initialize(workerConfig, new JsonConverter(), plugin);
        configManagementService.start();

        clusterManagementService.initialize(workerConfig);
        positionManagementService.initialize(workerConfig,new JsonConverter(),new JsonConverter());
        positionManagementService.start();
        stateManagementService = new StateManagementServiceImpl();
        stateManagementService.initialize(workerConfig,new JsonConverter());

        standaloneConfig.setHttpPort(8092);
        standaloneConnectController = new StandaloneConnectController(plugin, standaloneConfig, clusterManagementService,
            configManagementService, positionManagementService, stateManagementService);
        Set<WorkerConnector> workerConnectors = new HashSet<>();
        ConnectorContext connectorContext = new DefaultConnectorContext("testConnector", standaloneConnectController);
        statusListene = new WrapperStatusListener(stateManagementService, "worker1");
        workerConnectors.add(new WorkerConnector("testConnector", new TestConnector(), new ConnectKeyValue(), connectorContext, statusListene, this.getClass().getClassLoader()));
        standaloneConnectController.getWorker().setWorkingConnectors(workerConnectors);
        standaloneConnectController.start();
        defaultConnectorContext = new DefaultConnectorContext("testConnector", standaloneConnectController);
    }

    @After
    public void after() {
        standaloneConnectController.shutdown();
        positionManagementService.stop();
        configManagementService.stop();
        brokerMocker.shutdown();
        nameServerMocker.shutdown();
    }

    @Test
    public void requestTaskReconfigurationTest() {
        // todo fix after issue #338 is fixed
//        ConnectKeyValue connectKeyValue = new ConnectKeyValue();
//        connectKeyValue.put("connector.class", "org.apache.rocketmq.connect.runtime.connectorwrapper.testimpl.TestConnector");
//        connectKeyValue.put("connect.topicname", "testTopic");
//        connectKeyValue.setTargetState(TargetState.PAUSED);
//        configManagementService.putConnectorConfig("testConnector", connectKeyValue);
//
//        Assertions.assertThatCode(() -> defaultConnectorContext.requestTaskReconfiguration()).doesNotThrowAnyException();
    }
}
