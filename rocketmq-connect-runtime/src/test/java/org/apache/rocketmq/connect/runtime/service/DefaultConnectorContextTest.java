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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.openmessaging.connector.api.component.connector.ConnectorContext;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.rocketmq.client.consumer.DefaultLitePullConsumer;
import org.apache.rocketmq.client.consumer.DefaultMQPullConsumer;
import org.apache.rocketmq.client.impl.MQClientAPIImpl;
import org.apache.rocketmq.client.impl.consumer.DefaultMQPullConsumerImpl;
import org.apache.rocketmq.client.impl.consumer.RebalanceImpl;
import org.apache.rocketmq.client.impl.factory.MQClientInstance;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.admin.TopicOffset;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.connect.runtime.common.ConnectKeyValue;
import org.apache.rocketmq.connect.runtime.config.WorkerConfig;
import org.apache.rocketmq.connect.runtime.connectorwrapper.NameServerMocker;
import org.apache.rocketmq.connect.runtime.connectorwrapper.ServerResponseMocker;
import org.apache.rocketmq.connect.runtime.connectorwrapper.WorkerConnector;
import org.apache.rocketmq.connect.runtime.connectorwrapper.status.ConnectorStatus;
import org.apache.rocketmq.connect.runtime.connectorwrapper.status.WrapperStatusListener;
import org.apache.rocketmq.connect.runtime.connectorwrapper.testimpl.TestConnector;
import org.apache.rocketmq.connect.runtime.controller.isolation.Plugin;
import org.apache.rocketmq.connect.runtime.controller.standalone.StandaloneConfig;
import org.apache.rocketmq.connect.runtime.controller.standalone.StandaloneConnectController;
import org.apache.rocketmq.connect.runtime.converter.record.json.JsonConverter;
import org.apache.rocketmq.connect.runtime.service.local.LocalConfigManagementServiceImpl;
import org.apache.rocketmq.connect.runtime.service.local.LocalPositionManagementServiceImpl;
import org.apache.rocketmq.connect.runtime.service.local.LocalStateManagementServiceImpl;
import org.apache.rocketmq.connect.runtime.utils.ConnectUtil;
import org.apache.rocketmq.remoting.RemotingClient;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class DefaultConnectorContextTest {
    private final MockedStatic<ConnectUtil> connectUtil = mockStatic(ConnectUtil.class);
    @Mock
    private DefaultMQProducer producer;
    @Mock
    private DefaultLitePullConsumer consumer;
    @Mock
    private DefaultMQPullConsumer defaultMQPullConsumer;

    private DefaultConnectorContext defaultConnectorContext;

    private StandaloneConnectController standaloneConnectController;

    private Plugin plugin;
    private StandaloneConfig standaloneConfig = new StandaloneConfig();
    @Mock
    private DefaultMQPullConsumerImpl defaultMQPullConsumerImpl;

    private ConfigManagementService configManagementService;

    private ClusterManagementService clusterManagementService;

    private WorkerConfig workerConfig;

    private ServerResponseMocker nameServerMocker;

    private ServerResponseMocker brokerMocker;

    private StateManagementService stateManagementService;

    private ConnectorStatus.Listener statusListene;
    private PositionManagementService positionManagementService;

    @Before
    public void before() {
//        nameServerMocker = NameServerMocker.startByDefaultConf(9876, 10911);
//        brokerMocker = ServerResponseMocker.startServer(10911, "Hello World".getBytes(StandardCharsets.UTF_8));
//        List<String> pluginPaths = new ArrayList<>();
//        pluginPaths.add("src/test/java/org/apache/rocketmq/connect/runtime");
//        plugin = new Plugin(pluginPaths);
//        workerConfig = new WorkerConfig();
//        workerConfig.setNamesrvAddr("localhost:9876");
//
//        connectUtil.when(() -> ConnectUtil.initDefaultMQProducer(workerConfig)).thenReturn(producer);
//        connectUtil.when(() -> ConnectUtil.initDefaultLitePullConsumer(workerConfig, false)).thenReturn(consumer);
//        connectUtil.when(() -> ConnectUtil.initDefaultMQPullConsumer(workerConfig)).thenReturn(defaultMQPullConsumer);
//
//        Map<String, Map<MessageQueue, TopicOffset>> returnOffsets = new HashMap<>();
//        returnOffsets.put(workerConfig.getConfigStoreTopic(), new HashMap<>(0));
//        returnOffsets.put(workerConfig.getPositionStoreTopic(), new HashMap<>(0));
//        returnOffsets.put(workerConfig.getConnectStatusTopic(), new HashMap<>(0));
//
//        connectUtil.when(() -> ConnectUtil.offsetTopics(workerConfig, Lists.newArrayList(workerConfig.getConfigStoreTopic(), workerConfig.getPositionStoreTopic(), workerConfig.getConnectStatusTopic()))).thenReturn(returnOffsets);
//
//        configManagementService = new LocalConfigManagementServiceImpl();
//        configManagementService.initialize(workerConfig, new JsonConverter(), plugin);
//
//        clusterManagementService = new ClusterManagementServiceImpl();
//        clusterManagementService.initialize(workerConfig);
//        when(this.defaultMQPullConsumer.getDefaultMQPullConsumerImpl()).thenReturn(any(DefaultMQPullConsumerImpl.class));
//
//        positionManagementService = new LocalPositionManagementServiceImpl();
//        positionManagementService.initialize(workerConfig, new JsonConverter(), new JsonConverter());
//
//        stateManagementService = new LocalStateManagementServiceImpl();
//        stateManagementService.initialize(workerConfig, new JsonConverter());
//
//        standaloneConfig.setHttpPort(8092);
//        standaloneConnectController = new StandaloneConnectController(plugin, standaloneConfig, clusterManagementService,
//            configManagementService, positionManagementService, stateManagementService);
//        Set<WorkerConnector> workerConnectors = new HashSet<>();
//        ConnectorContext connectorContext = new DefaultConnectorContext("testConnector", standaloneConnectController);
//        statusListene = new WrapperStatusListener(stateManagementService, "worker1");
//        workerConnectors.add(new WorkerConnector("testConnector", new TestConnector(), new ConnectKeyValue(), connectorContext, statusListene, this.getClass().getClassLoader()));
//        standaloneConnectController.getWorker().setWorkingConnectors(workerConnectors);
//        standaloneConnectController.start();
//        defaultConnectorContext = new DefaultConnectorContext("testConnector", standaloneConnectController);
    }

    @After
    public void after() {
//        standaloneConnectController.shutdown();
//        positionManagementService.stop();
//        configManagementService.stop();
//        brokerMocker.shutdown();
//        nameServerMocker.shutdown();
    }

    @Test
    public void requestTaskReconfigurationTest() {
        // todo fix after issue #338 is fixed
//        ConnectKeyValue connectKeyValue = new ConnectKeyValue();
//        connectKeyValue.put("connector.class", "org.apache.rocketmq.connect.runtime.connectorwrapper.testimpl.TestConnector");
//        connectKeyValue.put("connect.topicname", "testTopic");
//        connectKeyValue.setTargetState(TargetState.PAUSED);
//        configManagementService.putConnectorConfig("testConnector", connectKeyValue);
//        Assertions.assertThatCode(() -> defaultConnectorContext.requestTaskReconfiguration()).doesNotThrowAnyException();
    }
}
