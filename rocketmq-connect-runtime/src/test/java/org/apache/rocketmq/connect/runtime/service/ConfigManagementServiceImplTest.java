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
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.rocketmq.connect.runtime.service;

import com.google.common.collect.Lists;
import java.io.File;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import com.google.common.collect.Maps;
import org.apache.rocketmq.client.consumer.DefaultLitePullConsumer;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.common.admin.TopicOffset;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.connect.runtime.common.ConnAndTaskConfigs;
import org.apache.rocketmq.connect.runtime.common.ConnectKeyValue;
import org.apache.rocketmq.connect.runtime.config.ConnectorConfig;
import org.apache.rocketmq.connect.runtime.config.WorkerConfig;
import org.apache.rocketmq.connect.runtime.connectorwrapper.NameServerMocker;
import org.apache.rocketmq.connect.runtime.connectorwrapper.ServerResponseMocker;
import org.apache.rocketmq.connect.runtime.converter.record.json.JsonConverter;
import org.apache.rocketmq.connect.runtime.controller.isolation.DelegatingClassLoader;
import org.apache.rocketmq.connect.runtime.service.local.LocalConfigManagementServiceImpl;
import org.apache.rocketmq.connect.runtime.store.KeyValueStore;
import org.apache.rocketmq.connect.runtime.controller.isolation.Plugin;
import org.apache.rocketmq.connect.runtime.utils.ConnectUtil;
import org.apache.rocketmq.connect.runtime.utils.TestUtils;
import org.apache.rocketmq.connect.runtime.utils.datasync.BrokerBasedLog;
import org.apache.rocketmq.connect.runtime.utils.datasync.DataSynchronizer;
import org.apache.rocketmq.connect.runtime.utils.datasync.DataSynchronizerCallback;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.junit.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mockStatic;

@RunWith(MockitoJUnitRunner.Silent.class)
public class ConfigManagementServiceImplTest {

    private KeyValueStore<String, ConnectKeyValue> connectorKeyValueStore;
    private KeyValueStore<String, List<ConnectKeyValue>> taskKeyValueStore;
    private Set<ConfigManagementService.ConnectorConfigUpdateListener> connectorConfigUpdateListener;
    private DataSynchronizer<String, ConnAndTaskConfigs> dataSynchronizer;
    private WorkerConfig connectConfig;

    @Mock
    private DefaultMQProducer producer;

    @Mock
    private DefaultLitePullConsumer consumer;

    private LocalConfigManagementServiceImpl configManagementService;

    private String connectorName;

    private ConnectKeyValue connectKeyValue;

    private Plugin plugin;

    private ServerResponseMocker nameServerMocker;

    private ServerResponseMocker brokerMocker;

    private final MockedStatic<ConnectUtil> connectUtil = mockStatic(ConnectUtil.class);

    @Before
    public void init() throws Exception {
        nameServerMocker = NameServerMocker.startByDefaultConf(9876, 10911);
        brokerMocker = ServerResponseMocker.startServer(10911, "Hello World".getBytes(StandardCharsets.UTF_8));

        String consumerGroup = UUID.randomUUID().toString();
        String producerGroup = UUID.randomUUID().toString();

        connectConfig = new WorkerConfig();
        connectConfig.setHttpPort(8081);
        connectConfig.setStorePathRootDir(System.getProperty("user.home") + File.separator + "testConnectorStore");
        connectConfig.setRmqConsumerGroup("testConsumerGroup");
        connectorName = "testConnector";

        connectConfig.setRmqConsumerGroup(consumerGroup);
        connectConfig.setRmqProducerGroup(producerGroup);
        connectConfig.setNamesrvAddr("127.0.0.1:9876");
        connectConfig.setRmqMinConsumeThreadNums(1);
        connectConfig.setRmqMaxConsumeThreadNums(32);
        connectConfig.setRmqMessageConsumeTimeout(3 * 1000);

        connectKeyValue = new ConnectKeyValue();
        connectKeyValue.put(ConnectorConfig.CONNECTOR_CLASS, "org.apache.rocketmq.connect.runtime.connectorwrapper.testimpl.TestConnector");
        connectKeyValue.put(ConnectorConfig.VALUE_CONVERTER, "source-record-converter");

        doAnswer(new Answer() {
            @Override
            public Void answer(InvocationOnMock invocation) throws Exception {
                final Message message = invocation.getArgument(0);
                byte[] bytes = message.getBody();

                final Field dataSynchronizerField = LocalConfigManagementServiceImpl.class.getDeclaredField("dataSynchronizer");
                dataSynchronizerField.setAccessible(true);
                BrokerBasedLog<String, ConnAndTaskConfigs> dataSynchronizer = (BrokerBasedLog<String, ConnAndTaskConfigs>) dataSynchronizerField.get(configManagementService);

                final Method decodeKeyValueMethod = BrokerBasedLog.class.getDeclaredMethod("decodeKeyValue", byte[].class);
                decodeKeyValueMethod.setAccessible(true);
                Map<String, ConnAndTaskConfigs> map = (Map<String, ConnAndTaskConfigs>) decodeKeyValueMethod.invoke(dataSynchronizer, bytes);

                final Field dataSynchronizerCallbackField = BrokerBasedLog.class.getDeclaredField("dataSynchronizerCallback");
                dataSynchronizerCallbackField.setAccessible(true);
                final DataSynchronizerCallback<String, ConnAndTaskConfigs> dataSynchronizerCallback = (DataSynchronizerCallback<String, ConnAndTaskConfigs>) dataSynchronizerCallbackField.get(dataSynchronizer);
                for (String key : map.keySet()) {
                    dataSynchronizerCallback.onCompletion(null, key, map.get(key));
                }
                return null;
            }
        }).when(producer).send(any(Message.class), any(SendCallback.class));

        configManagementService = new LocalConfigManagementServiceImpl();
        configManagementService.initialize(connectConfig, new JsonConverter(), plugin);
        final Field connectorKeyValueStoreField = LocalConfigManagementServiceImpl.class.getSuperclass().getDeclaredField("connectorKeyValueStore");
        connectorKeyValueStoreField.setAccessible(true);
        connectorKeyValueStore = (KeyValueStore<String, ConnectKeyValue>) connectorKeyValueStoreField.get(configManagementService);
        final Field taskKeyValueStoreField = LocalConfigManagementServiceImpl.class.getSuperclass().getDeclaredField("taskKeyValueStore");
        taskKeyValueStoreField.setAccessible(true);
        taskKeyValueStore = (KeyValueStore<String, List<ConnectKeyValue>>) taskKeyValueStoreField.get(configManagementService);
        List<String> pluginPaths = new ArrayList<>();
        pluginPaths.add("src/test/java/org/apache/rocketmq/connect/runtime");
        plugin = new Plugin(pluginPaths);

        connectUtil.when(() -> ConnectUtil.initDefaultMQProducer(connectConfig)).thenReturn(producer);
        connectUtil.when(() -> ConnectUtil.initDefaultLitePullConsumer(connectConfig, false)).thenReturn(consumer);
        Map<String, Map<MessageQueue, TopicOffset>> returnOffsets = new HashMap<>();
        returnOffsets.put(connectConfig.getConfigStoreTopic(), new HashMap<>(0));
        connectUtil.when(() -> ConnectUtil.offsetTopics(connectConfig, Lists.newArrayList(connectConfig.getConfigStoreTopic()))).thenReturn(returnOffsets);

        configManagementService.initialize(connectConfig, new JsonConverter(), plugin);
        configManagementService.start();

        final Field connectorKeyValueStoreField2 = LocalConfigManagementServiceImpl.class.getSuperclass().getDeclaredField("connectorKeyValueStore");
        connectorKeyValueStoreField2.setAccessible(true);
        connectorKeyValueStore = (KeyValueStore<String, ConnectKeyValue>) connectorKeyValueStoreField2.get(configManagementService);
        final Field taskKeyValueStoreField2 = LocalConfigManagementServiceImpl.class.getSuperclass().getDeclaredField("taskKeyValueStore");
        taskKeyValueStoreField2.setAccessible(true);
        taskKeyValueStore = (KeyValueStore<String, List<ConnectKeyValue>>) taskKeyValueStoreField2.get(configManagementService);

        final Field dataSynchronizerField = LocalConfigManagementServiceImpl.class.getSuperclass().getDeclaredField("dataSynchronizer");
        dataSynchronizerField.setAccessible(true);

        final Field producerField = BrokerBasedLog.class.getDeclaredField("producer");
        producerField.setAccessible(true);
        producerField.set((BrokerBasedLog<String, ConnAndTaskConfigs>) dataSynchronizerField.get(configManagementService), producer);

        final Field consumerField = BrokerBasedLog.class.getDeclaredField("consumer");
        consumerField.setAccessible(true);
        consumerField.set((BrokerBasedLog<String, ConnAndTaskConfigs>) dataSynchronizerField.get(configManagementService), consumer);

    }

    @After
    public void destroy() {
        connectUtil.close();
        configManagementService.stop();
        TestUtils.deleteFile(new File(System.getProperty("user.home") + File.separator + "testConnectorStore"));
        brokerMocker.shutdown();
        nameServerMocker.shutdown();
    }

    @Test
    public void testPutConnectorConfig() {
        final String result = configManagementService.putConnectorConfig(connectorName, connectKeyValue);
        Assert.assertEquals("testConnector", result);

    }

    @Test
    public void testGetConnectorConfigs() {
        Map<String, ConnectKeyValue> connectorConfigs = configManagementService.getConnectorConfigs();
        ConnectKeyValue connectKeyValue = connectorConfigs.get(connectorName);

        assertNull(connectKeyValue);

        final String result = configManagementService.putConnectorConfig(connectorName, this.connectKeyValue);

        Assert.assertEquals(connectorName, result);
    }

    @Test
    public void testGetTaskConfigs() {

        Map<String, List<ConnectKeyValue>> taskConfigs = configManagementService.getTaskConfigs();
        List<ConnectKeyValue> connectKeyValues = taskConfigs.get(connectorName);

        assertNull(connectKeyValues);

        configManagementService.putTaskConfigs(connectorName, Lists.newArrayList(this.connectKeyValue));

        taskConfigs = configManagementService.getTaskConfigs();
        connectKeyValues = taskConfigs.get(connectorName);

        assertNotNull(connectKeyValues);
    }

}

