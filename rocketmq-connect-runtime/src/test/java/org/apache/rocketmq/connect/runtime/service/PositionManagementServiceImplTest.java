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
import io.netty.util.internal.ConcurrentSet;
import io.openmessaging.Future;
import io.openmessaging.connector.api.data.RecordOffset;
import io.openmessaging.producer.SendResult;
import org.apache.rocketmq.client.consumer.DefaultLitePullConsumer;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.common.admin.TopicOffset;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.connect.runtime.common.ConnAndTaskConfigs;
import org.apache.rocketmq.connect.runtime.config.WorkerConfig;
import org.apache.rocketmq.connect.runtime.converter.record.json.JsonConverter;
import org.apache.rocketmq.connect.runtime.connectorwrapper.NameServerMocker;
import org.apache.rocketmq.connect.runtime.connectorwrapper.ServerResponseMocker;
import org.apache.rocketmq.connect.runtime.service.local.LocalPositionManagementServiceImpl;
import org.apache.rocketmq.connect.runtime.store.ExtendRecordPartition;
import org.apache.rocketmq.connect.runtime.store.KeyValueStore;
import org.apache.rocketmq.connect.runtime.utils.ConnectUtil;
import org.apache.rocketmq.connect.runtime.utils.TestUtils;
import org.apache.rocketmq.connect.runtime.utils.datasync.BrokerBasedLog;
import org.apache.rocketmq.connect.runtime.utils.datasync.DataSynchronizerCallback;
import org.assertj.core.util.Maps;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.junit.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;

import java.io.File;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mockStatic;

@RunWith(MockitoJUnitRunner.Silent.class)
public class PositionManagementServiceImplTest {

    private final String namespace = "namespace";
    private WorkerConfig connectConfig;
    @Mock
    private DefaultMQProducer producer;
    @Mock
    private DefaultLitePullConsumer consumer;

    private LocalPositionManagementServiceImpl positionManagementService;
    private Set<ExtendRecordPartition> needSyncPartition;
    private KeyValueStore<ExtendRecordPartition, RecordOffset> positionStore;
    private ExtendRecordPartition sourcePartition;
    private RecordOffset sourcePosition;
    private Map<ExtendRecordPartition, RecordOffset> positions;
    private ServerResponseMocker nameServerMocker;
    private ServerResponseMocker brokerMocker;

    private final MockedStatic<ConnectUtil> connectUtil = mockStatic(ConnectUtil.class);

    @Before
    public void init() throws Exception {
        nameServerMocker = NameServerMocker.startByDefaultConf(9876, 10911);
        brokerMocker = ServerResponseMocker.startServer(10911, "Hello World".getBytes(StandardCharsets.UTF_8));
        connectConfig = new WorkerConfig();

        connectConfig.setHttpPort(8081);
        connectConfig.setNamesrvAddr("localhost:9876");
        connectConfig.setStorePathRootDir(System.getProperty("user.home") + File.separator + "testConnectorStore");
        connectConfig.setRmqConsumerGroup("testConsumerGroup");
        doAnswer(new Answer() {
            @Override
            public Void answer(InvocationOnMock invocation) throws Exception {
                final Message message = invocation.getArgument(0);
                byte[] bytes = message.getBody();

                final Field dataSynchronizerField = LocalPositionManagementServiceImpl.class.getDeclaredField("dataSynchronizer");
                dataSynchronizerField.setAccessible(true);
                BrokerBasedLog<String, Map> dataSynchronizer = (BrokerBasedLog<String, Map>) dataSynchronizerField.get(positionManagementService);

                final Method decodeKeyValueMethod = BrokerBasedLog.class.getDeclaredMethod("decodeKeyValue", byte[].class);
                decodeKeyValueMethod.setAccessible(true);
                Map<String, Map> map = (Map<String, Map>) decodeKeyValueMethod.invoke(dataSynchronizer, bytes);

                final Field dataSynchronizerCallbackField = BrokerBasedLog.class.getDeclaredField("dataSynchronizerCallback");
                dataSynchronizerCallbackField.setAccessible(true);
                final DataSynchronizerCallback<String, Map> dataSynchronizerCallback = (DataSynchronizerCallback<String, Map>) dataSynchronizerCallbackField.get(dataSynchronizer);
                for (String key : map.keySet()) {
                    dataSynchronizerCallback.onCompletion(null, key, map.get(key));
                }
                return null;
            }
        }).when(producer).send(any(Message.class), any(SendCallback.class));

        positionManagementService = new LocalPositionManagementServiceImpl();
        positionManagementService.initialize(connectConfig, new JsonConverter(), new JsonConverter());

        final Field dataSynchronizerField = LocalPositionManagementServiceImpl.class.getSuperclass().getDeclaredField("dataSynchronizer");
        dataSynchronizerField.setAccessible(true);

        connectUtil.when(() -> ConnectUtil.initDefaultMQProducer(connectConfig)).thenReturn(producer);
        connectUtil.when(() -> ConnectUtil.initDefaultLitePullConsumer(connectConfig, false)).thenReturn(consumer);
        Map<String, Map<MessageQueue, TopicOffset>> returnOffsets = new HashMap<>();
        returnOffsets.put(connectConfig.getPositionStoreTopic(), new HashMap<>(0));
        connectUtil.when(() -> ConnectUtil.offsetTopics(connectConfig, Lists.newArrayList(connectConfig.getPositionStoreTopic()))).thenReturn(returnOffsets);
        positionManagementService.start();

        Field positionStoreField = LocalPositionManagementServiceImpl.class.getSuperclass().getDeclaredField("positionStore");
        positionStoreField.setAccessible(true);
        positionStore = (KeyValueStore<ExtendRecordPartition, RecordOffset>) positionStoreField.get(positionManagementService);


        Field needSyncPartitionField = LocalPositionManagementServiceImpl.class.getSuperclass().getDeclaredField("needSyncPartition");
        needSyncPartitionField.setAccessible(true);
        needSyncPartition = (ConcurrentSet<ExtendRecordPartition>) needSyncPartitionField.get(positionManagementService);
        Map<String, String> map = Maps.newHashMap("ip_port", "127.0.0.13306");
        sourcePartition = new ExtendRecordPartition(namespace, map);
        Map<String, String> map1 = Maps.newHashMap("binlog_file", "binlogFilename");
        map1.put("next_position", "100");
        sourcePosition = new RecordOffset(map1);
        positions = new HashMap<ExtendRecordPartition, RecordOffset>() {
            {
                put(sourcePartition, sourcePosition);
            }
        };
    }

    @After
    public void destroy() {
        connectUtil.close();
        positionManagementService.stop();
        TestUtils.deleteFile(new File(System.getProperty("user.home") + File.separator + "testConnectorStore"));
        brokerMocker.shutdown();
        nameServerMocker.shutdown();
    }

    @Test
    public void testGetPositionTable() {
        Map<ExtendRecordPartition, RecordOffset> positionTable = positionManagementService.getPositionTable();
        RecordOffset bytes = positionTable.get(sourcePartition);

        assertNull(bytes);

        positionManagementService.putPosition(positions);
        positionTable = positionManagementService.getPositionTable();
        bytes = positionTable.get(sourcePartition);

        assertNotNull(bytes);
    }

    @Test
    public void testPutPosition() throws Exception {
        RecordOffset bytes = positionStore.get(sourcePartition);

        assertNull(bytes);

        positionManagementService.putPosition(positions);

        bytes = positionStore.get(sourcePartition);

        assertNotNull(bytes);
    }

    @Test
    public void testRemovePosition() {
        positionManagementService.putPosition(positions);
        RecordOffset bytes = positionStore.get(sourcePartition);

        assertNotNull(bytes);

        List<ExtendRecordPartition> sourcePartitions = new ArrayList<ExtendRecordPartition>(8) {
            {
                add(sourcePartition);
            }
        };

        positionManagementService.removePosition(sourcePartitions);

        bytes = positionStore.get(sourcePartition);

        assertNull(bytes);
    }

    @Test
    public void testNeedSyncPartition() {
        positionManagementService.putPosition(positions);

        assertTrue(needSyncPartition.contains(sourcePartition));

        List<ExtendRecordPartition> sourcePartitions = new ArrayList<ExtendRecordPartition>(8) {
            {
                add(sourcePartition);
            }
        };

        positionManagementService.putPosition(sourcePartition, sourcePosition);

        assertTrue(needSyncPartition.contains(sourcePartition));

        positionManagementService.removePosition(sourcePartitions);
        assertFalse(positionStore.containsKey(sourcePartition));
    }

    @Test
    public void testSendNeedSynchronizePosition() throws Exception {
        positionManagementService.putPosition(positions);

        Map<String, String> map = Maps.newHashMap("ip_port", "127.0.0.2:3306");
        ExtendRecordPartition sourcePartitionTmp = new ExtendRecordPartition(namespace, map);
        Map<String, String> map1 = Maps.newHashMap("binlog_file", "binlogFilename");
        map1.put("next_position", "100");
        RecordOffset sourcePositionTmp = new RecordOffset(map1);
        positionStore.put(sourcePartitionTmp, sourcePositionTmp);

        Set<ExtendRecordPartition> needSyncPartitionTmp = needSyncPartition;
        needSyncPartition = new ConcurrentSet<>();
        Map<ExtendRecordPartition, RecordOffset> needSyncPosition = positionStore.getKVMap().entrySet().stream()
                .filter(entry -> needSyncPartitionTmp.contains(entry.getKey()))
                .collect(Collectors.toMap(entry -> entry.getKey(), entry -> entry.getValue()));

        assertTrue(needSyncPartition.size() == 0);

        RecordOffset bytes = needSyncPosition.get(sourcePartition);
        assertNotNull(bytes);

        RecordOffset tmpBytes = needSyncPosition.get(sourcePartitionTmp);
        assertNull(tmpBytes);

        List<ExtendRecordPartition> sourcePartitions = new ArrayList<ExtendRecordPartition>(8) {
            {
                add(sourcePartition);
                add(sourcePartitionTmp);
            }
        };

        needSyncPartition = needSyncPartitionTmp;
        needSyncPartition.addAll(sourcePartitions);
        positionManagementService.removePosition(sourcePartitions);
        assertTrue(positionStore.size() == 0);
    }

}