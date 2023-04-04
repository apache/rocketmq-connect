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

import io.openmessaging.connector.api.data.RecordOffset;
import io.openmessaging.connector.api.data.RecordPartition;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.rocketmq.client.consumer.DefaultMQPullConsumer;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.connect.runtime.common.ConnectKeyValue;
import org.apache.rocketmq.connect.runtime.config.WorkerConfig;
import org.apache.rocketmq.connect.runtime.connectorwrapper.NameServerMocker;
import org.apache.rocketmq.connect.runtime.connectorwrapper.ServerResponseMocker;
import org.apache.rocketmq.connect.runtime.service.strategy.AllocateConnAndTaskStrategy;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;
import org.assertj.core.api.Assertions;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class ConnectUtilTest {

    private ServerResponseMocker nameServerMocker;

    private ServerResponseMocker brokerMocker;

    private static final String TEST_GROUP = "testGroupName";

    private static final String NAME_SERVER_ADDR = "localhost:9876";

    private static final String TEST_TOPIC = "TEST_TOPIC";

    private static final String TEST_SUBSCRIPTION_GROUP = "TEST_SUBSCRIPTION_GROUP";

    @Before
    public void before() {
        nameServerMocker = NameServerMocker.startByDefaultConf(9876, 10911);
        brokerMocker = ServerResponseMocker.startServer(10911, "Hello World".getBytes());
    }

    @After
    public void after() {
        nameServerMocker.shutdown();
        brokerMocker.shutdown();
    }

    @Test
    public void createGroupNameTest() {
        final String groupName1 = ConnectUtil.createGroupName(TEST_GROUP);
        Assert.assertTrue(groupName1.startsWith(TEST_GROUP));

        final String groupName2 = ConnectUtil.createGroupName(TEST_GROUP, "group");
        Assert.assertTrue(groupName2.endsWith("group"));
    }

    @Test
    public void createInstanceTest() {
        String servers = "localhost:9876;localhost:9877";
        final String instance = ConnectUtil.createInstance(servers);
        List<String> serverList = new ArrayList<>();
        serverList.add(NAME_SERVER_ADDR);
        serverList.add("localhost:9877");
        Assert.assertTrue(instance.equals(String.valueOf(serverList.toString().hashCode())));
    }

    @Test
    public void createUniqInstanceTest() {
        final String instance = ConnectUtil.createUniqInstance(TEST_GROUP);
        Assert.assertTrue(instance.startsWith(TEST_GROUP));
    }

    @Test
    public void initAllocateConnAndTaskStrategyTest(){
        WorkerConfig connectConfig = new WorkerConfig();
        final AllocateConnAndTaskStrategy strategy = ConnectUtil.initAllocateConnAndTaskStrategy(connectConfig);
        Assert.assertEquals("DefaultAllocateConnAndTaskStrategy", strategy.getClass().getSimpleName());
    }

    @Test
    public void initDefaultMQProducerTest() {
        WorkerConfig connectConfig = new WorkerConfig();
        connectConfig.setNamesrvAddr(NAME_SERVER_ADDR);
        final DefaultMQProducer producer = ConnectUtil.initDefaultMQProducer(connectConfig);
        Assert.assertEquals(NAME_SERVER_ADDR, producer.getNamesrvAddr());
        Assert.assertEquals(30000, producer.getPollNameServerInterval());
        Assert.assertEquals(30000, producer.getHeartbeatBrokerInterval());
        Assert.assertEquals(5000, producer.getPersistConsumerOffsetInterval());
    }

    @Test
    public void initDefaultMQPushConsumerTest() {
        WorkerConfig connectConfig = new WorkerConfig();
        connectConfig.setNamesrvAddr(NAME_SERVER_ADDR);
        final DefaultMQPushConsumer consumer = ConnectUtil.initDefaultMQPushConsumer(connectConfig);
        Assert.assertEquals(NAME_SERVER_ADDR, consumer.getNamesrvAddr());
        Assert.assertEquals(30000, consumer.getPollNameServerInterval());
        Assert.assertEquals(30000, consumer.getHeartbeatBrokerInterval());
        Assert.assertEquals(5000, consumer.getPersistConsumerOffsetInterval());
    }

    @Test
    public void startMQAdminToolTest() throws MQClientException {
        WorkerConfig connectConfig = new WorkerConfig();
        connectConfig.setNamesrvAddr(NAME_SERVER_ADDR);
        final DefaultMQAdminExt defaultMQAdminExt = ConnectUtil.startMQAdminTool(connectConfig);
        Assert.assertEquals(NAME_SERVER_ADDR, defaultMQAdminExt.getNamesrvAddr());
        Assert.assertEquals(30000, defaultMQAdminExt.getPollNameServerInterval());
        Assert.assertEquals(30000, defaultMQAdminExt.getHeartbeatBrokerInterval());
        Assert.assertEquals(5000, defaultMQAdminExt.getPersistConsumerOffsetInterval());
    }

    @Test
    public void createTopicTest() {
        WorkerConfig connectConfig = new WorkerConfig();
        connectConfig.setNamesrvAddr(NAME_SERVER_ADDR);
        TopicConfig topicConfig = new TopicConfig();
        topicConfig.setTopicName(TEST_TOPIC);
        Assertions.assertThatCode(() -> ConnectUtil.createTopic(connectConfig, topicConfig)).doesNotThrowAnyException();

        final boolean exist = ConnectUtil.isTopicExist(connectConfig, TEST_TOPIC);
        Assert.assertTrue(exist);
    }

    @Test
    public void fetchAllConsumerGroupListTest() {
        WorkerConfig connectConfig = new WorkerConfig();
        connectConfig.setNamesrvAddr(NAME_SERVER_ADDR);
        final Set<String> consumerGroupSet = ConnectUtil.fetchAllConsumerGroupList(connectConfig);
        Assert.assertTrue(consumerGroupSet.contains("Consumer-group-one"));
    }

    @Test
    public void createSubGroupTest() {
        WorkerConfig connectConfig = new WorkerConfig();
        connectConfig.setNamesrvAddr(NAME_SERVER_ADDR);
        final String group = ConnectUtil.createSubGroup(connectConfig, TEST_SUBSCRIPTION_GROUP);
        Assert.assertEquals(TEST_SUBSCRIPTION_GROUP, group);
    }

    @Test
    public void convertToRecordPartitionTest() {
        MessageQueue messageQueue = new MessageQueue();
        messageQueue.setTopic(TEST_TOPIC);
        messageQueue.setBrokerName("mockBrokerName");
        messageQueue.setQueueId(0);
        final RecordPartition recordPartition1 = ConnectUtil.convertToRecordPartition(messageQueue);
        final Map<String, ?> partition = recordPartition1.getPartition();
        Assert.assertEquals(TEST_TOPIC, partition.get("topic"));
        Assert.assertEquals("mockBrokerName", partition.get("brokerName"));
        Assert.assertEquals("0", partition.get("queueId"));

        final RecordPartition recordPartition2 = ConnectUtil.convertToRecordPartition(TEST_TOPIC, "mockBrokerName", 0);
        final Map<String, ?> partition2 = recordPartition2.getPartition();
        Assert.assertEquals(TEST_TOPIC, partition2.get("topic"));
        Assert.assertEquals("mockBrokerName", partition2.get("brokerName"));
        Assert.assertEquals("0", partition2.get("queueId"));

    }

    @Test
    public void convertToRecordOffsetTest() {
        final RecordOffset recordOffset = ConnectUtil.convertToRecordOffset(1L);
        System.out.println(recordOffset.getOffset());
        Assert.assertEquals("1", recordOffset.getOffset().get("queueOffset"));

        final Long offset = ConnectUtil.convertToOffset(recordOffset);
        Assert.assertTrue(offset == 1);
    }
}
