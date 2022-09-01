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

import com.alibaba.fastjson.JSON;
import io.netty.channel.ChannelHandlerContext;
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.apache.rocketmq.common.protocol.RequestCode;
import org.apache.rocketmq.common.protocol.header.NotifyConsumerIdsChangedRequestHeader;
import org.apache.rocketmq.connect.runtime.config.WorkerConfig;
import org.apache.rocketmq.connect.runtime.connectorwrapper.NameServerMocker;
import org.apache.rocketmq.connect.runtime.connectorwrapper.ServerResponseMocker;
import org.apache.rocketmq.remoting.RemotingClient;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class ClusterManagementServiceImplTest {

    private ClusterManagementServiceImpl clusterManagementService = new ClusterManagementServiceImpl();

    private ServerResponseMocker nameServerMocker;

    private ServerResponseMocker brokerMocker;

    private WorkerConfig workerConfig;

    @Mock
    private RemotingClient  remotingClient;

    private ClusterManagementServiceImpl.WorkerChangeListener workerChangeListener;

    @Mock
    private ChannelHandlerContext channelHandlerContext;

    @Before
    public void before() {
        nameServerMocker = NameServerMocker.startByDefaultConf(9876, 10911);
        brokerMocker = ServerResponseMocker.startServer(10911, "Hello World".getBytes(StandardCharsets.UTF_8));
        workerConfig = new WorkerConfig();
        workerConfig.setNamesrvAddr("localhost:9876");
        clusterManagementService.initialize(workerConfig);
        clusterManagementService.start();
        workerChangeListener = clusterManagementService.new WorkerChangeListener();
    }

    @After
    public void after() {
        clusterManagementService.stop();
        brokerMocker.shutdown();
        nameServerMocker.shutdown();
    }

    @Test
    public void hasClusterStoreTopicTest() {
        final boolean flag = clusterManagementService.hasClusterStoreTopic();
        Assert.assertTrue(flag);
    }

    @Test
    public void getAllAliveWorkersTest() {
        final List<String> workers = clusterManagementService.getAllAliveWorkers();
        Assert.assertEquals(1, workers.size());
        Assert.assertEquals("mockConsumer1", workers.get(0));
    }

    @Test
    public void getCurrentWorkerTest() {
        final String worker = clusterManagementService.getCurrentWorker();
        Assert.assertNotNull(worker);
    }

    @Test
    public void notifyConsumerIdChangedTest() throws Exception {
        ClusterManagementService.WorkerStatusListener workerStatusListener = new ClusterManagementService.WorkerStatusListener() {
            @Override public void onWorkerChange() {

            }
        };
        clusterManagementService.registerListener(workerStatusListener);
        NotifyConsumerIdsChangedRequestHeader requestHeader = new NotifyConsumerIdsChangedRequestHeader();
        requestHeader.setConsumerGroup("mockConsumerGroup");
        RemotingCommand remotingCommand = RemotingCommand.createRequestCommand(RequestCode.NOTIFY_CONSUMER_IDS_CHANGED, requestHeader);
        remotingCommand.setBody(JSON.toJSONBytes(requestHeader));
        final RemotingCommand result = workerChangeListener.processRequest(channelHandlerContext, remotingCommand);
        Assert.assertNull(result);
    }
}
