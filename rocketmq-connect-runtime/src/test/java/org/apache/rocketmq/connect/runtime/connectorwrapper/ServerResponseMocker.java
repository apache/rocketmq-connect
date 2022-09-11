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

import com.alibaba.fastjson.JSON;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.timeout.IdleStateHandler;
import io.netty.util.concurrent.Future;
import org.apache.rocketmq.common.DataVersion;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.protocol.RequestCode;
import org.apache.rocketmq.common.protocol.body.ClusterInfo;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import org.apache.rocketmq.common.protocol.header.GetConsumerListByGroupResponseBody;
import org.apache.rocketmq.common.protocol.route.BrokerData;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.rocketmq.common.protocol.body.SubscriptionGroupWrapper;
import org.apache.rocketmq.common.protocol.route.QueueData;
import org.apache.rocketmq.common.protocol.route.TopicRouteData;
import org.apache.rocketmq.common.subscription.SubscriptionGroupConfig;
import org.apache.rocketmq.remoting.netty.NettyDecoder;
import org.apache.rocketmq.remoting.netty.NettyEncoder;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.RemotingSysResponseCode;
import org.junit.After;
import org.junit.Before;


/**
 * mock server response for command
 */
public abstract class ServerResponseMocker {

    private final NioEventLoopGroup eventLoopGroup = new NioEventLoopGroup();

    public static ServerResponseMocker startServer(int port, byte[] body) {
        return startServer(port, body, null);
    }

    public static ServerResponseMocker startServer(int port, byte[] body, HashMap<String, String> extMap) {
        ServerResponseMocker mocker = new ServerResponseMocker() {
            @Override
            protected int getPort() {
                return port;
            }

            @Override
            protected byte[] getBody() {
                return body;
            }
        };
        mocker.start(extMap);
        // add jvm hook, close connection when jvm down
        Runtime.getRuntime().addShutdownHook(new Thread(mocker::shutdown));
        return mocker;
    }

    @Before
    public void before() {
        start();
    }

    @After
    public void shutdown() {
        if (eventLoopGroup.isShutdown()) {
            return;
        }
        Future<?> future = eventLoopGroup.shutdownGracefully();
        try {
            future.get();
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
    }

    protected abstract int getPort();

    protected abstract byte[] getBody();

    public void start() {
        start(null);
    }

    public void start(HashMap<String, String> extMap) {
        ServerBootstrap serverBootstrap = new ServerBootstrap();
        serverBootstrap.group(eventLoopGroup)
                .channel(NioServerSocketChannel.class)
                .option(ChannelOption.SO_BACKLOG, 1024)
                .option(ChannelOption.SO_REUSEADDR, true)
                .option(ChannelOption.SO_KEEPALIVE, false)
                .childOption(ChannelOption.TCP_NODELAY, true)
                .childOption(ChannelOption.SO_SNDBUF, 65535)
                .childOption(ChannelOption.SO_RCVBUF, 65535)
                .localAddress(new InetSocketAddress(getPort()))
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    public void initChannel(SocketChannel ch) throws Exception {
                        ch.pipeline()
                                .addLast(eventLoopGroup,
                                        new NettyEncoder(),
                                        new NettyDecoder(),
                                        new IdleStateHandler(0, 0, 120),
                                        new ChannelDuplexHandler(),
                                        new NettyServerHandler(extMap)
                                );
                    }
                });
        try {
            ChannelFuture sync = serverBootstrap.bind().sync();
            InetSocketAddress addr = (InetSocketAddress) sync.channel().localAddress();
        } catch (InterruptedException e1) {
            throw new RuntimeException("this.serverBootstrap.bind().sync() InterruptedException", e1);
        }
    }

    @ChannelHandler.Sharable
    private class NettyServerHandler extends SimpleChannelInboundHandler<RemotingCommand> {
        private HashMap<String, String> extMap;

        public NettyServerHandler(HashMap<String, String> extMap) {
            this.extMap = extMap;
        }

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, RemotingCommand msg) throws Exception {
            String remark = "mock data";
            final RemotingCommand response =
                    RemotingCommand.createResponseCommand(RemotingSysResponseCode.SUCCESS, remark);
            response.setOpaque(msg.getOpaque());
            response.setBody(getBody());

            switch (msg.getCode()) {
                case RequestCode.GET_BROKER_CLUSTER_INFO: {
                    final ClusterInfo clusterInfo = buildClusterInfo();
                    response.setBody(JSON.toJSONBytes(clusterInfo));
                    break;
                }

                case RequestCode.GET_ALL_SUBSCRIPTIONGROUP_CONFIG: {
                    final SubscriptionGroupWrapper wrapper = buildSubscriptionGroupWrapper();
                    response.setBody(JSON.toJSONBytes(wrapper));
                    break;
                }

                case RequestCode.GET_ROUTEINFO_BY_TOPIC: {
                    final TopicRouteData topicRouteData = buildTopicRouteData();
                    response.setBody(JSON.toJSONBytes(topicRouteData));
                    break;
                }

                case RequestCode.GET_CONSUMER_LIST_BY_GROUP: {
                    GetConsumerListByGroupResponseBody getConsumerListByGroupResponseBody = new GetConsumerListByGroupResponseBody();
                    getConsumerListByGroupResponseBody.setConsumerIdList(Collections.singletonList("mockConsumer1"));
                    response.setBody(JSON.toJSONBytes(getConsumerListByGroupResponseBody));
                    break;
                }
                default:
                    break;
            }

            if (extMap != null && extMap.size() > 0) {
                response.setExtFields(extMap);
            }
            ctx.writeAndFlush(response);
        }
    }


    private ClusterInfo buildClusterInfo() {
        ClusterInfo clusterInfo = new ClusterInfo();
        HashMap<String, Set<String>> clusterAddrTable = new HashMap<>();
        Set<String> brokerNames = new HashSet<>();
        brokerNames.add("mockBrokerName");
        clusterAddrTable.put("mockCluster", brokerNames);
        clusterInfo.setClusterAddrTable(clusterAddrTable);
        HashMap<String, BrokerData> brokerAddrTable = new HashMap<String, BrokerData>();

        //build brokerData
        BrokerData brokerData = new BrokerData();
        brokerData.setBrokerName("mockBrokerName");
        brokerData.setCluster("mockCluster");

        //build brokerAddrs
        HashMap<Long, String> brokerAddrs = new HashMap<Long, String>();
        brokerAddrs.put(MixAll.MASTER_ID, "127.0.0.1:10911");

        brokerData.setBrokerAddrs(brokerAddrs);
        brokerAddrTable.put("master", brokerData);
        clusterInfo.setBrokerAddrTable(brokerAddrTable);
        return clusterInfo;
    }

    private SubscriptionGroupWrapper buildSubscriptionGroupWrapper() {
        SubscriptionGroupWrapper subscriptionGroupWrapper = new SubscriptionGroupWrapper();
        ConcurrentHashMap<String, SubscriptionGroupConfig> subscriptions = new ConcurrentHashMap<>();
        SubscriptionGroupConfig subscriptionGroupConfig = new SubscriptionGroupConfig();
        subscriptionGroupConfig.setConsumeBroadcastEnable(true);
        subscriptionGroupConfig.setBrokerId(0);
        subscriptionGroupConfig.setGroupName("Consumer-group-one");
        subscriptions.put("Consumer-group-one", subscriptionGroupConfig);
        subscriptionGroupWrapper.setSubscriptionGroupTable(subscriptions);
        DataVersion dataVersion = new DataVersion();
        dataVersion.nextVersion();
        subscriptionGroupWrapper.setDataVersion(dataVersion);
        return subscriptionGroupWrapper;
    }

    private TopicRouteData buildTopicRouteData() {
        TopicRouteData topicRouteData = new TopicRouteData();
        QueueData queueData = new QueueData();
        queueData.setBrokerName("mockBrokerName");
        queueData.setPerm(6);
        queueData.setReadQueueNums(8);
        queueData.setWriteQueueNums(8);
        List<QueueData> queueDataList = new ArrayList<QueueData>();
        queueDataList.add(queueData);
        topicRouteData.setQueueDatas(queueDataList);

        HashMap<Long, String> brokerAddrs = new HashMap<Long, String>();
        brokerAddrs.put(0L, "127.0.0.1:10911");

        BrokerData brokerData = new BrokerData();
        brokerData.setBrokerAddrs(brokerAddrs);
        brokerData.setBrokerName("mockBrokerName");
        brokerData.setCluster("mockCluster");

        List<BrokerData> brokerDataList = new ArrayList<BrokerData>();
        brokerDataList.add(brokerData);
        topicRouteData.setBrokerDatas(brokerDataList);

        topicRouteData.setFilterServerTable(new HashMap<>());
        return topicRouteData;
    }
}

