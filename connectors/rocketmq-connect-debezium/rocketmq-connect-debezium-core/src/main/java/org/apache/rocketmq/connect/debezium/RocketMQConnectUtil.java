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

package org.apache.rocketmq.connect.debezium;

import com.beust.jcommander.internal.Sets;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.acl.common.AclClientRPCHook;
import org.apache.rocketmq.acl.common.SessionCredentials;
import org.apache.rocketmq.client.consumer.DefaultLitePullConsumer;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.body.ClusterInfo;
import org.apache.rocketmq.common.protocol.body.SubscriptionGroupWrapper;
import org.apache.rocketmq.common.protocol.route.BrokerData;
import org.apache.rocketmq.common.protocol.route.QueueData;
import org.apache.rocketmq.common.protocol.route.TopicRouteData;
import org.apache.rocketmq.common.subscription.SubscriptionGroupConfig;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.remoting.common.RemotingUtil;
import org.apache.rocketmq.remoting.protocol.LanguageCode;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;
import org.apache.rocketmq.tools.command.CommandUtil;

import java.util.HashMap;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;


/**
 * rocket connect util
 */
public class RocketMQConnectUtil {

    public static String createGroupName(String prefix) {
        StringBuilder sb = new StringBuilder();
        sb.append(prefix).append("-");
        sb.append(RemotingUtil.getLocalAddress()).append("-");
        sb.append(UtilAll.getPid()).append("-");
        sb.append(System.nanoTime());
        return sb.toString().replace(".", "-");
    }

    public static String createUniqInstance(String prefix) {
        return new StringBuffer(prefix).append("-").append(UUID.randomUUID()).toString();
    }

    public static RPCHook getAclRPCHook(String accessKey, String secretKey) {
        return new AclClientRPCHook(new SessionCredentials(accessKey, secretKey));
    }

    /**
     * init default lite pull consumer
     *
     * @param connectConfig
     * @param autoCommit
     * @return
     * @throws MQClientException
     */
    public static DefaultLitePullConsumer initDefaultLitePullConsumer(RocketMqConnectConfig connectConfig, boolean autoCommit) throws MQClientException {
        DefaultLitePullConsumer consumer = null;
        if (Objects.isNull(consumer)) {
            if (StringUtils.isBlank(connectConfig.getAccessKey()) && StringUtils.isBlank(connectConfig.getSecretKey())) {
                consumer = new DefaultLitePullConsumer(
                        connectConfig.getRmqConsumerGroup()
                );
            } else {
                consumer = new DefaultLitePullConsumer(
                        connectConfig.getRmqConsumerGroup(),
                        getAclRPCHook(connectConfig.getAccessKey(), connectConfig.getSecretKey())
                );
            }
        }
        consumer.setNamesrvAddr(connectConfig.getNamesrvAddr());
        String uniqueName = Thread.currentThread().getName() + "-" + System.currentTimeMillis() % 1000;
        consumer.setInstanceName(uniqueName);
        consumer.setUnitName(uniqueName);
        consumer.setAutoCommit(autoCommit);
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        return consumer;
    }

    /**
     * init default lite pull consumer
     *
     * @param connectConfig
     * @param topic
     * @param autoCommit
     * @return
     * @throws MQClientException
     */
    public static DefaultLitePullConsumer initDefaultLitePullConsumer(RocketMqConnectConfig connectConfig, String topic, boolean autoCommit) throws MQClientException {
        DefaultLitePullConsumer consumer = null;
        if (Objects.isNull(consumer)) {
            if (StringUtils.isBlank(connectConfig.getAccessKey()) && StringUtils.isBlank(connectConfig.getSecretKey())) {
                consumer = new DefaultLitePullConsumer(
                        connectConfig.getRmqConsumerGroup()
                );
            } else {
                consumer = new DefaultLitePullConsumer(
                        connectConfig.getRmqConsumerGroup(),
                        getAclRPCHook(connectConfig.getAccessKey(), connectConfig.getSecretKey())
                );
            }
        }
        consumer.setNamesrvAddr(connectConfig.getNamesrvAddr());
        String uniqueName = Thread.currentThread().getName() + "-" + System.currentTimeMillis() % 1000;
        consumer.setInstanceName(uniqueName);
        consumer.setUnitName(uniqueName);
        consumer.setAutoCommit(autoCommit);
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        consumer.subscribe(topic, "*");
        return consumer;
    }

    public static DefaultMQProducer initDefaultMQProducer(RocketMqConnectConfig connectConfig) {
        RPCHook rpcHook = null;
        if (connectConfig.isAclEnable()) {
            rpcHook = new AclClientRPCHook(new SessionCredentials(connectConfig.getAccessKey(), connectConfig.getSecretKey()));
        }
        DefaultMQProducer producer = new DefaultMQProducer(rpcHook);
        producer.setNamesrvAddr(connectConfig.getNamesrvAddr());
        producer.setInstanceName(createUniqInstance(connectConfig.getNamesrvAddr()));
        producer.setProducerGroup(connectConfig.getRmqConsumerGroup());
        producer.setSendMsgTimeout(connectConfig.getOperationTimeout());
        producer.setLanguage(LanguageCode.JAVA);
        return producer;
    }

    public static DefaultMQAdminExt startMQAdminTool(RocketMqConnectConfig connectConfig) throws MQClientException {
        DefaultMQAdminExt admin;
        if (connectConfig.isAclEnable()) {
            admin = new DefaultMQAdminExt(new AclClientRPCHook(new SessionCredentials(connectConfig.getAccessKey(), connectConfig.getSecretKey())));
        } else {
            admin = new DefaultMQAdminExt();
        }
        admin.setNamesrvAddr(connectConfig.getNamesrvAddr());
        admin.setAdminExtGroup(connectConfig.getRmqConsumerGroup());
        admin.setInstanceName(RocketMQConnectUtil.createUniqInstance(connectConfig.getNamesrvAddr()));
        admin.start();
        return admin;
    }


    public static void createTopic(RocketMqConnectConfig connectConfig, TopicConfig topicConfig) {
        DefaultMQAdminExt defaultMQAdminExt = null;
        try {
            defaultMQAdminExt = startMQAdminTool(connectConfig);
            ClusterInfo clusterInfo = defaultMQAdminExt.examineBrokerClusterInfo();
            HashMap<String, Set<String>> clusterAddrTable = clusterInfo.getClusterAddrTable();
            Set<String> clusterNameSet = clusterAddrTable.keySet();
            for (String clusterName : clusterNameSet) {
                Set<String> masterSet = CommandUtil.fetchMasterAddrByClusterName(defaultMQAdminExt, clusterName);
                for (String addr : masterSet) {
                    defaultMQAdminExt.createAndUpdateTopicConfig(addr, topicConfig);
                }
            }
        } catch (Exception e) {
            throw new RuntimeException("create topic: " + topicConfig.getTopicName() + " failed", e);
        } finally {
            if (defaultMQAdminExt != null) {
                defaultMQAdminExt.shutdown();
            }
        }
    }

    public static boolean topicExist(RocketMqConnectConfig connectConfig, String topic) {
        DefaultMQAdminExt defaultMQAdminExt = null;
        boolean foundTopicRouteInfo = false;
        try {
            defaultMQAdminExt = startMQAdminTool(connectConfig);
            TopicRouteData topicRouteData = defaultMQAdminExt.examineTopicRouteInfo(topic);
            if (topicRouteData != null) {
                foundTopicRouteInfo = true;
            }
        } catch (Exception e) {
            foundTopicRouteInfo = false;
        } finally {
            if (defaultMQAdminExt != null) {
                defaultMQAdminExt.shutdown();
            }
        }
        return foundTopicRouteInfo;
    }


    public static Set<String> fetchAllConsumerGroup(RocketMqConnectConfig connectConfig) {
        Set<String> consumerGroupSet = Sets.newHashSet();
        DefaultMQAdminExt defaultMQAdminExt = null;
        try {
            defaultMQAdminExt = startMQAdminTool(connectConfig);
            ClusterInfo clusterInfo = defaultMQAdminExt.examineBrokerClusterInfo();
            for (BrokerData brokerData : clusterInfo.getBrokerAddrTable().values()) {
                SubscriptionGroupWrapper subscriptionGroupWrapper = defaultMQAdminExt.getAllSubscriptionGroup(brokerData.selectBrokerAddr(), 3000L);
                consumerGroupSet.addAll(subscriptionGroupWrapper.getSubscriptionGroupTable().keySet());
            }
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("fetch all topic  failed", e);
        } finally {
            if (defaultMQAdminExt != null) {
                defaultMQAdminExt.shutdown();
            }
        }
        return consumerGroupSet;
    }

    public static String createSubGroup(RocketMqConnectConfig connectConfig, String subGroup) {
        DefaultMQAdminExt defaultMQAdminExt = null;
        try {
            defaultMQAdminExt = startMQAdminTool(connectConfig);
            SubscriptionGroupConfig initConfig = new SubscriptionGroupConfig();
            initConfig.setGroupName(subGroup);
            ClusterInfo clusterInfo = defaultMQAdminExt.examineBrokerClusterInfo();
            HashMap<String, Set<String>> clusterAddrTable = clusterInfo.getClusterAddrTable();
            Set<String> clusterNameSet = clusterAddrTable.keySet();
            for (String clusterName : clusterNameSet) {
                Set<String> masterSet = CommandUtil.fetchMasterAddrByClusterName(defaultMQAdminExt, clusterName);
                for (String addr : masterSet) {
                    defaultMQAdminExt.createAndUpdateSubscriptionGroupConfig(addr, initConfig);
                }
            }
        } catch (Exception e) {
            throw new RuntimeException("create subGroup: " + subGroup + " failed", e);
        } finally {
            if (defaultMQAdminExt != null) {
                defaultMQAdminExt.shutdown();
            }
        }
        return subGroup;
    }
}


