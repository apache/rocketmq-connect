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

package org.apache.rocketmq.connect.metrics.reporter;

import com.beust.jcommander.internal.Sets;
import org.apache.rocketmq.acl.common.AclClientRPCHook;
import org.apache.rocketmq.acl.common.SessionCredentials;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.protocol.body.ClusterInfo;
import org.apache.rocketmq.common.protocol.body.SubscriptionGroupWrapper;
import org.apache.rocketmq.common.protocol.route.BrokerData;
import org.apache.rocketmq.common.protocol.route.TopicRouteData;
import org.apache.rocketmq.common.subscription.SubscriptionGroupConfig;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.remoting.protocol.LanguageCode;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;
import org.apache.rocketmq.tools.command.CommandUtil;

import java.util.HashMap;
import java.util.Set;
import java.util.UUID;


/**
 * rocket connect util
 */
public class RocketMQClientUtil {

    public static String createUniqInstance(String prefix) {
        return new StringBuffer(prefix).append("-").append(UUID.randomUUID()).toString();
    }

    private static RPCHook getAclRPCHook(String accessKey, String secretKey) {
        return new AclClientRPCHook(new SessionCredentials(accessKey, secretKey));
    }


    public static DefaultMQProducer initDefaultMQProducer(boolean aclEnabled,
                                                          String accessKey,
                                                          String secretKey,
                                                          String groupId,
                                                          String namesrvAddr) {
        RPCHook rpcHook = null;
        if (aclEnabled) {
            rpcHook = new AclClientRPCHook(new SessionCredentials(accessKey, secretKey));
        }
        DefaultMQProducer producer = new DefaultMQProducer(rpcHook);
        producer.setNamesrvAddr(namesrvAddr);
        producer.setInstanceName(createUniqInstance(namesrvAddr));
        producer.setProducerGroup(groupId);
        producer.setSendMsgTimeout(5000);
        producer.setLanguage(LanguageCode.JAVA);
        return producer;
    }

    public static DefaultMQAdminExt startMQAdminTool(boolean aclEnabled,
                                                     String accessKey,
                                                     String secretKey,
                                                     String groupId,
                                                     String namesrvAddr
    ) throws MQClientException {
        DefaultMQAdminExt admin;
        if (aclEnabled) {
            admin = new DefaultMQAdminExt(new AclClientRPCHook(new SessionCredentials(accessKey, secretKey)));
        } else {
            admin = new DefaultMQAdminExt();
        }
        admin.setNamesrvAddr(namesrvAddr);
        admin.setAdminExtGroup(groupId);
        admin.setInstanceName(createUniqInstance(namesrvAddr));
        admin.start();
        return admin;
    }


    public static void createTopic(DefaultMQAdminExt defaultMQAdminExt,
                                   TopicConfig topicConfig) {
        try {
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
        }
    }

    public static boolean topicExist(DefaultMQAdminExt defaultMQAdminExt, String topic) {
        boolean foundTopicRouteInfo = false;
        try {
            TopicRouteData topicRouteData = defaultMQAdminExt.examineTopicRouteInfo(topic);
            if (topicRouteData != null) {
                foundTopicRouteInfo = true;
            }
        } catch (Exception e) {
            foundTopicRouteInfo = false;
        }
        return foundTopicRouteInfo;
    }

    public static Set<String> fetchAllConsumerGroup(DefaultMQAdminExt defaultMQAdminExt) {
        Set<String> consumerGroupSet = Sets.newHashSet();
        try {
            ClusterInfo clusterInfo = defaultMQAdminExt.examineBrokerClusterInfo();
            for (BrokerData brokerData : clusterInfo.getBrokerAddrTable().values()) {
                SubscriptionGroupWrapper subscriptionGroupWrapper = defaultMQAdminExt.getAllSubscriptionGroup(brokerData.selectBrokerAddr(), 3000L);
                consumerGroupSet.addAll(subscriptionGroupWrapper.getSubscriptionGroupTable().keySet());
            }
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("fetch all topic  failed", e);
        }
        return consumerGroupSet;
    }

    public static String createSubGroup(DefaultMQAdminExt defaultMQAdminExt, String subGroup) {
        try {
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
        }
        return subGroup;
    }
}


