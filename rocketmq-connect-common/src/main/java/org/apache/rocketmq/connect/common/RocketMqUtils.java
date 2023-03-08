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

package org.apache.rocketmq.connect.common;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.acl.common.AclClientRPCHook;
import org.apache.rocketmq.acl.common.SessionCredentials;
import org.apache.rocketmq.client.consumer.DefaultLitePullConsumer;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.KeyBuilder;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.connect.common.constant.LoggerName;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.apache.rocketmq.remoting.protocol.LanguageCode;
import org.apache.rocketmq.remoting.protocol.ResponseCode;
import org.apache.rocketmq.remoting.protocol.admin.ConsumeStats;
import org.apache.rocketmq.remoting.protocol.admin.OffsetWrapper;
import org.apache.rocketmq.remoting.protocol.admin.TopicOffset;
import org.apache.rocketmq.remoting.protocol.admin.TopicStatsTable;
import org.apache.rocketmq.remoting.protocol.body.ClusterInfo;
import org.apache.rocketmq.remoting.protocol.body.SubscriptionGroupWrapper;
import org.apache.rocketmq.remoting.protocol.route.BrokerData;
import org.apache.rocketmq.remoting.protocol.route.TopicRouteData;
import org.apache.rocketmq.remoting.protocol.subscription.SubscriptionGroupConfig;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;
import org.apache.rocketmq.tools.command.CommandUtil;

/**
 * RocketMq utils
 */
public class RocketMqUtils {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.ROCKETMQ_RUNTIME);

    public static String createUniqInstance(String prefix) {
        return prefix.concat("-").concat(UUID.randomUUID().toString());
    }

    /**
     * Init default mq producer
     *
     * @param configuration
     * @return
     */
    public static DefaultMQProducer initDefaultMQProducer(ProducerConfiguration configuration) {
        RPCHook rpcHook = null;
        if (configuration.isAclEnable()) {
            rpcHook = new AclClientRPCHook(new SessionCredentials(configuration.getAccessKey(), configuration.getSecretKey()));
        }
        DefaultMQProducer producer = new DefaultMQProducer(rpcHook);
        producer.setNamesrvAddr(configuration.getNamesrvAddr());
        producer.setInstanceName(createUniqInstance(configuration.getNamesrvAddr()));
        producer.setProducerGroup(configuration.getGroupId());
        if (configuration.getSendMsgTimeout() != null) {
            producer.setSendMsgTimeout(configuration.getSendMsgTimeout());
        }
        if (configuration.getMaxMessageSize() != null) {
            producer.setMaxMessageSize(configuration.getMaxMessageSize());
        }
        producer.setLanguage(LanguageCode.JAVA);
        return producer;
    }

    /**
     * init default lite pull consumer
     *
     * @param configuration
     * @return
     * @throws MQClientException
     */
    public static DefaultLitePullConsumer initDefaultLitePullConsumer(ConsumerConfiguration configuration,
        boolean autoCommit) {
        RPCHook rpcHook = null;
        if (configuration.isAclEnable()) {
            rpcHook = new AclClientRPCHook(new SessionCredentials(configuration.getAccessKey(), configuration.getSecretKey()));
        }
        DefaultLitePullConsumer consumer = new DefaultLitePullConsumer(rpcHook);
        consumer.setNamesrvAddr(configuration.getNamesrvAddr());
        String uniqueName = Thread.currentThread().getName() + "-" + System.currentTimeMillis() % 1000;
        consumer.setInstanceName(uniqueName);
        consumer.setUnitName(uniqueName);
        consumer.setAutoCommit(autoCommit);
        if (StringUtils.isNotEmpty(configuration.getGroupId())) {
            consumer.setConsumerGroup(configuration.getGroupId());
        }
        consumer.setLanguage(LanguageCode.JAVA);
        return consumer;
    }

    /**
     * Created when the topic is not exist
     *
     * @param configuration
     * @param config
     */
    public static void maybeCreateTopic(RocketMqBaseConfiguration configuration, TopicConfig config) {
        log.info("Try to create topic: {}!", config.getTopicName());
        DefaultMQAdminExt defaultMQAdminExt = null;
        try {
            defaultMQAdminExt = startMQAdminTool(configuration);
            if (existTopicRoute(defaultMQAdminExt, config.getTopicName())) {
                log.info("Topic [{}] exist!", config.getTopicName());
                // topic exist
                return;
            }
            ClusterInfo clusterInfo = defaultMQAdminExt.examineBrokerClusterInfo();
            Map<String, Set<String>> clusterAddrTable = clusterInfo.getClusterAddrTable();
            Set<String> clusterNameSet = clusterAddrTable.keySet();
            for (String clusterName : clusterNameSet) {
                Set<String> masterSet = CommandUtil.fetchMasterAddrByClusterName(defaultMQAdminExt, clusterName);
                for (String addr : masterSet) {
                    defaultMQAdminExt.createAndUpdateTopicConfig(addr, config);
                }
            }
        } catch (Exception e) {
            throw new RuntimeException("Create topic [" + config.getTopicName() + "] failed", e);
        } finally {
            if (defaultMQAdminExt != null) {
                defaultMQAdminExt.shutdown();
            }
        }
    }

    /**
     * create topic
     *
     * @param configuration
     * @param config
     */
    public static void createTopic(RocketMqBaseConfiguration configuration, TopicConfig config) {
        DefaultMQAdminExt defaultMQAdminExt = null;
        try {
            defaultMQAdminExt = startMQAdminTool(configuration);
            ClusterInfo clusterInfo = defaultMQAdminExt.examineBrokerClusterInfo();
            Map<String, Set<String>> clusterAddrTable = clusterInfo.getClusterAddrTable();
            Set<String> clusterNameSet = clusterAddrTable.keySet();
            for (String clusterName : clusterNameSet) {
                Set<String> masterSet = CommandUtil.fetchMasterAddrByClusterName(defaultMQAdminExt, clusterName);
                for (String addr : masterSet) {
                    defaultMQAdminExt.createAndUpdateTopicConfig(addr, config);
                }
            }
        } catch (Exception e) {
            throw new RuntimeException("Create topic [" + config.getTopicName() + "] failed", e);
        } finally {
            if (defaultMQAdminExt != null) {
                defaultMQAdminExt.shutdown();
            }
        }
    }

    public static Set<String> fetchAllConsumerGroup(RocketMqBaseConfiguration configuration) {
        Set<String> consumerGroupSet = Sets.newHashSet();
        DefaultMQAdminExt defaultMQAdminExt = null;
        try {
            defaultMQAdminExt = startMQAdminTool(configuration);
            ClusterInfo clusterInfo = defaultMQAdminExt.examineBrokerClusterInfo();
            for (BrokerData brokerData : clusterInfo.getBrokerAddrTable().values()) {
                SubscriptionGroupWrapper subscriptionGroupWrapper = defaultMQAdminExt.getAllSubscriptionGroup(brokerData.selectBrokerAddr(), 3000L);
                consumerGroupSet.addAll(subscriptionGroupWrapper.getSubscriptionGroupTable().keySet());
            }
        } catch (Exception e) {
            throw new RuntimeException("Fetch all topic failed", e);
        } finally {
            if (defaultMQAdminExt != null) {
                defaultMQAdminExt.shutdown();
            }
        }
        return consumerGroupSet;
    }

    public static String createGroup(RocketMqBaseConfiguration configuration, String group) {
        DefaultMQAdminExt defaultMQAdminExt = null;
        try {
            defaultMQAdminExt = startMQAdminTool(configuration);
            SubscriptionGroupConfig initConfig = new SubscriptionGroupConfig();
            initConfig.setGroupName(group);
            ClusterInfo clusterInfo = defaultMQAdminExt.examineBrokerClusterInfo();
            Map<String, Set<String>> clusterAddrTable = clusterInfo.getClusterAddrTable();
            Set<String> clusterNameSet = clusterAddrTable.keySet();
            for (String clusterName : clusterNameSet) {
                Set<String> masterSet = CommandUtil.fetchMasterAddrByClusterName(defaultMQAdminExt, clusterName);
                for (String addr : masterSet) {
                    defaultMQAdminExt.createAndUpdateSubscriptionGroupConfig(addr, initConfig);
                }
            }
        } catch (Exception e) {
            throw new RuntimeException("Create group [" + group + "] failed", e);
        } finally {
            if (defaultMQAdminExt != null) {
                defaultMQAdminExt.shutdown();
            }
        }
        return group;
    }

    /**
     * Get topic offsets
     */
    public static Map<String, Map<MessageQueue, TopicOffset>> offsetTopics(
        RocketMqBaseConfiguration config, List<String> topics) {
        Map<String, Map<MessageQueue, TopicOffset>> offsets = Maps.newHashMap();
        DefaultMQAdminExt adminClient = null;
        try {
            adminClient = startMQAdminTool(config);
            for (String topic : topics) {
                TopicStatsTable topicStatsTable = examineTopicStats(adminClient, topic);
                offsets.put(topic, topicStatsTable.getOffsetTable());
            }
            return offsets;
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            if (adminClient != null) {
                adminClient.shutdown();
            }
        }
    }

    /**
     * Flat topics offsets
     */
    public static Map<MessageQueue, TopicOffset> flatOffsetTopics(
        RocketMqBaseConfiguration config, List<String> topics) {
        Map<MessageQueue, TopicOffset> messageQueueTopicOffsets = Maps.newHashMap();
        offsetTopics(config, topics).values()
            .forEach(
                offsetTopic -> {
                    messageQueueTopicOffsets.putAll(offsetTopic);
                });
        return messageQueueTopicOffsets;
    }

    /**
     * Search offsets by timestamp
     */
    public static Map<MessageQueue, Long> searchOffsetsByTimestamp(
        RocketMqBaseConfiguration config,
        Collection<MessageQueue> messageQueues,
        Long timestamp) {
        Map<MessageQueue, Long> offsets = Maps.newHashMap();
        DefaultMQAdminExt adminClient = null;
        try {
            adminClient = startMQAdminTool(config);
            for (MessageQueue messageQueue : messageQueues) {
                long offset = adminClient.searchOffset(messageQueue, timestamp);
                offsets.put(messageQueue, offset);
            }
            return offsets;
        } catch (MQClientException e) {
            throw new RuntimeException(e);
        } finally {
            if (adminClient != null) {
                adminClient.shutdown();
            }
        }
    }

    /**
     * Get consumer group offset
     */
    public static Map<MessageQueue, Long> currentOffsets(RocketMqBaseConfiguration config, String groupName,
        List<String> topics, Set<MessageQueue> messageQueues) {
        // Get consumer group offset
        DefaultMQAdminExt adminClient = null;
        try {
            adminClient = startMQAdminTool(config);
            Map<MessageQueue, OffsetWrapper> consumerOffsets = Maps.newHashMap();
            for (String topic : topics) {
                ConsumeStats consumeStats = examineConsumeStats(adminClient, groupName, topic);
                consumerOffsets.putAll(consumeStats.getOffsetTable());
            }
            return consumerOffsets.keySet().stream()
                .filter(messageQueue -> messageQueues.contains(messageQueue))
                .collect(
                    Collectors.toMap(
                        messageQueue -> messageQueue,
                        messageQueue ->
                            consumerOffsets.get(messageQueue).getConsumerOffset()));
        } catch (Exception e) {
            if (e instanceof MQClientException) {
                if (((MQClientException) e).getResponseCode() == ResponseCode.TOPIC_NOT_EXIST) {
                    return Collections.emptyMap();
                } else {
                    throw new RuntimeException(e);
                }
            } else {
                throw new RuntimeException(e);
            }
        } finally {
            if (adminClient != null) {
                adminClient.shutdown();
            }
        }
    }

    private static DefaultMQAdminExt startMQAdminTool(
        RocketMqBaseConfiguration configuration) throws MQClientException {
        RPCHook rpcHook = null;
        if (configuration.isAclEnable()) {
            rpcHook = new AclClientRPCHook(new SessionCredentials(configuration.getAccessKey(), configuration.getSecretKey()));
        }
        DefaultMQAdminExt defaultMQAdminExt = new DefaultMQAdminExt(rpcHook);
        defaultMQAdminExt.setNamesrvAddr(configuration.getNamesrvAddr());
        defaultMQAdminExt.setAdminExtGroup(configuration.getGroupId());
        defaultMQAdminExt.setInstanceName(createUniqInstance(configuration.getNamesrvAddr()));
        defaultMQAdminExt.start();
        return defaultMQAdminExt;
    }

    public static boolean isTopicExist(RocketMqBaseConfiguration connectConfig, String topic) {
        DefaultMQAdminExt defaultMQAdminExt = null;
        try {
            defaultMQAdminExt = startMQAdminTool(connectConfig);
            return existTopicRoute(defaultMQAdminExt, topic);
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            if (defaultMQAdminExt != null) {
                defaultMQAdminExt.shutdown();
            }
        }
    }

    private static boolean existTopicRoute(DefaultMQAdminExt defaultMQAdminExt, String topic) {
        boolean foundTopicRouteInfo = false;
        try {
            TopicRouteData topicRouteData = defaultMQAdminExt.examineTopicRouteInfo(topic);
            if (topicRouteData != null) {
                foundTopicRouteInfo = true;
            }
        } catch (Exception e) {
            if (e instanceof MQClientException) {
                if (((MQClientException) e).getResponseCode() == ResponseCode.TOPIC_NOT_EXIST) {
                    foundTopicRouteInfo = false;
                } else {
                    throw new RuntimeException("Get topic route info failed", e);
                }
            } else {
                throw new RuntimeException("Get topic route info failed", e);
            }
        }
        return foundTopicRouteInfo;
    }

    /**
     * Compatible with 4.9.4 and earlier
     *
     * @param adminClient
     * @param topic
     * @return
     */
    private static TopicStatsTable examineTopicStats(DefaultMQAdminExt adminClient, String topic) {
        try {
            return adminClient.examineTopicStats(topic);
        } catch (MQBrokerException e) {
            // Compatible with 4.9.4 and earlier
            if (e.getResponseCode() == ResponseCode.REQUEST_CODE_NOT_SUPPORTED) {
                try {
                    log.warn("Examine topic stats failure , the server version is less than 5.1.0, and downward compatibility begins, {}", e.getErrorMessage());
                    return overrideExamineTopicStats(adminClient, topic);
                } catch (Exception ex) {
                    throw new RuntimeException(ex);
                }
            } else {
                throw new RuntimeException(e);
            }
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    /**
     * examineConsumeStats
     * Compatible with 4.9.4 and earlier
     *
     * @param adminClient
     * @param topic
     * @return
     */
    private static ConsumeStats examineConsumeStats(DefaultMQAdminExt adminClient, String groupName, String topic) {
        try {
            return adminClient.examineConsumeStats(groupName, topic);
        } catch (MQBrokerException e) {
            // Compatible with 4.9.4 and earlier
            if (e.getResponseCode() == ResponseCode.REQUEST_CODE_NOT_SUPPORTED) {
                try {
                    log.warn("Examine consume stats failure, the server version is less than 5.1.0, and downward compatibility begins {}", e.getErrorMessage());
                    return overrideExamineConsumeStats(adminClient, groupName, topic);
                } catch (Exception ex) {
                    throw new RuntimeException(ex);
                }
            } else {
                throw new RuntimeException(e);
            }
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    /**
     * Compatible with version 4.9.4
     *
     * @param adminClient
     * @param topic
     * @return
     * @throws RemotingException
     * @throws InterruptedException
     * @throws MQClientException
     * @throws MQBrokerException
     */
    private static TopicStatsTable overrideExamineTopicStats(DefaultMQAdminExt adminClient,
        String topic) throws RemotingException, InterruptedException, MQClientException, MQBrokerException {
        TopicRouteData topicRouteData = adminClient.examineTopicRouteInfo(topic);
        TopicStatsTable topicStatsTable = new TopicStatsTable();
        for (BrokerData bd : topicRouteData.getBrokerDatas()) {
            String addr = bd.selectBrokerAddr();
            if (addr != null) {
                TopicStatsTable tst = adminClient
                    .getDefaultMQAdminExtImpl()
                    .getMqClientInstance()
                    .getMQClientAPIImpl()
                    .getTopicStatsInfo(addr, topic, 5000);
                topicStatsTable.getOffsetTable().putAll(tst.getOffsetTable());
            }
        }
        return topicStatsTable;
    }

    /**
     * Compatible with version 4.9.4
     *
     * @param adminExt
     * @param groupName
     * @param topic
     * @return
     * @throws MQClientException
     * @throws RemotingException
     * @throws InterruptedException
     * @throws MQBrokerException
     */
    private static ConsumeStats overrideExamineConsumeStats(DefaultMQAdminExt adminExt, String groupName,
        String topic) throws MQClientException, RemotingException, InterruptedException, MQBrokerException {
        TopicRouteData topicRouteData = null;
        List<String> routeTopics = new ArrayList<>();
        routeTopics.add(MixAll.getRetryTopic(groupName));
        if (topic != null) {
            routeTopics.add(topic);
            routeTopics.add(KeyBuilder.buildPopRetryTopic(topic, groupName));
        }
        for (int i = 0; i < routeTopics.size(); i++) {
            try {
                topicRouteData = adminExt.getDefaultMQAdminExtImpl().examineTopicRouteInfo(routeTopics.get(i));
                if (topicRouteData != null) {
                    break;
                }
            } catch (Throwable e) {
                if (i == routeTopics.size() - 1) {
                    throw e;
                }
            }
        }
        ConsumeStats result = new ConsumeStats();

        for (BrokerData bd : topicRouteData.getBrokerDatas()) {
            String addr = bd.selectBrokerAddr();
            if (addr != null) {
                ConsumeStats consumeStats = adminExt.getDefaultMQAdminExtImpl().getMqClientInstance().getMQClientAPIImpl().getConsumeStats(addr, groupName, topic, 5000 * 3);
                result.getOffsetTable().putAll(consumeStats.getOffsetTable());
                double value = result.getConsumeTps() + consumeStats.getConsumeTps();
                result.setConsumeTps(value);
            }
        }

        Set<String> topics = Sets.newHashSet();
        for (MessageQueue messageQueue : result.getOffsetTable().keySet()) {
            topics.add(messageQueue.getTopic());
        }

        ConsumeStats staticResult = new ConsumeStats();
        staticResult.setConsumeTps(result.getConsumeTps());

        for (String currentTopic : topics) {
            TopicRouteData currentRoute = adminExt.getDefaultMQAdminExtImpl().examineTopicRouteInfo(currentTopic);
            if (currentRoute.getTopicQueueMappingByBroker() == null
                || currentRoute.getTopicQueueMappingByBroker().isEmpty()) {
                //normal topic
                for (Map.Entry<MessageQueue, OffsetWrapper> entry : result.getOffsetTable().entrySet()) {
                    if (entry.getKey().getTopic().equals(currentTopic)) {
                        staticResult.getOffsetTable().put(entry.getKey(), entry.getValue());
                    }
                }
            }
        }

        if (staticResult.getOffsetTable().isEmpty()) {
            throw new MQClientException(ResponseCode.CONSUMER_NOT_ONLINE, "Not found the consumer group consume stats, because return offset table is empty, maybe the consumer not consume any message");
        }
        return staticResult;
    }

}
