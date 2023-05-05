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

package org.apache.rocketmq.connect.runtime.utils;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import io.openmessaging.connector.api.data.RecordOffset;
import io.openmessaging.connector.api.data.RecordPartition;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.acl.common.AclClientRPCHook;
import org.apache.rocketmq.acl.common.SessionCredentials;
import org.apache.rocketmq.client.consumer.DefaultLitePullConsumer;
import org.apache.rocketmq.client.consumer.DefaultMQPullConsumer;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.admin.ConsumeStats;
import org.apache.rocketmq.common.admin.OffsetWrapper;
import org.apache.rocketmq.common.admin.TopicOffset;
import org.apache.rocketmq.common.admin.TopicStatsTable;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.ResponseCode;
import org.apache.rocketmq.common.protocol.body.ClusterInfo;
import org.apache.rocketmq.common.protocol.body.SubscriptionGroupWrapper;
import org.apache.rocketmq.common.protocol.route.BrokerData;
import org.apache.rocketmq.common.protocol.route.TopicRouteData;
import org.apache.rocketmq.common.subscription.SubscriptionGroupConfig;
import org.apache.rocketmq.connect.runtime.common.ConnectKeyValue;
import org.apache.rocketmq.connect.runtime.config.ConnectorConfig;
import org.apache.rocketmq.connect.runtime.config.WorkerConfig;
import org.apache.rocketmq.connect.runtime.service.strategy.AllocateConnAndTaskStrategy;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.remoting.common.RemotingUtil;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.apache.rocketmq.remoting.protocol.LanguageCode;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;
import org.apache.rocketmq.tools.command.CommandUtil;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static org.apache.rocketmq.connect.runtime.connectorwrapper.WorkerSinkTask.QUEUE_OFFSET;

public class ConnectUtil {

    public static final String SYS_TASK_CG_PREFIX = "connect-";
    private final static AtomicLong GROUP_POSTFIX_ID = new AtomicLong(0);

    public static String createGroupName(String prefix) {
        StringBuilder sb = new StringBuilder();
        sb.append(prefix).append("-");
        sb.append(RemotingUtil.getLocalAddress()).append("-");
        sb.append(UtilAll.getPid()).append("-");
        sb.append(System.nanoTime());
        return sb.toString().replace(".", "-");
    }

    public static String createGroupName(String prefix, String postfix) {
        return new StringBuilder().append(prefix).append("-").append(postfix).toString();
    }

    public static String createInstance(String servers) {
        String[] serversArray = servers.split(";");
        List<String> serversList = new ArrayList<String>();
        for (String server : serversArray) {
            if (!serversList.contains(server)) {
                serversList.add(server);
            }
        }
        Collections.sort(serversList);
        return String.valueOf(serversList.toString().hashCode());
    }

    public static String createUniqInstance(String prefix) {
        return new StringBuffer(prefix).append("-").append(UUID.randomUUID().toString()).toString();
    }

    public static AllocateConnAndTaskStrategy initAllocateConnAndTaskStrategy(WorkerConfig connectConfig) {
        try {
            return (AllocateConnAndTaskStrategy) Thread.currentThread().getContextClassLoader().loadClass(connectConfig.getAllocTaskStrategy()).newInstance();
        } catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
            throw new IllegalArgumentException(e);
        }
    }

    public static DefaultMQProducer initDefaultMQProducer(WorkerConfig connectConfig) {
        RPCHook rpcHook = null;
        if (connectConfig.getAclEnable()) {
            rpcHook = new AclClientRPCHook(new SessionCredentials(connectConfig.getAccessKey(), connectConfig.getSecretKey()));
        }
        DefaultMQProducer producer = new DefaultMQProducer(rpcHook);
        producer.setNamesrvAddr(connectConfig.getNamesrvAddr());
        producer.setInstanceName(createUniqInstance(connectConfig.getNamesrvAddr()));
        producer.setProducerGroup(connectConfig.getRmqProducerGroup());
        producer.setSendMsgTimeout(connectConfig.getOperationTimeout());
        producer.setMaxMessageSize(ConnectorConfig.MAX_MESSAGE_SIZE);
        producer.setLanguage(LanguageCode.JAVA);
        return producer;
    }

    public static DefaultMQPullConsumer initDefaultMQPullConsumer(WorkerConfig connectConfig) {
        RPCHook rpcHook = null;
        if (connectConfig.getAclEnable()) {
            rpcHook = new AclClientRPCHook(new SessionCredentials(connectConfig.getAccessKey(), connectConfig.getSecretKey()));
        }
        DefaultMQPullConsumer consumer = new DefaultMQPullConsumer(rpcHook);
        consumer.setNamesrvAddr(connectConfig.getNamesrvAddr());
        consumer.setInstanceName(createUniqInstance(connectConfig.getNamesrvAddr()));
        consumer.setConsumerGroup(connectConfig.getRmqConsumerGroup());
        consumer.setMaxReconsumeTimes(connectConfig.getRmqMaxRedeliveryTimes());
        consumer.setBrokerSuspendMaxTimeMillis(connectConfig.getBrokerSuspendMaxTimeMillis());
        consumer.setConsumerPullTimeoutMillis(connectConfig.getRmqMessageConsumeTimeout());
        consumer.setLanguage(LanguageCode.JAVA);
        return consumer;
    }

    public static DefaultMQPushConsumer initDefaultMQPushConsumer(WorkerConfig connectConfig) {
        RPCHook rpcHook = null;
        if (connectConfig.getAclEnable()) {
            rpcHook = new AclClientRPCHook(new SessionCredentials(connectConfig.getAccessKey(), connectConfig.getSecretKey()));
        }
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(rpcHook);
        consumer.setNamesrvAddr(connectConfig.getNamesrvAddr());
        consumer.setInstanceName(createUniqInstance(connectConfig.getNamesrvAddr()));
        consumer.setConsumerGroup(createGroupName(connectConfig.getRmqConsumerGroup()));
        consumer.setMaxReconsumeTimes(connectConfig.getRmqMaxRedeliveryTimes());
        consumer.setConsumeTimeout(connectConfig.getRmqMessageConsumeTimeout());
        consumer.setConsumeThreadMin(connectConfig.getRmqMinConsumeThreadNums());
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET);
        consumer.setLanguage(LanguageCode.JAVA);
        return consumer;
    }

    public static DefaultMQAdminExt startMQAdminTool(WorkerConfig connectConfig) throws MQClientException {
        RPCHook rpcHook = null;
        if (connectConfig.getAclEnable()) {
            rpcHook = new AclClientRPCHook(new SessionCredentials(connectConfig.getAccessKey(), connectConfig.getSecretKey()));
        }
        DefaultMQAdminExt defaultMQAdminExt = new DefaultMQAdminExt(rpcHook);
        defaultMQAdminExt.setNamesrvAddr(connectConfig.getNamesrvAddr());
        defaultMQAdminExt.setAdminExtGroup(connectConfig.getAdminExtGroup());
        defaultMQAdminExt.setInstanceName(ConnectUtil.createUniqInstance(connectConfig.getNamesrvAddr()));
        defaultMQAdminExt.start();
        return defaultMQAdminExt;
    }

    public static void createTopic(WorkerConfig connectConfig, TopicConfig topicConfig) {
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
            throw new RuntimeException("Create topic [" + topicConfig.getTopicName() + "] failed", e);
        } finally {
            if (defaultMQAdminExt != null) {
                defaultMQAdminExt.shutdown();
            }
        }
    }

    public static boolean isTopicExist(WorkerConfig connectConfig, String topic) {
        DefaultMQAdminExt defaultMQAdminExt = null;
        boolean foundTopicRouteInfo = false;
        try {
            defaultMQAdminExt = startMQAdminTool(connectConfig);
            TopicRouteData topicRouteData = defaultMQAdminExt.examineTopicRouteInfo(topic);
            if (topicRouteData != null) {
                foundTopicRouteInfo = true;
            }
        } catch (Exception e) {
            if (e instanceof MQClientException) {
                if (((MQClientException) e).getResponseCode() == ResponseCode.TOPIC_NOT_EXIST) {
                    foundTopicRouteInfo = false;
                } else {
                    throw new RuntimeException("Get topic route info  failed", e);
                }
            } else {
                throw new RuntimeException("Get topic route info  failed", e);
            }
        } finally {
            if (defaultMQAdminExt != null) {
                defaultMQAdminExt.shutdown();
            }
        }
        return foundTopicRouteInfo;
    }

    public static Set<String> fetchAllConsumerGroupList(WorkerConfig connectConfig) {
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
            throw new RuntimeException("Fetch all topic failed", e);
        } finally {
            if (defaultMQAdminExt != null) {
                defaultMQAdminExt.shutdown();
            }
        }
        return consumerGroupSet;
    }

    public static String createSubGroup(WorkerConfig connectConfig, String subGroup) {
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


    public static RecordPartition convertToRecordPartition(MessageQueue messageQueue) {
        Map<String, String> map = new HashMap<>();
        map.put("topic", messageQueue.getTopic());
        map.put("brokerName", messageQueue.getBrokerName());
        map.put("queueId", messageQueue.getQueueId() + "");
        RecordPartition recordPartition = new RecordPartition(map);
        return recordPartition;
    }

    /**
     * convert to message queue
     *
     * @param recordPartition
     * @return
     */
    public static MessageQueue convertToMessageQueue(RecordPartition recordPartition) {
        Map<String, ?> partion = recordPartition.getPartition();
        String topic = partion.get("topic").toString();
        String brokerName = partion.get("brokerName").toString();
        int queueId = partion.containsKey("queueId") ? Integer.parseInt(partion.get("queueId").toString()) : 0;
        return new MessageQueue(topic, brokerName, queueId);
    }

    public static RecordOffset convertToRecordOffset(Long offset) {
        Map<String, String> offsetMap = new HashMap<>();
        offsetMap.put(QUEUE_OFFSET, offset + "");
        RecordOffset recordOffset = new RecordOffset(offsetMap);
        return recordOffset;
    }

    public static Long convertToOffset(RecordOffset recordOffset) {
        if (null == recordOffset || null == recordOffset.getOffset()) {
            return null;
        }
        Map<String, ?> offsetMap = (Map<String, String>) recordOffset.getOffset();
        Object offsetObject = offsetMap.get(QUEUE_OFFSET);
        if (null == offsetObject) {
            return null;
        }
        return Long.valueOf(String.valueOf(offsetObject));
    }


    public static RecordPartition convertToRecordPartition(String topic, String brokerName, int queueId) {
        Map<String, String> map = new HashMap<>();
        map.put("topic", topic);
        map.put("brokerName", brokerName);
        map.put("queueId", queueId + "");
        RecordPartition recordPartition = new RecordPartition(map);
        return recordPartition;
    }

    /**
     * init default lite pull consumer
     *
     * @param connectConfig
     * @return
     * @throws MQClientException
     */
    public static DefaultLitePullConsumer initDefaultLitePullConsumer(WorkerConfig connectConfig, boolean autoCommit) {
        DefaultLitePullConsumer consumer = null;
        if (Objects.isNull(consumer)) {
            if (StringUtils.isBlank(connectConfig.getAccessKey()) && StringUtils.isBlank(connectConfig.getSecretKey())) {
                consumer = new DefaultLitePullConsumer();
            } else {
                consumer = new DefaultLitePullConsumer(getAclRPCHook(connectConfig.getAccessKey(), connectConfig.getSecretKey()));
            }
        }
        consumer.setNamesrvAddr(connectConfig.getNamesrvAddr());
        String uniqueName = Thread.currentThread().getName() + "-" + System.currentTimeMillis() % 1000;
        consumer.setInstanceName(uniqueName);
        consumer.setUnitName(uniqueName);
        consumer.setAutoCommit(autoCommit);
        return consumer;
    }

    private static RPCHook getAclRPCHook(String accessKey, String secretKey) {
        return new AclClientRPCHook(new SessionCredentials(accessKey, secretKey));
    }

    public static DefaultMQPullConsumer initDefaultMQPullConsumer(WorkerConfig connectConfig, ConnectorTaskId id, ConnectKeyValue keyValue) {
        RPCHook rpcHook = null;
        if (connectConfig.getAclEnable()) {
            rpcHook = new AclClientRPCHook(new SessionCredentials(connectConfig.getAccessKey(), connectConfig.getSecretKey()));
        }
        DefaultMQPullConsumer consumer = new DefaultMQPullConsumer(rpcHook);
        consumer.setInstanceName(id.toString());
        String taskGroupId = keyValue.getString("task-group-id");
        if (StringUtils.isNotBlank(taskGroupId)) {
            consumer.setConsumerGroup(taskGroupId);
        } else {
            consumer.setConsumerGroup(SYS_TASK_CG_PREFIX + id.connector());
        }
        if (StringUtils.isNotBlank(connectConfig.getNamesrvAddr())) {
            consumer.setNamesrvAddr(connectConfig.getNamesrvAddr());
        }
        return consumer;
    }


    /**
     * Get topic offsets
     */
    public static Map<String, Map<MessageQueue, TopicOffset>> offsetTopics(
        WorkerConfig config, List<String> topics) {
        Map<String, Map<MessageQueue, TopicOffset>> offsets = Maps.newConcurrentMap();
        DefaultMQAdminExt adminClient = null;
        try {
            adminClient = startMQAdminTool(config);
            for (String topic : topics) {
                TopicStatsTable topicStatsTable = adminClient.examineTopicStats(topic);
                offsets.put(topic, topicStatsTable.getOffsetTable());
            }
            return offsets;
        } catch (MQClientException
                 | MQBrokerException
                 | RemotingException
                 | InterruptedException e) {
            throw new RuntimeException(e);
        } finally {
            if (adminClient != null) {
                adminClient.shutdown();
            }
        }
    }

    /** Flat topics offsets */
    public static Map<MessageQueue, TopicOffset> flatOffsetTopics(
        WorkerConfig config, List<String> topics) {
        Map<MessageQueue, TopicOffset> messageQueueTopicOffsets = Maps.newConcurrentMap();
        offsetTopics(config, topics).values()
            .forEach(
                offsetTopic -> {
                    messageQueueTopicOffsets.putAll(offsetTopic);
                });
        return messageQueueTopicOffsets;
    }

    /** Search offsets by timestamp */
    public static Map<MessageQueue, Long> searchOffsetsByTimestamp(
        WorkerConfig config,
        Collection<MessageQueue> messageQueues,
        Long timestamp) {
        Map<MessageQueue, Long> offsets = Maps.newConcurrentMap();
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

    /** Get consumer group offset */
    public static Map<MessageQueue, Long> currentOffsets(WorkerConfig config, String groupName, List<String> topics, Set<MessageQueue> messageQueues) {
        // Get consumer group offset
        DefaultMQAdminExt adminClient = null;
        try {
            adminClient = startMQAdminTool(config);
            Map<MessageQueue, OffsetWrapper> consumerOffsets = Maps.newConcurrentMap();
            for (String topic : topics) {
                ConsumeStats consumeStats = adminClient.examineConsumeStats(groupName, topic);
                consumerOffsets.putAll(consumeStats.getOffsetTable());
            }
            return consumerOffsets.keySet().stream()
                .filter(messageQueue -> messageQueues.contains(messageQueue))
                .collect(
                    Collectors.toMap(
                        messageQueue -> messageQueue,
                        messageQueue ->
                            consumerOffsets.get(messageQueue).getConsumerOffset()));
        } catch (MQClientException
                 | MQBrokerException
                 | RemotingException
                 | InterruptedException e) {
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

}
