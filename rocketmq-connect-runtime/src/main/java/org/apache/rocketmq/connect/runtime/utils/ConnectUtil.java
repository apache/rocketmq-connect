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

import io.openmessaging.connector.api.data.RecordOffset;
import io.openmessaging.connector.api.data.RecordPartition;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.apache.rocketmq.acl.common.AclClientRPCHook;
import org.apache.rocketmq.acl.common.SessionCredentials;
import org.apache.rocketmq.client.consumer.DefaultLitePullConsumer;
import org.apache.rocketmq.client.consumer.DefaultMQPullConsumer;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.utils.NetworkUtil;
import org.apache.rocketmq.connect.common.ConsumerConfiguration;
import org.apache.rocketmq.connect.common.ProducerConfiguration;
import org.apache.rocketmq.connect.common.RocketMqBaseConfiguration;
import org.apache.rocketmq.connect.common.RocketMqUtils;
import org.apache.rocketmq.connect.runtime.config.ConnectorConfig;
import org.apache.rocketmq.connect.runtime.config.WorkerConfig;
import org.apache.rocketmq.connect.runtime.service.strategy.AllocateConnAndTaskStrategy;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.remoting.protocol.LanguageCode;
import org.apache.rocketmq.remoting.protocol.admin.TopicOffset;

import static org.apache.rocketmq.connect.runtime.connectorwrapper.WorkerSinkTask.QUEUE_OFFSET;

public class ConnectUtil {

    public static final String SYS_TASK_CG_PREFIX = "connect-";

    public static String generateGroupName(String prefix) {
        StringBuilder sb = new StringBuilder();
        sb.append(prefix).append("-");
        sb.append(NetworkUtil.getLocalAddress()).append("-");
        sb.append(UtilAll.getPid()).append("-");
        sb.append(System.nanoTime());
        return sb.toString().replace(".", "-");
    }

    public static String generateGroupName(String prefix, String postfix) {
        return new StringBuilder().append(prefix).append("-").append(postfix).toString();
    }

    private static String createUniqInstance(String prefix) {
        return prefix + "-" + UUID.randomUUID();
    }

    public static AllocateConnAndTaskStrategy initAllocateConnAndTaskStrategy(WorkerConfig connectConfig) {
        try {
            return (AllocateConnAndTaskStrategy) Thread.currentThread().getContextClassLoader().loadClass(connectConfig.getAllocTaskStrategy()).newInstance();
        } catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
            throw new IllegalArgumentException(e);
        }
    }

    private static RocketMqBaseConfiguration toBaseConfiguration(WorkerConfig connectConfig, String group) {
        return RocketMqBaseConfiguration
            .builder()
            .namesrvAddr(connectConfig.getNamesrvAddr())
            .aclEnable(connectConfig.isAclEnable())
            .accessKey(connectConfig.getAccessKey())
            .secretKey(connectConfig.getSecretKey())
            .groupId(group)
            .build();
    }

    private static ConsumerConfiguration toConsumerConfiguration(WorkerConfig connectConfig) {
        return ConsumerConfiguration
            .consumerBuilder()
            .namesrvAddr(connectConfig.getNamesrvAddr())
            .aclEnable(connectConfig.isAclEnable())
            .accessKey(connectConfig.getAccessKey())
            .secretKey(connectConfig.getSecretKey())
            .build();
    }

    private static ProducerConfiguration toProducerConfiguration(WorkerConfig connectConfig, String group) {
        return ProducerConfiguration
            .producerBuilder()
            .namesrvAddr(connectConfig.getNamesrvAddr())
            .aclEnable(connectConfig.isAclEnable())
            .accessKey(connectConfig.getAccessKey())
            .secretKey(connectConfig.getSecretKey())
            .groupId(group)
            .maxMessageSize(ConnectorConfig.MAX_MESSAGE_SIZE)
            .sendMsgTimeout(connectConfig.getOperationTimeout())
            .build();
    }

    public static DefaultMQProducer initDefaultMQProducer(WorkerConfig connectConfig) {
        ProducerConfiguration producerConfiguration = toProducerConfiguration(connectConfig, connectConfig.getRmqProducerGroup());
        return RocketMqUtils.initDefaultMQProducer(producerConfiguration);
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

    public static void maybeCreateTopic(WorkerConfig connectConfig, TopicConfig topicConfig) {
        RocketMqUtils.maybeCreateTopic(toBaseConfiguration(connectConfig, connectConfig.getAdminExtGroup()), topicConfig);
    }

    public static boolean isTopicExist(WorkerConfig connectConfig, String topic) {
        return RocketMqUtils.isTopicExist(toBaseConfiguration(connectConfig, connectConfig.getAdminExtGroup()), topic);
    }

    public static Set<String> fetchAllConsumerGroupList(WorkerConfig connectConfig) {
        RocketMqBaseConfiguration baseConfiguration = toBaseConfiguration(connectConfig, connectConfig.getAdminExtGroup());
        return RocketMqUtils.fetchAllConsumerGroup(baseConfiguration);
    }

    public static String createSubGroup(WorkerConfig connectConfig, String subGroup) {
        RocketMqBaseConfiguration baseConfiguration = toBaseConfiguration(connectConfig, connectConfig.getAdminExtGroup());
        return RocketMqUtils.createGroup(baseConfiguration, subGroup);
    }

    /**
     * init default lite pull consumer
     *
     * @param connectConfig
     * @return
     * @throws MQClientException
     */
    public static DefaultLitePullConsumer initDefaultLitePullConsumer(WorkerConfig connectConfig, boolean autoCommit) {
        ConsumerConfiguration consumerConfiguration = toConsumerConfiguration(connectConfig);
        return RocketMqUtils.initDefaultLitePullConsumer(consumerConfiguration, autoCommit);
    }

    /**
     * Get topic offsets
     */
    public static Map<String, Map<MessageQueue, TopicOffset>> offsetTopics(WorkerConfig config, List<String> topics) {
        RocketMqBaseConfiguration baseConfiguration = toBaseConfiguration(config, config.getAdminExtGroup());
        return RocketMqUtils.offsetTopics(baseConfiguration, topics);
    }

    /**
     * Get consumer group offset
     */
    public static Map<MessageQueue, Long> currentOffsets(WorkerConfig config, String groupName, List<String> topics,
        Set<MessageQueue> messageQueues) {
        RocketMqBaseConfiguration baseConfiguration = toBaseConfiguration(config, config.getAdminExtGroup());
        return RocketMqUtils.currentOffsets(baseConfiguration, groupName, topics, messageQueues);
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

}
