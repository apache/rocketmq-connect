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
package org.apache.rocketmq.replicator;

import io.openmessaging.KeyValue;
import io.openmessaging.connector.api.component.task.source.SourceTask;
import io.openmessaging.connector.api.data.ConnectRecord;
import org.apache.rocketmq.acl.common.AclClientRPCHook;
import org.apache.rocketmq.acl.common.SessionCredentials;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;
import org.apache.rocketmq.common.subscription.SubscriptionGroupConfig;
import org.apache.rocketmq.replicator.config.ReplicatorConnectorConfig;
import org.apache.rocketmq.replicator.exception.StartTaskException;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.remoting.exception.RemotingConnectException;
import org.apache.rocketmq.remoting.exception.RemotingSendRequestException;
import org.apache.rocketmq.remoting.exception.RemotingTimeoutException;
import org.apache.rocketmq.replicator.stats.ReplicatorTaskStats;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;
import org.apache.rocketmq.tools.command.CommandUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.*;

/**
 * @author osgoo
 */
public class ReplicatorHeartbeatTask extends SourceTask {
    private Logger log = LoggerFactory.getLogger(ReplicatorHeartbeatTask.class);
    private ReplicatorConnectorConfig connectorConfig = new ReplicatorConnectorConfig();
    private DefaultMQProducer producer;
    private final String consumeGroup = "ReplicatorHeartbeatTask";
    private DefaultMQPushConsumer consumer;
    private volatile long producerLastSendOk = System.currentTimeMillis();
    private volatile long consumerLastConsumeOk = System.currentTimeMillis();
    private final long PRODUCER_SEND_ERROR_MAX_LASTING = 15000;
    private final long CONSUMER_CONSUME_ERROR_MAX_LASTING = 15000;
    private final long HEALTH_CHECK_PERIOD_MS = 1000;
    private ScheduledExecutorService executorService;

    private void reBuildProducer() throws Exception {
        if (producer != null) {
            producer.shutdown();
        }
        RPCHook rpcHook = null;
        if (connectorConfig.isSrcAclEnable()) {
            rpcHook = new AclClientRPCHook(new SessionCredentials());
        }
        producer = new DefaultMQProducer(rpcHook);
        producer.setNamesrvAddr(connectorConfig.getSrcEndpoint());
        producer.setProducerGroup(consumeGroup);
        producer.setInstanceName(connectorConfig.generateSourceString() + "-" + UUID.randomUUID().toString());
        producer.start();
    }

    private void createAndUpdatePullConsumerGroup(String clusterName,
        String subscriptionGroupName) throws MQClientException, InterruptedException, MQBrokerException, RemotingTimeoutException, RemotingSendRequestException, RemotingConnectException {
        DefaultMQAdminExt srcMQAdminExt;
        RPCHook rpcHook = null;
        if (connectorConfig.isDestAclEnable()) {
            rpcHook = new AclClientRPCHook(new SessionCredentials());
        }
        srcMQAdminExt = new DefaultMQAdminExt(rpcHook);
        srcMQAdminExt.setNamesrvAddr(connectorConfig.getDestEndpoint());
        srcMQAdminExt.setAdminExtGroup(ReplicatorConnectorConfig.ADMIN_GROUP + "-" + UUID.randomUUID().toString());
        srcMQAdminExt.setInstanceName(connectorConfig.generateDestinationString() + "-" + UUID.randomUUID().toString());

        log.info("initAdminThread : " + Thread.currentThread().getName());
        srcMQAdminExt.start();

        SubscriptionGroupConfig subscriptionGroupConfig = new SubscriptionGroupConfig();
        subscriptionGroupConfig.setGroupName(subscriptionGroupName);
        Set<String> masterSet =
            CommandUtil.fetchMasterAddrByClusterName(srcMQAdminExt, clusterName);
        for (String addr : masterSet) {
            try {
                srcMQAdminExt.createAndUpdateSubscriptionGroupConfig(addr, subscriptionGroupConfig);
                log.info("create subscription group to %s success.%n", addr);
            } catch (Exception e) {
                e.printStackTrace();
                Thread.sleep(1000 * 1);
            }
        }
    }

    private void reBuildConsumer() throws Exception {
        if (consumer != null) {
            consumer.shutdown();
        }
        RPCHook rpcHook = null;
        if (connectorConfig.isDestAclEnable()) {
            rpcHook = new AclClientRPCHook(new SessionCredentials());
        }
        consumer = new DefaultMQPushConsumer(rpcHook);
        consumer.setNamesrvAddr(connectorConfig.getDestEndpoint());
        consumer.setInstanceName(connectorConfig.generateDestinationString() + "-" + UUID.randomUUID().toString());
        consumer.setConsumerGroup(consumeGroup);
        consumer.subscribe(connectorConfig.getHeartbeatTopic(), "*");
        consumer.setMessageModel(MessageModel.CLUSTERING);
        consumer.registerMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> list,
                ConsumeConcurrentlyContext consumeConcurrentlyContext) {
                // check message & calculate rt
                MessageExt messageExt = list.get(0);
                long bornTimestamp = messageExt.getBornTimestamp();
                long storeTimestamp = messageExt.getStoreTimestamp();
                long consumeTimestamp = System.currentTimeMillis();
                long rt = consumeTimestamp - bornTimestamp;
                ReplicatorTaskStats.incItemValue(ReplicatorTaskStats.REPLICATOR_HEARTBEAT_DELAY_MS, connectorConfig.getConnectorId(), (int) rt, 1);
                log.info(messageExt.getUserProperty("src") + " -->  " + messageExt.getUserProperty("dest") + " RT " + bornTimestamp + "," + storeTimestamp + "," + consumeTimestamp);
                consumerLastConsumeOk = consumeTimestamp;
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });
        consumer.start();
    }

    @Override
    public List<ConnectRecord> poll() throws InterruptedException {
        try {
            // health check producer & consumer
            healthCheck();
            Thread.sleep(HEALTH_CHECK_PERIOD_MS);
        } catch (Exception e) {
            log.error("ReplicatorHeartbeatTask healthCheck exception,", e);
        }
        return null;
    }

    private void healthCheck() throws Exception {
        if (producerLastSendOk + PRODUCER_SEND_ERROR_MAX_LASTING < System.currentTimeMillis()) {
            log.error(" rebuild producer, ReplicatorHeartbeatTask healthCheck producer send error > " + PRODUCER_SEND_ERROR_MAX_LASTING);
            reBuildProducer();
        }
        if (consumerLastConsumeOk + CONSUMER_CONSUME_ERROR_MAX_LASTING < System.currentTimeMillis()) {
            log.error(" rebuild consumer, ReplicatorHeartbeatTask healthCheck consumer consume error > " + CONSUMER_CONSUME_ERROR_MAX_LASTING);
            reBuildConsumer();
        }
    }

    @Override
    public void validate(KeyValue config) {

    }

    @Override
    public void start(KeyValue config) {
        log.info("ReplicatorHeartbeatTask init " + config);
        log.info("sourceTaskContextConfigs : " + sourceTaskContext.configs());
        // build connectConfig
        connectorConfig.setTaskId(sourceTaskContext.getTaskName().substring(sourceTaskContext.getConnectorName().length()));
        connectorConfig.setConnectorId(sourceTaskContext.getConnectorName());
        connectorConfig.setSrcCloud(config.getString(connectorConfig.SRC_CLOUD));
        connectorConfig.setSrcRegion(config.getString(connectorConfig.SRC_REGION));
        connectorConfig.setSrcCluster(config.getString(connectorConfig.SRC_CLUSTER));
        connectorConfig.setSrcInstanceId(config.getString(connectorConfig.SRC_INSTANCEID));
        connectorConfig.setSrcEndpoint(config.getString(connectorConfig.SRC_ENDPOINT));
        connectorConfig.setSrcTopicTags(config.getString(connectorConfig.SRC_TOPICTAGS));
        connectorConfig.setDestCloud(config.getString(connectorConfig.DEST_CLOUD));
        connectorConfig.setDestRegion(config.getString(connectorConfig.DEST_REGION));
        connectorConfig.setDestCluster(config.getString(connectorConfig.DEST_CLUSTER));
        connectorConfig.setDestInstanceId(config.getString(connectorConfig.DEST_INSTANCEID));
        connectorConfig.setDestEndpoint(config.getString(connectorConfig.DEST_ENDPOINT));
        connectorConfig.setDestTopic(config.getString(connectorConfig.DEST_TOPIC));

        connectorConfig.setDestAclEnable(Boolean.valueOf(config.getString(ReplicatorConnectorConfig.DEST_ACL_ENABLE, "true")));
        connectorConfig.setSrcAclEnable(Boolean.valueOf(config.getString(ReplicatorConnectorConfig.SRC_ACL_ENABLE, "true")));

        connectorConfig.setHeartbeatIntervalMs(config.getInt(connectorConfig.HEARTBEAT_INTERVALS_MS, connectorConfig.getHeartbeatIntervalMs()));
        connectorConfig.setHeartbeatTopic(config.getString(connectorConfig.HEARTBEAT_TOPIC, connectorConfig.DEFAULT_HEARTBEAT_TOPIC));

        log.info("ReplicatorHeartbeatTask connectorConfig : " + connectorConfig);

        try {
            ReplicatorTaskStats.init();
            // init consumer group
            String destClusterName = connectorConfig.getDestCluster();
            createAndUpdatePullConsumerGroup(destClusterName, consumeGroup);
            // build producer
            reBuildProducer();
            // build consumer
            reBuildConsumer();
            // start schedule task send msg to src heartbeat topic;
            executorService = Executors.newSingleThreadScheduledExecutor(new ThreadFactory() {
                @Override
                public Thread newThread(Runnable r) {
                    return new Thread(r, ReplicatorHeartbeatTask.class.getName() + "-producer");
                }
            });
            executorService.scheduleAtFixedRate(new Runnable() {
                @Override
                public void run() {
                    try {
                        log.info("heartbeat prepare send message.");
                        Message message = new Message(connectorConfig.getHeartbeatTopic(), "ping".getBytes("UTF-8"));
                        message.putUserProperty("src", connectorConfig.getSrcCloud());
                        message.putUserProperty("dest", connectorConfig.getDestCloud());
                        message.putUserProperty("heartbeatStartAt", System.currentTimeMillis() + "");
                        SendResult sendResult = producer.send(message);
                        producerLastSendOk = System.currentTimeMillis();
                        log.info("heartbeat send message ok");
                    } catch (Exception e) {
                        log.error("heartbeat producer to src " + connectorConfig.getHeartbeatTopic() + " error,", e);
                    }
                }
            }, connectorConfig.getHeartbeatIntervalMs(), connectorConfig.getHeartbeatIntervalMs(), TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            cleanResource();
            log.error("start ReplicatorHeartbeatTask error,", e);
            throw new StartTaskException("Start Replicator heartbeat task error, errMsg : " + e.getMessage(), e);
        }
    }

    private void cleanResource() {
        try {
            this.executorService.shutdown();
            if (this.producer != null) {
                producer.shutdown();
            }
            if (this.consumer != null) {
                consumer.shutdown();
            }
        } catch (Exception e) {
            log.error("clean resource error,", e);
        }
    }

    @Override
    public void stop() {
        cleanResource();
    }

}
