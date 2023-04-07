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

import com.alibaba.fastjson.JSON;
import io.openmessaging.KeyValue;
import io.openmessaging.connector.api.component.task.source.SourceTask;
import io.openmessaging.connector.api.data.*;
import io.openmessaging.internal.DefaultKeyValue;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.acl.common.AclClientRPCHook;
import org.apache.rocketmq.acl.common.SessionCredentials;
import org.apache.rocketmq.client.consumer.DefaultLitePullConsumer;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.admin.ConsumeStats;
import org.apache.rocketmq.common.admin.OffsetWrapper;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.body.ClusterInfo;
import org.apache.rocketmq.common.protocol.route.BrokerData;
import org.apache.rocketmq.common.subscription.SubscriptionGroupConfig;
import org.apache.rocketmq.connect.runtime.common.LoggerName;
import org.apache.rocketmq.connect.runtime.config.ConnectorConfig;
import org.apache.rocketmq.connect.runtime.utils.ConnectUtil;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.remoting.exception.RemotingConnectException;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.apache.rocketmq.remoting.exception.RemotingSendRequestException;
import org.apache.rocketmq.remoting.exception.RemotingTimeoutException;
import org.apache.rocketmq.replicator.config.ConsumeFromWhere;
import org.apache.rocketmq.replicator.config.FailoverStrategy;
import org.apache.rocketmq.replicator.config.ReplicatorConnectorConfig;
import org.apache.rocketmq.replicator.context.UnAckMessage;
import org.apache.rocketmq.replicator.exception.StartTaskException;
import org.apache.rocketmq.replicator.stats.ReplicatorTaskStats;
import org.apache.rocketmq.replicator.stats.TpsLimiter;
import org.apache.rocketmq.replicator.utils.ReplicatorUtils;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import static org.apache.rocketmq.connect.runtime.connectorwrapper.WorkerSinkTask.QUEUE_OFFSET;
import static org.apache.rocketmq.connect.runtime.connectorwrapper.WorkerSinkTask.TOPIC;

/**
 * @author osgoo
 */
public class ReplicatorSourceTask extends SourceTask {
    private static final Logger log = LoggerFactory.getLogger(ReplicatorSourceTask.class);

    private static final Logger buglog = LoggerFactory.getLogger(LoggerName.CONNECT_BUG);
    private static final Logger workerErrorMsgLog = LoggerFactory.getLogger(LoggerName.WORKER_ERROR_MSG_ID);

    private ReplicatorConnectorConfig connectorConfig = new ReplicatorConnectorConfig();
    private DefaultMQAdminExt srcMQAdminExt;
    private ScheduledExecutorService metricsMonitorExecutorService = Executors.newSingleThreadScheduledExecutor(new ThreadFactory() {
        @Override
        public Thread newThread(Runnable r) {
            return new Thread(r, "Replicator_lag_metrics");
        }
    });
    private ScheduledExecutorService commitOffsetScheduleService = Executors.newSingleThreadScheduledExecutor(new ThreadFactory() {
        @Override
        public Thread newThread(Runnable r) {
            return new Thread(r, "Commit_offset_schedule");
        }
    });
    private Map<String, List<String>> metricsItem2KeyMap = new HashMap<>();
    private final long period = 60 * 1000;
    private DefaultLitePullConsumer pullConsumer;
    private AtomicLong noMessageCounter = new AtomicLong();
    private Random random = new Random();
    private final int printLogThreshold = 100000;
    private int tpsLimit;

    private AtomicInteger unAckCounter = new AtomicInteger();

    private static final int MAX_UNACK = 5000;

    private ConcurrentHashMap<MessageQueue, TreeMap<Long/* offset */, UnAckMessage/* can commit */>> queue2Offsets = new ConcurrentHashMap<>();

    private ConcurrentHashMap<MessageQueue, Long> mq2MaxOffsets = new ConcurrentHashMap<>();

    private ConcurrentHashMap<MessageQueue, ReadWriteLock> locks = new ConcurrentHashMap<>();
    private List<MessageQueue> normalQueues = new ArrayList<>();

    private AtomicLong circleReplicateCounter = new AtomicLong();

    private ConcurrentHashMap<MessageQueue, AtomicLong> prepareCommitOffset = new ConcurrentHashMap<>();

    private AtomicInteger pollCounter = new AtomicInteger();
    private AtomicInteger rateCounter = new AtomicInteger();

    private static final String REPLICATOR_SRC_TOPIC_PROPERTY_KEY = "REPLICATOR-source-topic";
    // msg born timestamp on src
    private static final String REPLICATOR_BORN_SOURCE_TIMESTAMP = "REPLICATOR-BORN-SOURCE-TIMESTAMP";
    // msg born from where
    private static final String REPLICATOR_BORN_SOURCE_CLOUD_CLUSTER_REGION = "REPLICATOR-BORN-SOURCE";
    // msg born from which topic
    private static final String REPLICATOR_BORE_INSTANCEID_TOPIC = "REPLICATOR-BORN-TOPIC";
    // src message id  equals MessageConst.PROPERTY_EXTEND_UNIQ_INFO
    private static final String REPLICATOR_SRC_MESSAGE_ID = "EXTEND_UNIQ_INFO";
    // src dup info  equals MessageConst.DUP_INFO
    private static final String REPLICATOR_DUP_INFO = "DUP_INFO";

    // following sys reserved properties
    public static final String PROPERTY_TIMER_DELAY_SEC = "TIMER_DELAY_SEC";
    public static final String PROPERTY_TIMER_DELIVER_MS = "TIMER_DELIVER_MS";
    public static final String PROPERTY_TIMER_IN_MS = "TIMER_IN_MS";
    public static final String PROPERTY_TIMER_OUT_MS = "TIMER_OUT_MS";
    public static final String PROPERTY_TIMER_ENQUEUE_MS = "TIMER_ENQUEUE_MS";
    public static final String PROPERTY_TIMER_DEQUEUE_MS = "TIMER_DEQUEUE_MS";
    public static final String PROPERTY_TIMER_ROLL_TIMES = "TIMER_ROLL_TIMES";
    public static final String PROPERTY_TIMER_DEL_UNIQKEY = "TIMER_DEL_UNIQKEY";
    public static final String PROPERTY_TIMER_DELAY_LEVEL = "TIMER_DELAY_LEVEL";
    public static final String PROPERTY_POP_CK = "POP_CK";
    public static final String PROPERTY_POP_CK_OFFSET = "POP_CK_OFFSET";
    public static final String PROPERTY_FIRST_POP_TIME = "1ST_POP_TIME";
    public static final String PROPERTY_VTOA_TUNNEL_ID = "VTOA_TUNNEL_ID";
    private static final Set<String> MQ_SYS_KEYS = new HashSet<String>() {
        {
            add(MessageConst.PROPERTY_MIN_OFFSET);
            add(MessageConst.PROPERTY_TRACE_SWITCH);
            add(MessageConst.PROPERTY_MAX_OFFSET);
            add(MessageConst.PROPERTY_MSG_REGION);
            add(MessageConst.PROPERTY_REAL_TOPIC);
            add(MessageConst.PROPERTY_REAL_QUEUE_ID);
            add(MessageConst.PROPERTY_PRODUCER_GROUP);
            add(MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX);
            add(REPLICATOR_DUP_INFO);
            add(REPLICATOR_SRC_MESSAGE_ID);
            add(MessageConst.PROPERTY_WAIT_STORE_MSG_OK);
            add(MessageConst.PROPERTY_TAGS);
            add(MessageConst.PROPERTY_DELAY_TIME_LEVEL);
            //
            add(MessageConst.PROPERTY_REAL_QUEUE_ID);
            add(MessageConst.PROPERTY_TRANSACTION_PREPARED);
            add(MessageConst.PROPERTY_BUYER_ID);
            add(MessageConst.PROPERTY_ORIGIN_MESSAGE_ID);
            add(MessageConst.PROPERTY_TRANSFER_FLAG);
            add(MessageConst.PROPERTY_CORRECTION_FLAG);
            add(MessageConst.PROPERTY_MQ2_FLAG);
            add(MessageConst.PROPERTY_RECONSUME_TIME);
            add(MessageConst.PROPERTY_MAX_RECONSUME_TIMES);
            add(MessageConst.PROPERTY_CONSUME_START_TIMESTAMP);
            add(MessageConst.PROPERTY_CHECK_IMMUNITY_TIME_IN_SECONDS);
            add(MessageConst.PROPERTY_TRANSACTION_PREPARED_QUEUE_OFFSET);
            add(MessageConst.PROPERTY_INSTANCE_ID);
            add(PROPERTY_TIMER_DELAY_SEC);
            add(PROPERTY_TIMER_DELIVER_MS);
            add(PROPERTY_TIMER_IN_MS);
            add(PROPERTY_TIMER_OUT_MS);
            add(PROPERTY_TIMER_ENQUEUE_MS);
            add(PROPERTY_TIMER_DEQUEUE_MS);
            add(PROPERTY_TIMER_ROLL_TIMES);
            add(PROPERTY_TIMER_DEL_UNIQKEY);
            add(PROPERTY_TIMER_DELAY_LEVEL);
            add(PROPERTY_POP_CK);
            add(PROPERTY_POP_CK_OFFSET);
            add(PROPERTY_FIRST_POP_TIME);
            add(PROPERTY_VTOA_TUNNEL_ID);
        }
    };

    private void buildMqAdminClient() throws MQClientException {
        if (srcMQAdminExt != null) {
            srcMQAdminExt.shutdown();
        }
        RPCHook rpcHook = null;
        if (connectorConfig.isSrcAclEnable()) {
            if (StringUtils.isNotEmpty(connectorConfig.getSrcAccessKey()) && StringUtils.isNotEmpty(connectorConfig.getSrcSecretKey())) {
                String srcAccessKey = connectorConfig.getSrcAccessKey();
                String srcSecretKey = connectorConfig.getSrcSecretKey();
                rpcHook = new AclClientRPCHook(new SessionCredentials(srcAccessKey, srcSecretKey));
            } else {
                rpcHook = new AclClientRPCHook(new SessionCredentials());
            }
        }
        srcMQAdminExt = new DefaultMQAdminExt(rpcHook);
        srcMQAdminExt.setNamesrvAddr(connectorConfig.getSrcEndpoint());
        srcMQAdminExt.setAdminExtGroup(ReplicatorConnectorConfig.ADMIN_GROUP + "-" + UUID.randomUUID());
        srcMQAdminExt.setInstanceName(connectorConfig.generateSourceString() + "-" + UUID.randomUUID());

        log.info("initAdminThread : " + Thread.currentThread().getName());
        srcMQAdminExt.start();
    }

    private void createAndUpdatePullConsumerGroup(String clusterName,
        String subscriptionGroupName) throws InterruptedException, MQBrokerException, RemotingTimeoutException, RemotingSendRequestException, RemotingConnectException {
        SubscriptionGroupConfig subscriptionGroupConfig = new SubscriptionGroupConfig();
        subscriptionGroupConfig.setGroupName(subscriptionGroupName);
        ClusterInfo clusterInfo = srcMQAdminExt.examineBrokerClusterInfo();
        Collection<BrokerData> brokerDatas = clusterInfo.getBrokerAddrTable().values();
        Set<String> brokerNames = null;
        if (StringUtils.isNotEmpty(clusterName)) {
            brokerNames = clusterInfo.getClusterAddrTable().get(clusterName);
        }
        Set<String> masterSet = new HashSet<>();
        for (BrokerData brokerData : brokerDatas) {
            for (Map.Entry<Long, String> entry : brokerData.getBrokerAddrs().entrySet()) {
                if (null != brokerNames && brokerNames.contains(brokerData.getBrokerName()) && entry.getKey().equals(0L)) {
                    masterSet.add(entry.getValue());
                }
            }
        }
        for (String addr : masterSet) {
            try {
                srcMQAdminExt.createAndUpdateSubscriptionGroupConfig(addr, subscriptionGroupConfig);
                log.info("create subscription group to {} success.", addr);
            } catch (Exception e) {
                log.error("create subscription error,", e);
                Thread.sleep(1000);
            }
        }
    }

    private synchronized void buildConsumer() {
        if (pullConsumer != null) {
            return;
        }
        String consumerGroup = connectorConfig.generateTaskIdWithIndexAsConsumerGroup();
        log.info("prepare to use " + consumerGroup + " as consumerGroup start consumer.");
        RPCHook rpcHook = null;
        if (connectorConfig.isSrcAclEnable()) {
            if (StringUtils.isNotEmpty(connectorConfig.getSrcAccessKey()) && StringUtils.isNotEmpty(connectorConfig.getSrcSecretKey())) {
                String srcAccessKey = connectorConfig.getSrcAccessKey();
                String srcSecretKey = connectorConfig.getSrcSecretKey();
                rpcHook = new AclClientRPCHook(new SessionCredentials(srcAccessKey, srcSecretKey));
            } else {
                rpcHook = new AclClientRPCHook(new SessionCredentials());
            }
        }
        pullConsumer = new DefaultLitePullConsumer(consumerGroup, rpcHook);
        String namesrvAddr = connectorConfig.getSrcEndpoint();
        pullConsumer.setNamesrvAddr(namesrvAddr);
        pullConsumer.setInstanceName(connectorConfig.generateSourceString() + "-" + UUID.randomUUID().toString());
        pullConsumer.setAutoCommit(false);
    }

    private void subscribeTopicAndStartConsumer() throws MQClientException {
        ConsumeFromWhere consumeFromWhere = connectorConfig.getConsumeFromWhere();
        pullConsumer.setConsumeFromWhere(org.apache.rocketmq.common.consumer.ConsumeFromWhere.valueOf(consumeFromWhere.name()));
        log.info("litePullConsumer use " + consumeFromWhere.name());
        if (consumeFromWhere == ConsumeFromWhere.CONSUME_FROM_TIMESTAMP) {
            long consumeFromTimestamp = connectorConfig.getConsumeFromTimestamp();
            String timestamp = UtilAll.timeMillisToHumanString3(consumeFromTimestamp);
            pullConsumer.setConsumeTimestamp(timestamp);
            log.info("litePullConsumer consume start at " + timestamp);
        }

        // init normal queues
        String normalQueueStrs = connectorConfig.getDividedNormalQueues();
        List<MessageQueue> allQueues;
        allQueues = parseMessageQueues(normalQueueStrs);
        normalQueues.addAll(allQueues);
        log.info("allQueues : " + allQueues);
        for (MessageQueue mq : allQueues) {
            log.info("mq : " + mq.getBrokerName() + mq.getQueueId() + " " + mq.hashCode() + mq.getClass());
        }

        for (MessageQueue mq : allQueues) {
            String topic = mq.getTopic();
            String tag = ReplicatorConnectorConfig.getSrcTopicTagMap(connectorConfig.getSrcInstanceId(), connectorConfig.getSrcTopicTags()).get(topic);
            pullConsumer.setSubExpressionForAssign(topic, tag);
        }

        try {
            pullConsumer.start();
            pullConsumer.assign(allQueues);
        } catch (MQClientException e) {
            log.error("litePullConsumer start error", e);
            throw e;
        }
    }

    private List<MessageQueue> parseMessageQueues(String queueStrs) {
        log.info("prepare to parse queueStr 2 obj : " + queueStrs);
        List<MessageQueue> allQueues = new ArrayList<>();
        List<MessageQueue> array = JSON.parseArray(queueStrs, MessageQueue.class);
        for (int i = 0; i < array.size(); i++) {
            MessageQueue mq = array.get(i);
            allQueues.add(mq);
        }
        return allQueues;
    }

    private void execScheduleTask() {
        metricsMonitorExecutorService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                try {
                    replicateLagMetric();
                } catch (Throwable e) {
                    log.error("replicate log metric error", e);
                }
            }
        }, period, period, TimeUnit.MILLISECONDS);

        commitOffsetScheduleService.scheduleAtFixedRate(new Runnable() {
            @Override public void run() {
                try {
                    commitOffsetSchedule();
                } catch (Throwable e) {
                    log.error("commit offset error", e);
                }
            }
        }, connectorConfig.getCommitOffsetIntervalMs(), connectorConfig.getCommitOffsetIntervalMs(), TimeUnit.MILLISECONDS);
    }

    private void commitOffsetSchedule() {
        Map<MessageQueue, Long> commitOffsetTable = new HashMap<>();
        prepareCommitOffset.forEach((messageQueue, offset) -> commitOffsetTable.put(messageQueue, offset.get()));
        pullConsumer.commitSync(commitOffsetTable, true);
    }

    private void replicateLagMetric() {
        String consumerGroup = connectorConfig.generateTaskIdWithIndexAsConsumerGroup();
        try {
            ConsumeStats consumeStats = srcMQAdminExt.examineConsumeStats(consumerGroup);
            AtomicLong normalDelayCount = new AtomicLong();
            AtomicLong normalDelayMs = new AtomicLong();
            Map<MessageQueue, OffsetWrapper> offsets = consumeStats.getOffsetTable();
            offsets.forEach(new BiConsumer<MessageQueue, OffsetWrapper>() {
                @Override
                public void accept(MessageQueue messageQueue, OffsetWrapper offsetWrapper) {
                    long delayMs = System.currentTimeMillis() - offsetWrapper.getLastTimestamp();
                    long delayCount = offsetWrapper.getBrokerOffset() - offsetWrapper.getConsumerOffset();
                    if (normalQueues.contains(messageQueue)) {
                        normalDelayCount.addAndGet(delayCount);
                        normalDelayMs.set(delayMs);
                    } else {
                        // unknown queues, just ignore;
                    }
                }
            });
            List<String> delayNumsKeys = new ArrayList<>();
            List<String> delayMsKeys = new ArrayList<>();
            String normalNumKey = connectorConfig.getConnectorId();
            delayNumsKeys.add(normalNumKey);
            ReplicatorTaskStats.incItemValue(ReplicatorTaskStats.REPLICATOR_SOURCE_TASK_DELAY_NUMS, normalNumKey, (int) normalDelayCount.get(), 1);
            String normalMsKey = connectorConfig.getConnectorId();
            delayMsKeys.add(normalMsKey);
            ReplicatorTaskStats.incItemValue(ReplicatorTaskStats.REPLICATOR_SOURCE_TASK_DELAY_MS, normalMsKey, (int) normalDelayMs.get(), 1);

            metricsItem2KeyMap.put(ReplicatorTaskStats.REPLICATOR_SOURCE_TASK_DELAY_NUMS, delayNumsKeys);
            metricsItem2KeyMap.put(ReplicatorTaskStats.REPLICATOR_SOURCE_TASK_DELAY_MS, delayMsKeys);
        } catch (RemotingException | MQClientException e) {
            log.error("occur remoting or mqclient exception, retry build mqadminclient", e);
            try {
                buildMqAdminClient();
            } catch (MQClientException mqClientException) {
                log.error("rebuild mqadmin client error", e);
            }
        } catch (Exception e) {
            log.error(" occur unknown exception", e);
        }
    }

    public synchronized boolean putPulledQueueOffset(MessageQueue mq, long currentOffset, int needAck, MessageExt msg) {
        log.info("putPulledQueueOffset " + mq + ", currentOffset : " + currentOffset + ", ackCount : " + needAck);
        TreeMap<Long, UnAckMessage> offsets = queue2Offsets.get(mq);
        if (offsets == null) {
            TreeMap<Long, UnAckMessage> newOffsets = new TreeMap<>();
            offsets = queue2Offsets.putIfAbsent(mq, newOffsets);
            if (offsets == null) {
                offsets = newOffsets;
            }
        }
        ReadWriteLock mqLock = locks.get(mq);
        if (mqLock == null) {
            ReadWriteLock newLock = new ReentrantReadWriteLock();
            mqLock = locks.putIfAbsent(mq, newLock);
            if (mqLock == null) {
                mqLock = newLock;
            }
        }
        try {
            mqLock.writeLock().lockInterruptibly();
            try {
                UnAckMessage old = offsets.put(currentOffset, new UnAckMessage(needAck, msg, currentOffset, mq));
                if (null == old) {
                    mq2MaxOffsets.put(mq, currentOffset);
                    unAckCounter.incrementAndGet();
                }
                return true;
            } finally {
                mqLock.writeLock().unlock();
            }
        } catch (InterruptedException e) {
            log.error("lock error", e);
            return false;
        }
    }

    @Override
    public List<ConnectRecord> poll() throws InterruptedException {
        if (unAckCounter.get() > MAX_UNACK) {
            Thread.sleep(2);
            if (pollCounter.incrementAndGet() % 1000 == 0) {
                log.info("poll unAckCount > 10000 sleep 2ms");
            }
            return null;
        }
        // sync wait for rate limit
        boolean overflow = TpsLimiter.isOverFlow(sourceTaskContext.getTaskName(), tpsLimit);
        if (overflow) {
            if (rateCounter.incrementAndGet() % 1000 == 0) {
                log.info("rateLimiter occur.");
            }
            return null;
        }
        try {
            List<MessageExt> messageExts = pullConsumer.poll();
//            PullResult pullResult = pullConsumer.pull(mq, tag, pullRequest.getNextOffset(), maxNum);
            if (null != messageExts && messageExts.size() > 0) {
                List<ConnectRecord> connectRecords = new ArrayList<>(messageExts.size());
                int index = 0;
                for (MessageExt msg : messageExts) {
                    MessageQueue mq = new MessageQueue();
                    mq.setTopic(msg.getTopic());
                    mq.setBrokerName(msg.getBrokerName());
                    mq.setQueueId(msg.getQueueId());

                    boolean put = putPulledQueueOffset(mq, msg.getQueueOffset(), 1, msg);
                    if (!put) {
                        log.error("bug");
                        int i = 0;
                        for (MessageExt tmp : messageExts) {
                            if (i++ < index) {
                                removeMessage(mq, tmp.getQueueOffset());
                            }
                        }
                        return null;
                    }
                    index++;

                    ConnectRecord connectRecord = convertToSinkDataEntry(msg);
                    try {
                        if (connectRecord != null) {
                            connectRecords.add(connectRecord);
                            TpsLimiter.addPv(connectorConfig.getConnectorId(), 1);
                        }
                    } finally {
                        if (connectRecord == null) {
                            long canCommitOffset = removeMessage(mq, msg.getQueueOffset());
                            commitOffset(mq, canCommitOffset);
                        }
                    }
                }
                return connectRecords;
            } else {
                if ((noMessageCounter.incrementAndGet() + random.nextInt(10)) % printLogThreshold == 0) {
                    log.info("no new message");
                }
            }
        } catch (Exception e) {
            log.error("pull message error,", e);
        }
        return null;
    }

    private String swapTopic(String topic) {
        if (topic.startsWith("%RETRY%") || topic.startsWith("%DLQ%")) {
            log.error("topic : " + topic + " is retry or dlq.");
            return null;
        }
        String targetTopic = connectorConfig.getDestTopic();
        String targetTopicWithInstanceId;
        if (StringUtils.isEmpty(targetTopic) || StringUtils.isBlank(targetTopic)) {
            targetTopicWithInstanceId = ReplicatorUtils.buildTopicWithNamespace(topic, connectorConfig.getDestInstanceId());
        } else {
            targetTopicWithInstanceId = ReplicatorUtils.buildTopicWithNamespace(targetTopic, connectorConfig.getDestInstanceId());
        }
        return targetTopicWithInstanceId;
    }

    private ConnectRecord convertToSinkDataEntry(MessageExt message) {
        String topic = message.getTopic();
        Map<String, String> properties = message.getProperties();
        log.debug("srcProperties : " + properties);
        Long timestamp;
        ConnectRecord sinkDataEntry = null;

        String connectTimestamp = properties.get(ConnectorConfig.CONNECT_TIMESTAMP);
        timestamp = StringUtils.isNotEmpty(connectTimestamp) ? Long.parseLong(connectTimestamp) : System.currentTimeMillis();
//        String connectSchema = properties.get(ConnectorConfig.CONNECT_SCHEMA);
//        schema = StringUtils.isNotEmpty(connectSchema) ? JSON.parseObject(connectSchema, Schema.class) : null;
        Schema schema = SchemaBuilder.string().build();
        byte[] body = message.getBody();
        String destTopic = swapTopic(topic);
        if (destTopic == null) {
            if (!connectorConfig.getFailoverStrategy().equals(FailoverStrategy.DISMISS)) {
                throw new RuntimeException("cannot find dest topic.");
            } else {
                log.error("swap topic got null, topic : " + topic);
            }
        }
        RecordPartition recordPartition = ConnectUtil.convertToRecordPartition(topic, message.getBrokerName(), message.getQueueId());
        RecordOffset recordOffset = ConnectUtil.convertToRecordOffset(message.getQueueOffset());
        String bodyStr = new String(body, StandardCharsets.UTF_8);
        sinkDataEntry = new ConnectRecord(recordPartition, recordOffset, timestamp, schema, bodyStr);
        KeyValue keyValue = new DefaultKeyValue();
        if (org.apache.commons.collections.MapUtils.isNotEmpty(properties)) {
            for (Map.Entry<String, String> entry : properties.entrySet()) {
                if (MQ_SYS_KEYS.contains(entry.getKey())) {
                    keyValue.put("MQ-SYS-" + entry.getKey(), entry.getValue());
                } else if (entry.getKey().startsWith("connect-ext-")) {
                    keyValue.put(entry.getKey().replaceAll("connect-ext-", ""), entry.getValue());
                } else {
                    keyValue.put(entry.getKey(), entry.getValue());
                }
            }
        }
        // check bornSource have destinationStr + ","
        String bornSource = keyValue.getString(REPLICATOR_BORN_SOURCE_CLOUD_CLUSTER_REGION);
        // skip msg born from destination
        if (bornSource != null && bornSource.contains(connectorConfig.generateDestinationString() + ",")) {
            if (circleReplicateCounter.incrementAndGet() % 100 == 0) {
                log.warn("skip " + circleReplicateCounter.get() + " message have replicated from " + connectorConfig.generateDestinationString() + ", bornSource : " + bornSource + ", message : " + message);
            }
            return null;
        }
        // save all source in born source, format is srcCloud "_" srcCluster "_" srcRegion ",";
        if (StringUtils.isEmpty(bornSource)) {
            bornSource = "";
        }
        keyValue.put(REPLICATOR_BORN_SOURCE_CLOUD_CLUSTER_REGION, bornSource + connectorConfig.generateSourceString() + ",");
        String bornTopic = keyValue.getString(REPLICATOR_BORE_INSTANCEID_TOPIC);
        // save born topic if empty
        if (StringUtils.isEmpty(bornTopic)) {
            // save full topic, format is srcInstanceId "%" srcTopicTags;
            keyValue.put(REPLICATOR_BORE_INSTANCEID_TOPIC, connectorConfig.generateFullSourceTopicTags());
        }
        // put src born timestamp
        keyValue.put(REPLICATOR_BORN_SOURCE_TIMESTAMP, message.getBornTimestamp());
        // put src topic
        keyValue.put(REPLICATOR_SRC_TOPIC_PROPERTY_KEY, topic);
        // save tags
        if (StringUtils.isNotBlank(message.getTags())) {
            keyValue.put(MessageConst.PROPERTY_TAGS, message.getTags());
        }
        // save keys
        if (StringUtils.isNotBlank(message.getKeys())) {
            keyValue.put(MessageConst.PROPERTY_KEYS, message.getKeys());
        }
        // save src messageid
        keyValue.put(REPLICATOR_SRC_MESSAGE_ID, message.getMsgId());
        log.debug("addExtension : " + keyValue.keySet());
        sinkDataEntry.addExtension(keyValue);
        sinkDataEntry.addExtension(TOPIC, destTopic);

        return sinkDataEntry;
    }

    private AtomicLong flushInterval = new AtomicLong();

    public long removeMessage(MessageQueue mq, long removeOffset) {
        TreeMap<Long, UnAckMessage> offsets = queue2Offsets.get(mq);
        if (offsets == null) {
            // warn log, maybe just rebalance
            log.error("queue2Offset get mq wrong, mq : " + mq);
            return -1;
        }
        ReadWriteLock mqLock = locks.get(mq);
        if (mqLock == null) {
            log.error("bug");
            return -1;
        }
        long finalMaxCommitOffset = -1;
        try {
            mqLock.writeLock().lockInterruptibly();
            try {
                if (!offsets.isEmpty()) {
                    Long maxOffset = mq2MaxOffsets.get(mq);
                    if (maxOffset == null) {
                        log.error("bug");
                        return -1;
                    }
                    finalMaxCommitOffset = maxOffset + 1;
                    UnAckMessage prev = offsets.remove(removeOffset);
                    if (prev != null) {
                        unAckCounter.decrementAndGet();
                    }

                    if (!offsets.isEmpty()) {
                        finalMaxCommitOffset = offsets.firstKey();
                    }
                }
            } finally {
                mqLock.writeLock().unlock();
            }
        } catch (Throwable t) {
            log.error("removeMessage exception", t);
        }
        log.info("markQueueCommitted remove mq : " + mq + " offset : " + removeOffset + ", commit offset : " + finalMaxCommitOffset);
        return finalMaxCommitOffset;
    }

    public void commitOffset(MessageQueue mq, long canCommitOffset) {
        if (canCommitOffset == -1) {
            return;
        }
        AtomicLong commitOffset = prepareCommitOffset.get(mq);
        if (commitOffset == null) {
            commitOffset = new AtomicLong(canCommitOffset);
            AtomicLong old = prepareCommitOffset.putIfAbsent(mq, new AtomicLong(canCommitOffset));
            if (old != null) {
                commitOffset = old;
            }
        }
        MixAll.compareAndIncreaseOnly(commitOffset, canCommitOffset);
    }

    @Override
    public void commit(ConnectRecord record, Map<String, String> metadata) {
        if (metadata == null) {
            // send failed
            if (FailoverStrategy.DISMISS.equals(connectorConfig.getFailoverStrategy())) {
                // log
                saveFailedMessage(record, "failed");
            } else {
                saveFailedMessage(record, "failed");
            }
        }
        try {
            // send success, record offset
            Map<String, ?> map = record.getPosition().getPartition().getPartition();
            String brokerName = (String) map.get("brokerName");
            String topic = (String) map.get("topic");
            int queueId = Integer.parseInt((String) map.get("queueId"));
            MessageQueue mq = new MessageQueue(topic, brokerName, queueId);
            Map<String, ?> offsetMap = record.getPosition().getOffset().getOffset();
            long offset = Long.parseLong((String) offsetMap.get(QUEUE_OFFSET));
            long canCommitOffset = removeMessage(mq, offset);
            commitOffset(mq, canCommitOffset);
        } catch (Exception e) {
            buglog.error("[Bug] commit parse record error", e);
        }
    }

    private void saveFailedMessage(Object msg, String errType) {
        workerErrorMsgLog.error("putMessage error " + errType + ", msg : " + msg);
    }

    @Override
    public void validate(KeyValue config) {

    }

    @Override
    public void start(KeyValue config) {
        log.info("ReplicatorSourceTask init " + config);
        log.info("sourceTaskContextConfigs : " + sourceTaskContext.configs());
        // build connectConfig
        connectorConfig.setTaskId(sourceTaskContext.getTaskName().substring(sourceTaskContext.getConnectorName().length()) + 1);
        connectorConfig.setConnectorId(sourceTaskContext.getConnectorName());
        connectorConfig.setSrcCloud(config.getString(ReplicatorConnectorConfig.SRC_CLOUD));
        connectorConfig.setSrcRegion(config.getString(ReplicatorConnectorConfig.SRC_REGION));
        connectorConfig.setSrcCluster(config.getString(ReplicatorConnectorConfig.SRC_CLUSTER));
        connectorConfig.setSrcInstanceId(config.getString(ReplicatorConnectorConfig.SRC_INSTANCEID));
        connectorConfig.setSrcEndpoint(config.getString(ReplicatorConnectorConfig.SRC_ENDPOINT));
        connectorConfig.setSrcTopicTags(config.getString(ReplicatorConnectorConfig.SRC_TOPICTAGS));
        connectorConfig.setDestCloud(config.getString(ReplicatorConnectorConfig.DEST_CLOUD));
        connectorConfig.setDestRegion(config.getString(ReplicatorConnectorConfig.DEST_REGION));
        connectorConfig.setDestCluster(config.getString(ReplicatorConnectorConfig.DEST_CLUSTER));
        connectorConfig.setDestInstanceId(config.getString(ReplicatorConnectorConfig.DEST_INSTANCEID));
        connectorConfig.setDestEndpoint(config.getString(ReplicatorConnectorConfig.DEST_ENDPOINT));
        connectorConfig.setDestTopic(config.getString(ReplicatorConnectorConfig.DEST_TOPIC));
        connectorConfig.setDestAclEnable(Boolean.parseBoolean(config.getString(ReplicatorConnectorConfig.DEST_ACL_ENABLE, "true")));
        connectorConfig.setSrcAclEnable(Boolean.parseBoolean(config.getString(ReplicatorConnectorConfig.SRC_ACL_ENABLE, "true")));
        connectorConfig.setAutoCreateInnerConsumergroup(Boolean.parseBoolean(config.getString(ReplicatorConnectorConfig.AUTO_CREATE_INNER_CONSUMERGROUP, "false")));

        connectorConfig.setSyncTps(config.getInt(ReplicatorConnectorConfig.SYNC_TPS));
        connectorConfig.setDividedNormalQueues(config.getString(ReplicatorConnectorConfig.DIVIDED_NORMAL_QUEUES));
        connectorConfig.setSrcAccessKey(config.getString(ReplicatorConnectorConfig.SRC_ACCESS_KEY));
        connectorConfig.setSrcSecretKey(config.getString(ReplicatorConnectorConfig.SRC_SECRET_KEY));

        connectorConfig.setCommitOffsetIntervalMs(config.getLong(ReplicatorConnectorConfig.COMMIT_OFFSET_INTERVALS_MS, 10 * 1000));

        connectorConfig.setConsumeFromWhere(config.getString(ReplicatorConnectorConfig.CONSUME_FROM_WHERE, ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET.name()));
        if (connectorConfig.getConsumeFromWhere() == ConsumeFromWhere.CONSUME_FROM_TIMESTAMP) {
            connectorConfig.setConsumeFromTimestamp(Long.parseLong(config.getString(ReplicatorConnectorConfig.CONSUME_FROM_TIMESTAMP)));
        }
        log.info("ReplicatorSourceTask connectorConfig : " + connectorConfig);

        try {
            log.info("prepare init ....");
            // get pull consumer group & create group
            String srcClusterName = connectorConfig.getSrcCluster();
            String pullConsumerGroup = connectorConfig.generateTaskIdWithIndexAsConsumerGroup();
            buildMqAdminClient();
            if (connectorConfig.isAutoCreateInnerConsumergroup()) {
                createAndUpdatePullConsumerGroup(srcClusterName, pullConsumerGroup);
            }
            log.info("createAndUpdatePullConsumerGroup " + pullConsumerGroup + " finished.");
            ReplicatorTaskStats.init();
            log.info("TaskStats init.");
            // init converter
            // init pullConsumer
            buildConsumer();
            log.info("buildConsumer finished.");
            // init limiter
            tpsLimit = connectorConfig.getSyncTps();
            log.info("RateLimiter init finished.");
            // subscribe topic & start consumer
            subscribeTopicAndStartConsumer();
            // init sync delay metrics monitor
            execScheduleTask();
            log.info("RateLimiter init finished.");
            log.info("QueueOffsetManager init finished.");
        } catch (Exception e) {
            log.error("start ReplicatorSourceTask error, please check connectorConfig.", e);
            cleanResource();
            throw new StartTaskException("Start replicator source task error, errMsg : " + e.getMessage(), e);
        }

    }

    private void cleanMetricsMonitor() {
        metricsItem2KeyMap.forEach(new BiConsumer<String, List<String>>() {
            @Override
            public void accept(String itemName, List<String> itemKeys) {
                itemKeys.forEach(new Consumer<String>() {
                    @Override
                    public void accept(String itemKey) {
                        ReplicatorTaskStats.getConnectStatsManager().removeAdditionalItem(itemName, itemKey);
                    }
                });
            }
        });
    }

    private void cleanResource() {
        try {
            if (pullConsumer != null) {
                pullConsumer.shutdown();
            }
            if (metricsMonitorExecutorService != null) {
                metricsMonitorExecutorService.shutdown();
            }
            cleanMetricsMonitor();
        } catch (Exception e) {
            log.error("clean resource error,", e);
        }
    }

    @Override
    public void stop() {
        cleanResource();
    }

}
