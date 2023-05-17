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
import io.openmessaging.connector.api.component.task.Task;
import io.openmessaging.connector.api.component.task.source.SourceConnector;
import io.openmessaging.connector.api.errors.ConnectException;
import io.openmessaging.internal.DefaultKeyValue;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.UUID;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.rocketmq.acl.common.AclClientRPCHook;
import org.apache.rocketmq.acl.common.SessionCredentials;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.route.QueueData;
import org.apache.rocketmq.common.protocol.route.TopicRouteData;
import org.apache.rocketmq.replicator.config.*;
import org.apache.rocketmq.replicator.exception.GetMetaDataException;
import org.apache.rocketmq.replicator.exception.InitMQClientException;
import org.apache.rocketmq.replicator.utils.ReplicatorUtils;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;

import static org.apache.rocketmq.replicator.config.ReplicatorConnectorConfig.CONNECT_TOPICNAME;


/**
 * @author osgoo
 */
public class ReplicatorSourceConnector extends SourceConnector {
    private final Log log = LogFactory.getLog(ReplicatorSourceConnector.class);
    private KeyValue connectorConfig;
    private DefaultMQAdminExt srcMQAdminExt;
    private volatile Set<MessageQueue> curMessageQueues = new HashSet<>();
    private long requestTaskReconfigIntervalMs;
    private ScheduledExecutorService requestTaskReconfigExecutorService = Executors.newSingleThreadScheduledExecutor(new ThreadFactory() {
        @Override
        public Thread newThread(Runnable r) {
            return new Thread(r, "Replicator_task_reconfig");
        }
    });

    private synchronized void initAdmin() throws MQClientException {
        if (srcMQAdminExt == null) {
            RPCHook rpcHook = null;
            String srcAclEnable = connectorConfig.getString(ReplicatorConnectorConfig.SRC_ACL_ENABLE, "false");
            if (srcAclEnable.equalsIgnoreCase("true")) {
                String srcAccessKey = connectorConfig.getString(ReplicatorConnectorConfig.SRC_ACCESS_KEY);
                String srcSecretKey = connectorConfig.getString(ReplicatorConnectorConfig.SRC_SECRET_KEY);
                rpcHook = new AclClientRPCHook(new SessionCredentials(srcAccessKey, srcSecretKey));
            }
            srcMQAdminExt = new DefaultMQAdminExt(rpcHook);
            srcMQAdminExt.setNamesrvAddr(connectorConfig.getString(ReplicatorConnectorConfig.SRC_ENDPOINT));
            srcMQAdminExt.setAdminExtGroup(ReplicatorConnectorConfig.ADMIN_GROUP + "-" + UUID.randomUUID().toString());
            srcMQAdminExt.setInstanceName("ReplicatorSourceConnector_InstanceName_" + UUID.randomUUID().toString());

            log.info("initAdminThread : " + Thread.currentThread().getName());
            srcMQAdminExt.start();
        }
        log.info("SOURCE: RocketMQ srcMQAdminExt started");
    }

    private synchronized void closeAdmin() {
        if (srcMQAdminExt != null) {
            srcMQAdminExt.shutdown();
        }
    }

    private synchronized void closeScheduleTask() {
        if (requestTaskReconfigExecutorService != null) {
            requestTaskReconfigExecutorService.shutdown();
        }
    }

    private Set<MessageQueue> fetchMessageQueues(List<String> topicList) {
        Set<MessageQueue> messageQueues = new HashSet<>();
        try {
            for (String topic : topicList) {
                TopicRouteData topicRouteData = srcMQAdminExt.examineTopicRouteInfo(topic);
                for (QueueData qd : topicRouteData.getQueueDatas()) {
                    for (int i = 0; i < qd.getReadQueueNums(); i++) {
                        MessageQueue messageQueue = new MessageQueue(topic, qd.getBrokerName(), i);
                        messageQueues.add(messageQueue);
                    }
                }
            }
        } catch (Exception e) {
            log.error("fetch src topic route error,", e);
            throw new GetMetaDataException("Replicator source connector fetch topic[" + topicList + "] error.", e);
        }
        return messageQueues;
    }

    private List<List<MessageQueue>> divide(List<MessageQueue> taskTopicInfos, int maxTasks) {
        taskTopicInfos = ReplicatorUtils.sortList(taskTopicInfos, new Comparator<MessageQueue>() {
            @Override
            public int compare(MessageQueue o1, MessageQueue o2) {
                return o1.compareTo(o2);
            }
        });
        List<List<MessageQueue>> result = new ArrayList<>(maxTasks);
        for (int i = 0; i < maxTasks; i++) {
            List<MessageQueue> subTasks = new ArrayList<>();
            result.add(subTasks);
            log.info("add subTask");
        }
        for (int i = 0; i < taskTopicInfos.size(); i++) {
            int hash = i % maxTasks;
            MessageQueue messageQueue = taskTopicInfos.get(i);
            result.get(hash).add(messageQueue);
            log.info("subtask add queue" + messageQueue);
        }
        return result;
    }

    private void checkAndRequestTaskReconfig() {
        Map<String, String> topicTagMap = ReplicatorConnectorConfig.getSrcTopicTagMap(
                connectorConfig.getString(ReplicatorConnectorConfig.SRC_INSTANCEID),
                connectorConfig.getString(ReplicatorConnectorConfig.SRC_TOPICTAGS));
        if (MapUtils.isEmpty(topicTagMap)) {
            throw new ConnectException("Source topics & tags config cannot be null, please check the config info");
        }
        List<String> topics = new LinkedList<>(topicTagMap.keySet());
        Set<MessageQueue> updatedMessageQueues = fetchMessageQueues(topics);

        if (CollectionUtils.isEmpty(curMessageQueues)) {
            curMessageQueues = updatedMessageQueues;
            return;
        }
        if (!CollectionUtils.isEqualCollection(curMessageQueues, updatedMessageQueues)) {
            curMessageQueues = updatedMessageQueues;
            connectorContext.requestTaskReconfiguration();
        }
    }

    @Override
    public List<KeyValue> taskConfigs(int maxTasks) {
        // normal topic
        Map<String, String> topicTagMap = ReplicatorConnectorConfig.getSrcTopicTagMap(
                connectorConfig.getString(ReplicatorConnectorConfig.SRC_INSTANCEID),
                connectorConfig.getString(ReplicatorConnectorConfig.SRC_TOPICTAGS));
        if (MapUtils.isEmpty(topicTagMap)) {
            throw new ConnectException("Source topics & tags config cannot be null, please check the config info");
        }
        List<String> topics = new LinkedList<>(topicTagMap.keySet());
        // todo rebalance 使用原生的；runtime & connector 都保存offset；
        // get queue
        curMessageQueues = fetchMessageQueues(topics);
        int taskNum;
        taskNum = Math.min(curMessageQueues.size(), maxTasks);
        log.info("messageQueue : " + curMessageQueues.size() + " " + curMessageQueues);
        // divide
        List<List<MessageQueue>> normalDivided = divide(new ArrayList<>(curMessageQueues), taskNum);
        log.info("normalDivided : " + normalDivided + " " + normalDivided);

        List<KeyValue> configs = new ArrayList<>();
        for (int i = 0; i < taskNum; i++) {
            KeyValue keyValue = new DefaultKeyValue();
            keyValue.put(ReplicatorConnectorConfig.DIVIDED_NORMAL_QUEUES, JSON.toJSONString(normalDivided.get(i)));

            // CONNECTOR_ID is not fulfilled by rebalance
//            keyValue.put(CONNECTOR_ID, connectorConfig.getString(CONNECTOR_ID));
//            keyValue.put(ERRORS_TOLERANCE_CONFIG, connectorConfig.getString(ERRORS_TOLERANCE_CONFIG, ToleranceType.ALL.name()));
            keyValue.put(ReplicatorConnectorConfig.SRC_CLOUD, connectorConfig.getString(ReplicatorConnectorConfig.SRC_CLOUD));
            keyValue.put(ReplicatorConnectorConfig.SRC_REGION, connectorConfig.getString(ReplicatorConnectorConfig.SRC_REGION));
            keyValue.put(ReplicatorConnectorConfig.SRC_CLUSTER, connectorConfig.getString(ReplicatorConnectorConfig.SRC_CLUSTER));
            if (null != connectorConfig.getString(ReplicatorConnectorConfig.SRC_INSTANCEID)) {
                keyValue.put(ReplicatorConnectorConfig.SRC_INSTANCEID, connectorConfig.getString(ReplicatorConnectorConfig.SRC_INSTANCEID));
            }
            keyValue.put(ReplicatorConnectorConfig.SRC_ENDPOINT, connectorConfig.getString(ReplicatorConnectorConfig.SRC_ENDPOINT));
            keyValue.put(ReplicatorConnectorConfig.SRC_TOPICTAGS, connectorConfig.getString(ReplicatorConnectorConfig.SRC_TOPICTAGS));
            keyValue.put(ReplicatorConnectorConfig.SRC_ACL_ENABLE, connectorConfig.getString(ReplicatorConnectorConfig.SRC_ACL_ENABLE, "false"));
            keyValue.put(ReplicatorConnectorConfig.SRC_ACCESS_KEY, connectorConfig.getString(ReplicatorConnectorConfig.SRC_ACCESS_KEY, ""));
            keyValue.put(ReplicatorConnectorConfig.SRC_SECRET_KEY, connectorConfig.getString(ReplicatorConnectorConfig.SRC_SECRET_KEY, ""));
            keyValue.put(ReplicatorConnectorConfig.DEST_CLOUD, connectorConfig.getString(ReplicatorConnectorConfig.DEST_CLOUD));
            keyValue.put(ReplicatorConnectorConfig.DEST_REGION, connectorConfig.getString(ReplicatorConnectorConfig.DEST_REGION));
            keyValue.put(ReplicatorConnectorConfig.DEST_CLUSTER, connectorConfig.getString(ReplicatorConnectorConfig.DEST_CLUSTER));
            if (null != connectorConfig.getString(ReplicatorConnectorConfig.DEST_INSTANCEID)) {
                keyValue.put(ReplicatorConnectorConfig.DEST_INSTANCEID, connectorConfig.getString(ReplicatorConnectorConfig.DEST_INSTANCEID));
            }
            keyValue.put(ReplicatorConnectorConfig.DEST_ENDPOINT, connectorConfig.getString(ReplicatorConnectorConfig.DEST_ENDPOINT));
            keyValue.put(ReplicatorConnectorConfig.DEST_TOPIC, connectorConfig.getString(ReplicatorConnectorConfig.DEST_TOPIC));
            keyValue.put(ReplicatorConnectorConfig.DEST_ACL_ENABLE, connectorConfig.getString(ReplicatorConnectorConfig.DEST_ACL_ENABLE, "false"));
            keyValue.put(ReplicatorConnectorConfig.DEST_ACCESS_KEY, connectorConfig.getString(ReplicatorConnectorConfig.DEST_ACCESS_KEY, ""));
            keyValue.put(ReplicatorConnectorConfig.DEST_SECRET_KEY, connectorConfig.getString(ReplicatorConnectorConfig.DEST_SECRET_KEY, ""));

            keyValue.put(ReplicatorConnectorConfig.SYNC_TPS, connectorConfig.getInt(ReplicatorConnectorConfig.SYNC_TPS, ReplicatorConnectorConfig.DEFAULT_SYNC_TPS));
            keyValue.put(ReplicatorConnectorConfig.COMMIT_OFFSET_INTERVALS_MS, connectorConfig.getLong(ReplicatorConnectorConfig.COMMIT_OFFSET_INTERVALS_MS, 10 * 1000L));

            configs.add(keyValue);
            log.info("ReplicatorSourceConnector sub task config : " + keyValue);
        }
        // sort config's items for consistent rebalance
        configs = ReplicatorUtils.sortList(configs, new Comparator<DefaultKeyValue>() {
            @Override
            public int compare(DefaultKeyValue o1, DefaultKeyValue o2) {
                return buildCompareString(o1).compareTo(buildCompareString(o2));
            }
        });
        return configs;
    }

    private String buildCompareString(DefaultKeyValue keyValue) {
        String normal = keyValue.getString(ReplicatorConnectorConfig.DIVIDED_NORMAL_QUEUES, "");
        return normal;
    }

    @Override
    public Class<? extends Task> taskClass() {
        return ReplicatorSourceTask.class;
    }

    private Set<String> neededParamKeys = new HashSet<String>() {
        {
            add(ReplicatorConnectorConfig.SRC_CLOUD);
            add(ReplicatorConnectorConfig.SRC_REGION);
            add(ReplicatorConnectorConfig.SRC_CLUSTER);
            add(ReplicatorConnectorConfig.SRC_ENDPOINT);
            add(ReplicatorConnectorConfig.SRC_TOPICTAGS);
            add(ReplicatorConnectorConfig.DEST_CLOUD);
            add(ReplicatorConnectorConfig.DEST_REGION);
            add(ReplicatorConnectorConfig.DEST_CLUSTER);
            add(ReplicatorConnectorConfig.DEST_ENDPOINT);
            add(ReplicatorConnectorConfig.SRC_ACL_ENABLE);
            add(ReplicatorConnectorConfig.DEST_ACL_ENABLE);
        }
    };

    @Override
    public void validate(KeyValue config) {
        log.info("source connector validate : " + config);
        if (StringUtils.isNotBlank(config.getString(CONNECT_TOPICNAME))) {
            log.warn("ReplicatorSourceConnector no need to set " + CONNECT_TOPICNAME + ", use " + ReplicatorConnectorConfig.DEST_TOPIC + " instead.");
            // use destInstanceId % destTopic for sink instead of CONNECT_TOPICNAME
            config.put(CONNECT_TOPICNAME, "");
        }
        ReplicatorUtils.checkNeedParams(ReplicatorSourceConnector.class.getName(), config, neededParamKeys);
        String consumeFromWhere = config.getString(ReplicatorConnectorConfig.CONSUME_FROM_WHERE, ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET.name());
        if (StringUtils.isNotBlank(consumeFromWhere) && consumeFromWhere.equals(ConsumeFromWhere.CONSUME_FROM_TIMESTAMP.name())) {
            ReplicatorUtils.checkNeedParamNotEmpty(ReplicatorSourceConnector.class.getName(), config, ReplicatorConnectorConfig.CONSUME_FROM_TIMESTAMP);
        }
    }

    @Override
    public void start(KeyValue keyValue) {
        this.connectorConfig = keyValue;
        try {
            initAdmin();
        } catch (Exception e) {
            log.error("init admin client error", e);
            throw new InitMQClientException("Replicator source connector init mqAdminClient error.", e);
        }
        requestTaskReconfigIntervalMs = connectorConfig.getLong(ReplicatorConnectorConfig.REQUEST_TASK_RECONFIG_INTERVAL_MS, 30 * 1000);
        requestTaskReconfigExecutorService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                try {
                    checkAndRequestTaskReconfig();
                } catch (Throwable e) {
                    log.error("Request task reconfig error", e);
                }
            }
        }, 60 * 1000, requestTaskReconfigIntervalMs, TimeUnit.MILLISECONDS);
    }

    @Override
    public void stop() {
        closeAdmin();
        closeScheduleTask();
    }

}
