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
 *
 */

package org.apache.rocketmq.replicator;

import io.openmessaging.KeyValue;
import io.openmessaging.connector.api.component.task.Task;
import io.openmessaging.connector.api.component.task.source.SourceConnector;
import io.openmessaging.internal.DefaultKeyValue;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.acl.common.AclClientRPCHook;
import org.apache.rocketmq.acl.common.SessionCredentials;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.constant.PermName;
import org.apache.rocketmq.common.protocol.route.TopicRouteData;
import org.apache.rocketmq.connect.runtime.common.LoggerName;
import org.apache.rocketmq.connect.runtime.config.ConnectorConfig;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.replicator.config.ReplicatorConnectorConfig;
import org.apache.rocketmq.replicator.exception.InitMQClientException;
import org.apache.rocketmq.replicator.utils.ReplicatorUtils;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

public class ReplicatorCheckpointConnector extends SourceConnector {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.REPLICATRO_RUNTIME);
    private KeyValue config;

    private DefaultMQAdminExt targetMqAdminExt;

    @Override
    public List<KeyValue> taskConfigs(int maxTasks) {
        List<KeyValue> configs = new ArrayList<>();
        KeyValue keyValue = new DefaultKeyValue();
        keyValue.put(ReplicatorConnectorConfig.SRC_CLOUD, config.getString(ReplicatorConnectorConfig.SRC_CLOUD));
        keyValue.put(ReplicatorConnectorConfig.SRC_REGION, config.getString(ReplicatorConnectorConfig.SRC_REGION));
        keyValue.put(ReplicatorConnectorConfig.SRC_CLUSTER, config.getString(ReplicatorConnectorConfig.SRC_CLUSTER));
        if (null != config.getString(ReplicatorConnectorConfig.SRC_INSTANCEID)) {
            keyValue.put(ReplicatorConnectorConfig.SRC_INSTANCEID, config.getString(ReplicatorConnectorConfig.SRC_INSTANCEID));
        }
        keyValue.put(ReplicatorConnectorConfig.SRC_ENDPOINT, config.getString(ReplicatorConnectorConfig.SRC_ENDPOINT));
        keyValue.put(ReplicatorConnectorConfig.SRC_ACL_ENABLE, config.getString(ReplicatorConnectorConfig.SRC_ACL_ENABLE, "false"));
        keyValue.put(ReplicatorConnectorConfig.SRC_ACCESS_KEY, config.getString(ReplicatorConnectorConfig.SRC_ACCESS_KEY, ""));
        keyValue.put(ReplicatorConnectorConfig.SRC_SECRET_KEY, config.getString(ReplicatorConnectorConfig.SRC_SECRET_KEY, ""));
        keyValue.put(ReplicatorConnectorConfig.SRC_TOPICTAGS, config.getString(ReplicatorConnectorConfig.SRC_TOPICTAGS));
        keyValue.put(ReplicatorConnectorConfig.DEST_CLOUD, config.getString(ReplicatorConnectorConfig.DEST_CLOUD));
        keyValue.put(ReplicatorConnectorConfig.DEST_REGION, config.getString(ReplicatorConnectorConfig.DEST_REGION));
        keyValue.put(ReplicatorConnectorConfig.DEST_CLUSTER, config.getString(ReplicatorConnectorConfig.DEST_CLUSTER));
        if (null != config.getString(ReplicatorConnectorConfig.DEST_INSTANCEID)) {
            keyValue.put(ReplicatorConnectorConfig.DEST_INSTANCEID, config.getString(ReplicatorConnectorConfig.DEST_INSTANCEID));
        }
        keyValue.put(ReplicatorConnectorConfig.DEST_ENDPOINT, config.getString(ReplicatorConnectorConfig.DEST_ENDPOINT));
        if (null != config.getString(ReplicatorConnectorConfig.DEST_TOPIC)) {
            keyValue.put(ReplicatorConnectorConfig.DEST_TOPIC, config.getString(ReplicatorConnectorConfig.DEST_TOPIC));
        }
        keyValue.put(ReplicatorConnectorConfig.DEST_ACL_ENABLE, config.getString(ReplicatorConnectorConfig.DEST_ACL_ENABLE, "false"));
        keyValue.put(ReplicatorConnectorConfig.DEST_ACCESS_KEY, config.getString(ReplicatorConnectorConfig.DEST_ACCESS_KEY, ""));
        keyValue.put(ReplicatorConnectorConfig.DEST_SECRET_KEY, config.getString(ReplicatorConnectorConfig.DEST_SECRET_KEY, ""));
        keyValue.put(ReplicatorConnectorConfig.SYNC_GIDS, config.getString(ReplicatorConnectorConfig.SYNC_GIDS));
        configs.add(keyValue);
        return configs;
    }

    @Override
    public Class<? extends Task> taskClass() {
        return ReplicatorCheckpointTask.class;
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
            add(ReplicatorConnectorConfig.SYNC_GIDS);
            add(ReplicatorConnectorConfig.SRC_ACL_ENABLE);
            add(ReplicatorConnectorConfig.DEST_ACL_ENABLE);
        }
    };

    @Override
    public void validate(KeyValue config) {
        if (config.getInt(ConnectorConfig.MAX_TASK, 1) > 1) {
            log.warn("ReplicatorCheckpointConnector no need to set max-task, only used 1.");
        }
        // checkpoint just need only one task.
        config.put(ConnectorConfig.MAX_TASK, 1);
        ReplicatorUtils.checkNeedParams(ReplicatorCheckpointConnector.class.getName(), config, neededParamKeys);

    }

    @Override
    public void start(KeyValue keyValue) {
        this.config = keyValue;
        try {
            buildAndStartTargetMQAdmin();
            createCheckpointTopicIfNotExist();
        } catch (MQClientException e) {
            throw new InitMQClientException("Replicator checkpoint connector init mqAdminClient error.", e);
        }
    }

    @Override
    public void stop() {
        closeAdmin();
    }

    private void buildAndStartTargetMQAdmin() throws MQClientException {
        RPCHook rpcHook = null;
        String destAclEnable = config.getString(ReplicatorConnectorConfig.DEST_ACL_ENABLE, "false");
        if (destAclEnable.equalsIgnoreCase("true")) {
            String destAccessKey = config.getString(ReplicatorConnectorConfig.DEST_ACCESS_KEY);
            String destSecretKey = config.getString(ReplicatorConnectorConfig.DEST_SECRET_KEY);
            if (StringUtils.isNotEmpty(destAccessKey) && StringUtils.isNotEmpty(destSecretKey)) {
                rpcHook = new AclClientRPCHook(new SessionCredentials(destAccessKey, destSecretKey));
            } else {
                rpcHook = new AclClientRPCHook(new SessionCredentials());
            }
        }
        if (null == targetMqAdminExt) {
            targetMqAdminExt = new DefaultMQAdminExt(rpcHook);
            targetMqAdminExt.setNamesrvAddr(config.getString(ReplicatorConnectorConfig.DEST_ENDPOINT));
            targetMqAdminExt.setAdminExtGroup(ReplicatorConnectorConfig.ADMIN_GROUP + "-" + UUID.randomUUID().toString());
            targetMqAdminExt.setInstanceName("ReplicatorCheckpointConnector_InstanceName_" + UUID.randomUUID().toString());
            targetMqAdminExt.start();
        }
    }

    private synchronized void closeAdmin() {
        if (targetMqAdminExt != null) {
            try {
                targetMqAdminExt.shutdown();
            } catch (Throwable e) {
                log.error("close Admin error,", e);
            }
        }
    }

    private void createCheckpointTopicIfNotExist() {
        String checkpointTopic = config.getString(ReplicatorConnectorConfig.CHECKPOINT_TOPIC, ReplicatorConnectorConfig.DEFAULT_CHECKPOINT_TOPIC);
        TopicRouteData topicRouteData = null;
        try {
            topicRouteData = targetMqAdminExt.examineTopicRouteInfo(checkpointTopic);
        } catch (Exception ignored) {
        }
        if (topicRouteData != null && !topicRouteData.getQueueDatas().isEmpty()) {
            return;
        }
        //create target checkpoint topic, todo compact topic
        TopicConfig topicConfig = new TopicConfig();
        topicConfig.setReadQueueNums(8);
        topicConfig.setWriteQueueNums(8);
        int perm = PermName.PERM_READ | PermName.PERM_WRITE;
        topicConfig.setPerm(perm);
        topicConfig.setTopicName(checkpointTopic);
        ReplicatorUtils.createTopic(targetMqAdminExt, topicConfig);
    }
}
