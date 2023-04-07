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
import io.openmessaging.connector.api.component.task.source.SourceTask;
import io.openmessaging.connector.api.data.ConnectRecord;
import io.openmessaging.connector.api.data.RecordOffset;
import io.openmessaging.connector.api.data.RecordPartition;
import io.openmessaging.connector.api.data.Schema;
import io.openmessaging.connector.api.data.SchemaBuilder;
import io.openmessaging.connector.api.data.Struct;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.acl.common.AclClientRPCHook;
import org.apache.rocketmq.acl.common.SessionCredentials;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.admin.ConsumeStats;
import org.apache.rocketmq.common.admin.OffsetWrapper;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.connect.runtime.common.LoggerName;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.apache.rocketmq.replicator.config.ReplicatorConnectorConfig;
import org.apache.rocketmq.replicator.exception.InitMQClientException;
import org.apache.rocketmq.replicator.utils.ReplicatorUtils;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static org.apache.rocketmq.replicator.utils.ReplicatorUtils.CONSUMER_GROUP_KEY;
import static org.apache.rocketmq.replicator.utils.ReplicatorUtils.DOWNSTREAM_LASTTIMESTAMP_KEY;
import static org.apache.rocketmq.replicator.utils.ReplicatorUtils.METADATA_KEY;
import static org.apache.rocketmq.replicator.utils.ReplicatorUtils.TOPIC_KEY;
import static org.apache.rocketmq.replicator.utils.ReplicatorUtils.UPSTREAM_LASTTIMESTAMP_KEY;

public class ReplicatorCheckpointTask extends SourceTask {

    private static final Logger log = LoggerFactory.getLogger(LoggerName.REPLICATRO_RUNTIME);
    private ReplicatorConnectorConfig connectorConfig = new ReplicatorConnectorConfig();
    private long lastCheckPointTimestamp = System.currentTimeMillis();
    private DefaultMQAdminExt srcMqAdminExt;
    private DefaultMQAdminExt targetMqAdminExt;

    public static final Schema VALUE_SCHEMA_V0 = SchemaBuilder.struct()
        .field(CONSUMER_GROUP_KEY, SchemaBuilder.string().build())
        .field(TOPIC_KEY, SchemaBuilder.string().build())
        .field(UPSTREAM_LASTTIMESTAMP_KEY, SchemaBuilder.int64().build())
        .field(DOWNSTREAM_LASTTIMESTAMP_KEY, SchemaBuilder.int64().build())
        .field(METADATA_KEY, SchemaBuilder.string().build())
        .build();

    public static final Schema KEY_SCHEMA = SchemaBuilder.struct()
        .field(CONSUMER_GROUP_KEY, SchemaBuilder.string().build())
        .field(TOPIC_KEY, SchemaBuilder.string().build())
        .build();

    private void buildMqAdmin() throws MQClientException {
        buildAndStartSrcMQAdmin();
        buildAndStartTargetMQAdmin();
    }

    private void buildAndStartSrcMQAdmin() throws MQClientException {
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
        srcMqAdminExt = new DefaultMQAdminExt(rpcHook);
        srcMqAdminExt.setNamesrvAddr(connectorConfig.getSrcEndpoint());
        srcMqAdminExt.setAdminExtGroup(ReplicatorConnectorConfig.ADMIN_GROUP + "-" + UUID.randomUUID().toString());
        srcMqAdminExt.setInstanceName(connectorConfig.generateSourceString() + "-" + UUID.randomUUID().toString());
        try {
            srcMqAdminExt.start();
        } catch (Exception e) {
            log.error("ReplicatorCheckpoint init src mqadmin error,", e);
            throw e;
        }
    }

    private void buildAndStartTargetMQAdmin() throws MQClientException {
        RPCHook rpcHook = null;
        if (connectorConfig.isDestAclEnable()) {
            if (StringUtils.isNotEmpty(connectorConfig.getDestAccessKey()) && StringUtils.isNotEmpty(connectorConfig.getDestSecretKey())) {
                String destAccessKey = connectorConfig.getDestAccessKey();
                String destSecretKey = connectorConfig.getDestSecretKey();
                rpcHook = new AclClientRPCHook(new SessionCredentials(destAccessKey, destSecretKey));
            } else {
                rpcHook = new AclClientRPCHook(new SessionCredentials());
            }
        }
        targetMqAdminExt = new DefaultMQAdminExt(rpcHook);
        targetMqAdminExt.setNamesrvAddr(connectorConfig.getDestEndpoint());
        targetMqAdminExt.setAdminExtGroup(ReplicatorConnectorConfig.ADMIN_GROUP + "-" + UUID.randomUUID().toString());
        targetMqAdminExt.setInstanceName(connectorConfig.generateDestinationString() + "-" + UUID.randomUUID().toString());
        try {
            targetMqAdminExt.start();
        } catch (Exception e) {
            log.error("ReplicatorCheckpoint init target mqadmin error,", e);
            throw e;
        }
    }

    @Override
    public List<ConnectRecord> poll() throws InterruptedException {
        if (lastCheckPointTimestamp + connectorConfig.getCheckpointIntervalMs() > System.currentTimeMillis()) {
            log.info("sleep " + lastCheckPointTimestamp + ", " + connectorConfig.getCheckpointIntervalMs() + ", " + System.currentTimeMillis());
            Thread.sleep(connectorConfig.getCheckpointIntervalMs() - System.currentTimeMillis() + lastCheckPointTimestamp + 100);
            return null;
        }
        log.info("not sleep " + lastCheckPointTimestamp + ", " + connectorConfig.getCheckpointIntervalMs() + ", " + System.currentTimeMillis());
        List<ConnectRecord> connectRecords = new LinkedList<>();
        // pull consumer group's consumer commit offset
        String syncGids = connectorConfig.getSyncGids();
        if (StringUtils.isEmpty(syncGids)) {
            lastCheckPointTimestamp = System.currentTimeMillis();
            return null;
        }
        Set<String> srcTopics = ReplicatorConnectorConfig.getSrcTopicTagMap(connectorConfig.getSrcInstanceId(), connectorConfig.getSrcTopicTags()).keySet();
        try {
            String[] syncGidArr = syncGids.split(connectorConfig.GID_SPLITTER);
            for (String consumerGroup : syncGidArr) {
                for (String srcTopic : srcTopics) {
                    String srcTopicWithInstanceId = ReplicatorUtils.buildTopicWithNamespace(srcTopic, connectorConfig.getSrcInstanceId());
                    String srcConsumerGroupWithInstanceId = ReplicatorUtils.buildConsumergroupWithNamespace(consumerGroup, connectorConfig.getSrcInstanceId());
                    try {
                        ConsumeStats srcConsumeStats = srcMqAdminExt.examineConsumeStats(srcConsumerGroupWithInstanceId, srcTopicWithInstanceId);
                        long minSrcLasttimestamp = getMinSrcLasttimestamp(srcConsumeStats);

                        String targetTopic = connectorConfig.getDestTopic();
                        String targetTopicWithInstanceId;
                        if (StringUtils.isEmpty(targetTopic) || StringUtils.isBlank(targetTopic)) {
                            targetTopicWithInstanceId = ReplicatorUtils.buildTopicWithNamespace(srcTopic, connectorConfig.getDestInstanceId());
                        } else {
                            targetTopicWithInstanceId = ReplicatorUtils.buildTopicWithNamespace(targetTopic, connectorConfig.getDestInstanceId());
                        }
                        ConsumeStats targetConsumeStats = targetMqAdminExt.examineConsumeStats(consumerGroup, targetTopicWithInstanceId);
                        long minDestLasttimestamp = getMinSrcLasttimestamp(targetConsumeStats);

                        RecordPartition recordPartition = ReplicatorUtils.convertToRecordPartition(srcTopic, consumerGroup);
                        RecordOffset recordOffset = ReplicatorUtils.convertToRecordOffset(0L);
                        ConnectRecord connectRecord = new ConnectRecord(recordPartition, recordOffset, System.currentTimeMillis());
                        Struct keyStruct = buildCheckpointKey(srcTopicWithInstanceId, srcConsumerGroupWithInstanceId);
                        Struct valueStruct = buildCheckpointPayload(srcTopicWithInstanceId, srcConsumerGroupWithInstanceId, minSrcLasttimestamp, minDestLasttimestamp);
                        connectRecord.setKeySchema(KEY_SCHEMA);
                        connectRecord.setKey(keyStruct);
                        connectRecord.setSchema(VALUE_SCHEMA_V0);
                        connectRecord.setData(valueStruct);
                        connectRecord.addExtension(TOPIC_KEY, connectorConfig.getCheckpointTopic());
                        connectRecords.add(connectRecord);
                    } catch (Exception e) {
                        log.error("examineConsumeStats gid : " + consumerGroup + ", topic : " + srcTopic + " error", e);
                    }
                }
            }
        } catch (Exception e) {
            log.error("get syncGids committed offset error, syncGids : " + syncGids, e);
        }
        //
        lastCheckPointTimestamp = System.currentTimeMillis();
        return connectRecords;
    }

    @NotNull
    private static Struct buildCheckpointPayload(String srcTopicWithInstanceId, String srcConsumerGroupWithInstanceId,
        long minSrcLasttimestamp, long minDestLasttimestamp) {
        Struct struct = new Struct(VALUE_SCHEMA_V0);
        struct.put(CONSUMER_GROUP_KEY, srcConsumerGroupWithInstanceId);
        struct.put(TOPIC_KEY, srcTopicWithInstanceId);
        struct.put(UPSTREAM_LASTTIMESTAMP_KEY, minSrcLasttimestamp);
        struct.put(DOWNSTREAM_LASTTIMESTAMP_KEY, minDestLasttimestamp);
        struct.put(METADATA_KEY, String.valueOf(System.currentTimeMillis()));
        return struct;
    }

    private static Struct buildCheckpointKey(String srcTopicWithInstanceId, String srcConsumerGroupWithInstanceId) {
        Struct struct = new Struct(KEY_SCHEMA);
        struct.put(CONSUMER_GROUP_KEY, srcConsumerGroupWithInstanceId);
        struct.put(TOPIC_KEY, srcTopicWithInstanceId);
        return struct;
    }

    private long getMinSrcLasttimestamp(
        ConsumeStats consumeStats) throws RemotingException, MQClientException, InterruptedException, MQBrokerException {
        long minSrcLasttimestamp = -1;
        if (null != consumeStats) {
            Map<MessageQueue, OffsetWrapper> offsetTable = consumeStats.getOffsetTable();
            if (null != offsetTable) {
                for (Map.Entry<MessageQueue, OffsetWrapper> entry : offsetTable.entrySet()) {
                    OffsetWrapper offsetWrapper = entry.getValue();
                    if (null == offsetWrapper) {
                        continue;
                    }
                    long lastTimestamp = offsetWrapper.getLastTimestamp();
                    if (minSrcLasttimestamp == -1) {
                        minSrcLasttimestamp = lastTimestamp;
                    } else if (minSrcLasttimestamp > lastTimestamp) {
                        minSrcLasttimestamp = lastTimestamp;
                    }
                }
            }
        }
        return minSrcLasttimestamp;
    }

    @Override
    public void validate(KeyValue config) {

    }

    @Override
    public void start(KeyValue config) {
        log.info("ReplicatorCheckpointTask init " + config);
        log.info("sourceTaskContextConfigs : " + sourceTaskContext.configs());
        fillConnectorConfig(config);
        // init mqadmin client
        try {
            buildMqAdmin();
        } catch (Exception e) {
            cleanResource();
            log.error("buildMqAdmin error,", e);
            throw new InitMQClientException("Replicator checkpoint task init mqAdminClient error.", e);
        }
    }

    private void fillConnectorConfig(KeyValue config) {
        // build connectConfig
        connectorConfig.setSrcCloud(config.getString(connectorConfig.SRC_CLOUD));
        connectorConfig.setSrcRegion(config.getString(connectorConfig.SRC_REGION));
        connectorConfig.setSrcCluster(config.getString(connectorConfig.SRC_CLUSTER));
        connectorConfig.setSrcInstanceId(config.getString(connectorConfig.SRC_INSTANCEID));
        connectorConfig.setSrcEndpoint(config.getString(connectorConfig.SRC_ENDPOINT));
        connectorConfig.setSrcTopicTags(config.getString(connectorConfig.getSrcTopicTags()));
        connectorConfig.setDestCloud(config.getString(connectorConfig.DEST_CLOUD));
        connectorConfig.setDestRegion(config.getString(connectorConfig.DEST_REGION));
        connectorConfig.setDestCluster(config.getString(connectorConfig.DEST_CLUSTER));
        connectorConfig.setDestInstanceId(config.getString(connectorConfig.DEST_INSTANCEID));
        connectorConfig.setDestEndpoint(config.getString(connectorConfig.DEST_ENDPOINT));
        connectorConfig.setDestTopic(config.getString(connectorConfig.DEST_TOPIC));
        connectorConfig.setDestAclEnable(Boolean.valueOf(config.getString(ReplicatorConnectorConfig.DEST_ACL_ENABLE, "true")));
        connectorConfig.setSrcAclEnable(Boolean.valueOf(config.getString(ReplicatorConnectorConfig.SRC_ACL_ENABLE, "true")));
        connectorConfig.setCheckpointIntervalMs(config.getInt(connectorConfig.CHECKPOINT_INTERVAL_MS, connectorConfig.getCheckpointIntervalMs()));
        connectorConfig.setSyncGids(config.getString(connectorConfig.SYNC_GIDS));
        connectorConfig.setCheckpointTopic(config.getString(connectorConfig.CHECKPOINT_TOPIC, connectorConfig.DEFAULT_CHECKPOINT_TOPIC));
        log.info("ReplicatorCheckpointTask connectorConfig : " + connectorConfig);
    }

    private void cleanResource() {
        try {
            if (srcMqAdminExt != null) {
                srcMqAdminExt.shutdown();
            }
            if (targetMqAdminExt != null) {
                targetMqAdminExt.shutdown();
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
