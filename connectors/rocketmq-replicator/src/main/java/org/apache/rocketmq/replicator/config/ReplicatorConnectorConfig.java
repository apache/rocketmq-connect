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

package org.apache.rocketmq.replicator.config;

import com.google.common.base.Splitter;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class ReplicatorConnectorConfig {
    private Log log = LogFactory.getLog(ReplicatorConnectorConfig.class);
    // replicator task id
    private String taskId;
    // connector id
    private String connectorId;
    // replicator task id with index for max-task, format replicatorTaskId-i
//    private String replicatorTaskIdWithIndex;
    // src & dest
    private String srcCloud;
    private String srcRegion;
    private String srcCluster;
    private String srcInstanceId;
    private String srcTopicTags; // format topic-1,tag-a;topic-2,tag-b;topic-3,tag-c
    private String srcEndpoint;
    private boolean srcAclEnable;
    private boolean autoCreateInnerConsumergroup;
    private String srcAccessKey;
    private String srcSecretKey;
    private String destCloud;
    private String destRegion;
    private String destCluster;
    private String destInstanceId;
    private String destTopic;
    private String destEndpoint;
    private boolean destAclEnable;
    private String destAccessKey;
    private String destSecretKey;
    // filter properties
    private String filterProperties;
    private Map<String, String> filterPropertiesMap = new HashMap<>();
    private String filterPropertiesOperator = "and";
    // consume from where
    private ConsumeFromWhere consumeFromWhere = ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET;
    // consume from timestamp
    private long consumeFromTimestamp = System.currentTimeMillis();
    // source task replicate to mq failover strategy
    private FailoverStrategy failoverStrategy = FailoverStrategy.DISMISS;
    private boolean enableHeartbeat = true;
    private boolean enableCheckpoint = true;
    private boolean enableRetrySync = true;
    // srcRetryGids used for retry sync,
    private String srcRetryGids;
    // destRetryTopic used for store messages from %RETRY%srcRetryGids, can set normal topic or retry topic
    private String destRetryTopic;
    private boolean enableDlqSync = true;
    // srcDlqGids used for dlq sync
    private String srcDlqGids;
    // destDlqTopic used for store messages from DLQ, can set normal topic
    private String destDlqTopic;
    // specify needed to sync %RETRY%GID, GID format is instanceId%Gid
    // if not set, sync all online gids
    private String syncGids;
    private String dividedNormalQueues;
    private String dividedRetryQueues;
    private String dividedDlqQueues;
    // tps limit
    private int syncTps = 1000;
    private int maxTask = 2;
    //
    private int heartbeatIntervalMs = 1000;
    private int checkpointIntervalMs = 10 * 1000;
    private long commitOffsetIntervalMs = 10 * 1000;
    private String heartbeatTopic;
    private String checkpointTopic;
    public final static String DEFAULT_HEARTBEAT_TOPIC = "replicator_heartbeat";
    public final static String DEFAULT_CHECKPOINT_TOPIC = "replicator_checkpoint";
    public final static String TOPIC_TAG_SPLITTER = ",";
    public final static String TOPIC_SPLITTER = ";";
    public final static String TASK_NAME_SPLITTER = "_";
    public final static String TASK_LINK_SPLITTER = "-";
    public final static String TOPIC_TAG_FROMAT_SPLITTER = "_";
    public final static String GID_SPLITTER = ",";
    public final static String ADMIN_GROUP = "replicator_admin_group";
    public final static int DEFAULT_SYNC_TPS = 1000;
    public final static int DEFAULT_MAX_TASK = 2;

    // poll config
    private int eachQueueBufferSize = 1000;
    private int pullMaxNum = 100;

    // converter
    private String sourceConverter;

//    public final static String REPLICATOR_TASK_ID_WITH_INDEX = "replicator-subtask-id";
    public final static String SRC_CLOUD = "src.cloud";
    public final static String SRC_REGION = "src.region";
    public final static String SRC_CLUSTER = "src.cluster";
    public final static String SRC_INSTANCEID = "src.instanceid";
    public final static String SRC_TOPICTAGS = "src.topictags";
    public final static String SRC_ENDPOINT = "src.endpoint";
    public final static String SRC_ACL_ENABLE = "src.acl.enable";
    public final static String SRC_ACCESS_KEY = "src.access.key";
    public final static String SRC_SECRET_KEY = "src.secret.key";
    public final static String DEST_CLOUD = "dest.cloud";
    public final static String DEST_REGION = "dest.region";
    public final static String DEST_CLUSTER = "dest.cluster";
    public final static String DEST_INSTANCEID = "dest.instanceid";
    public final static String DEST_TOPIC = "dest.topic";
    public final static String DEST_ENDPOINT = "dest.endpoint";
    public final static String DEST_ACL_ENABLE = "dest.acl.enable";
    public final static String AUTO_CREATE_INNER_CONSUMERGROUP = "auto.create.inner.consumergroup";
    public final static String DEST_ACCESS_KEY = "dest.access.key";
    public final static String DEST_SECRET_KEY = "dest.secret.key";
    public final static String FILTER_PROPERTIES = "filter.properties";// pro1,pro2 or  pro1=val1,prop2=val2
    public final static String FILTER_PROPERTIES_OPERATOR = "filter.properties.operator";// and or
    public final static String FAILOVER_STRATEGY = "failover.strategy";
    public final static String ENABLE_HEARTBEAT = "enable.heartbeat";
    public final static String ENABLE_CHECKPOINT = "enable.checkpoint";
    public final static String ENABLE_RETRY_SYNC = "enable.retrysync";
    public final static String SRC_RETRY_GIDS = "src.retrygids";
    public final static String DEST_RETRY_TOPIC = "dest.retrytopic";
    public final static String ENABLE_DLQ_SYNC = "enable.dlqsync";
    public final static String SRC_DLQ_GIDS = "dest.dlqgids";
    public final static String DEST_DLQ_TOPIC = "dest.dqltopic";
    public final static String SYNC_GIDS = "sync.gids";
    public final static String HEARTBEAT_INTERVALS_MS = "heartbeat.interval.ms";
    public final static String CHECKPOINT_INTERVAL_MS = "checkpoint.interval.ms";
    public final static String CHECKPOINT_TOPIC = "checkpoint.topic";
    public final static String HEARTBEAT_TOPIC = "heartbeat.topic";
    public final static String EACH_QUEUE_BUFFER_SIZE = "each.queue.buffer.size";
    public final static String CONSUME_FROM_WHERE = "consumefromwhere";
    public final static String CONSUME_FROM_TIMESTAMP = "consumefromtimestamp";
    public final static String DIVIDE_STRATEGY = "divide.strategy";
    public final static String DIVIDED_NORMAL_QUEUES = "divided.normalqueues";
    public final static String DIVIDED_RETRY_QUEUES = "divided.retryqueues";
    public final static String DIVIDED_DLQ_QUEUES = "divided.dlqqueues";
    public final static String SYNC_TPS = "sync.tps";
    public final static String MAX_TASK = "max.tasks";
    public final static String COMMIT_OFFSET_INTERVALS_MS = "commit.offset.interval.ms";
    public final static String REQUEST_TASK_RECONFIG_INTERVAL_MS = "request.task.reconfig.ms";

    public static final String CONNECT_TIMESTAMP = "connect.timestamp";

    public static final String CONNECT_TOPICNAME = "connect.topicname";


    public String getTaskId() {
        return taskId;
    }

    public void setTaskId(String taskId) {
        this.taskId = taskId;
    }

    public String getConnectorId() {
        return connectorId;
    }

    public void setConnectorId(String connectorId) {
        this.connectorId = connectorId;
    }

    public String getSrcCloud() {
        return srcCloud;
    }

    public void setSrcCloud(String srcCloud) {
        this.srcCloud = srcCloud;
    }

    public String getSrcRegion() {
        return srcRegion;
    }

    public void setSrcRegion(String srcRegion) {
        this.srcRegion = srcRegion;
    }

    public String getSrcCluster() {
        return srcCluster;
    }

    public void setSrcCluster(String srcCluster) {
        this.srcCluster = srcCluster;
    }

    public String generateFullSourceTopicTags() {
        return this.srcInstanceId + "%" + this.srcTopicTags;
    }

    public String getSrcInstanceId() {
        return srcInstanceId;
    }

    public void setSrcInstanceId(String srcInstanceId) {
        this.srcInstanceId = srcInstanceId;
    }

    public static Map<String, String> getSrcTopicTagMap(String srcInstanceId, String srcTopicTags) {
        if (StringUtils.isEmpty(srcTopicTags)) {
            return null;
        }
        List<String> topicTagList = Splitter.on(TOPIC_SPLITTER).omitEmptyStrings().trimResults().splitToList(srcTopicTags);
        Map<String, String> topicTagMap = new HashMap<>(8);
        for (String topicTagPair : topicTagList) {
            List<String> topicAndTag = Splitter.on(TOPIC_TAG_SPLITTER).omitEmptyStrings().trimResults().splitToList(topicTagPair);
            if (topicAndTag.size() == 1) {
                if (StringUtils.isBlank(srcInstanceId)) {
                    topicTagMap.put(topicAndTag.get(0), "*");
                } else if (topicTagPair.startsWith("%RETRY%") || topicTagPair.startsWith("%DLQ%")) {
                    topicTagMap.put(topicAndTag.get(0), "*");
                } else {
                    topicTagMap.put(srcInstanceId + "%" + topicAndTag.get(0), "*");
                }
            } else {
                if (StringUtils.isBlank(srcInstanceId)) {
                    topicTagMap.put(topicAndTag.get(0), topicAndTag.get(1));
                } else {
                    topicTagMap.put(srcInstanceId + "%" + topicAndTag.get(0), topicAndTag.get(1));
                }
            }
        }
        return topicTagMap;
    }

    public String getSrcTopicTags() {
        return srcTopicTags;
    }

    public void setSrcTopicTags(String srcTopicTags) {
        this.srcTopicTags = srcTopicTags;
    }

    public String getSrcEndpoint() {
        return srcEndpoint;
    }

    public void setSrcEndpoint(String srcEndpoint) {
        this.srcEndpoint = srcEndpoint;
    }

    public String getDestCloud() {
        return destCloud;
    }

    public void setDestCloud(String destCloud) {
        this.destCloud = destCloud;
    }

    public String getDestRegion() {
        return destRegion;
    }

    public void setDestRegion(String destRegion) {
        this.destRegion = destRegion;
    }

    public String getDestCluster() {
        return destCluster;
    }

    public void setDestCluster(String destCluster) {
        this.destCluster = destCluster;
    }

    public String getDestInstanceId() {
        return destInstanceId;
    }

    public void setDestInstanceId(String destInstanceId) {
        this.destInstanceId = destInstanceId;
    }

    public String getDestTopic() {
        return destTopic;
    }

    public void setDestTopic(String destTopic) {
        this.destTopic = destTopic;
    }

    public String getDestEndpoint() {
        return destEndpoint;
    }

    public void setDestEndpoint(String destEndpoint) {
        this.destEndpoint = destEndpoint;
    }

    public String getFilterProperties() {
        return filterProperties;
    }

    public Map<String, String> getFilterPropertiesMap() {
        return this.filterPropertiesMap;
    }

    public void setFilterProperties(String filterProperties) {
        this.filterProperties = filterProperties;
        if (StringUtils.isNotBlank(filterProperties)) {
            String[] kvs = filterProperties.split(",");
            for (String kv : kvs) {
                String[] items = kv.split("=");
                if (items.length == 1) {
                    filterPropertiesMap.put(items[0], null);
                } else if (items.length == 2) {
                    filterPropertiesMap.put(items[0], items[1]);
                } else {
                    log.error("parse filterProperties error");
                }
            }
        }
    }

    public String getFilterPropertiesOperator() {
        return filterPropertiesOperator;
    }

    public void setFilterPropertiesOperator(String filterPropertiesOperator) {
        this.filterPropertiesOperator = filterPropertiesOperator;
    }

    public FailoverStrategy getFailoverStrategy() {
        return failoverStrategy;
    }

    public void setFailoverStrategy(FailoverStrategy failoverStrategy) {
        this.failoverStrategy = failoverStrategy;
    }

    public boolean isEnableHeartbeat() {
        return enableHeartbeat;
    }

    public void setEnableHeartbeat(boolean enableHeartbeat) {
        this.enableHeartbeat = enableHeartbeat;
    }

    public boolean isEnableCheckpoint() {
        return enableCheckpoint;
    }

    public void setEnableCheckpoint(boolean enableCheckpoint) {
        this.enableCheckpoint = enableCheckpoint;
    }

    public boolean isEnableRetrySync() {
        return enableRetrySync;
    }

    public void setEnableRetrySync(boolean enableRetrySync) {
        this.enableRetrySync = enableRetrySync;
    }

    public String getSrcRetryGids() {
        return srcRetryGids;
    }

    public void setSrcRetryGids(String srcRetryGids) {
        this.srcRetryGids = srcRetryGids;
    }

    public String getDestRetryTopic() {
        return destRetryTopic;
    }

    public void setDestRetryTopic(String destRetryTopic) {
        this.destRetryTopic = destRetryTopic;
    }

    public boolean isEnableDlqSync() {
        return enableDlqSync;
    }

    public void setEnableDlqSync(boolean enableDlqSync) {
        this.enableDlqSync = enableDlqSync;
    }

    public String getSrcDlqGids() {
        return srcDlqGids;
    }

    public void setSrcDlqGids(String srcDlqGids) {
        this.srcDlqGids = srcDlqGids;
    }

    public String getDestDlqTopic() {
        return destDlqTopic;
    }

    public void setDestDlqTopic(String destDlqTopic) {
        this.destDlqTopic = destDlqTopic;
    }

    public String getSyncGids() {
        return syncGids;
    }

    public void setSyncGids(String syncGids) {
        this.syncGids = syncGids;
    }

    public String getDividedNormalQueues() {
        return dividedNormalQueues;
    }

    public void setDividedNormalQueues(String dividedNormalQueues) {
        this.dividedNormalQueues = dividedNormalQueues;
    }

    public String getDividedRetryQueues() {
        return dividedRetryQueues;
    }

    public void setDividedRetryQueues(String dividedRetryQueues) {
        this.dividedRetryQueues = dividedRetryQueues;
    }

    public String getDividedDlqQueues() {
        return dividedDlqQueues;
    }

    public void setDividedDlqQueues(String dividedDlqQueues) {
        this.dividedDlqQueues = dividedDlqQueues;
    }

    public int getHeartbeatIntervalMs() {
        return heartbeatIntervalMs;
    }

    public void setHeartbeatIntervalMs(int heartbeatIntervalMs) {
        this.heartbeatIntervalMs = heartbeatIntervalMs;
    }

    public int getCheckpointIntervalMs() {
        return checkpointIntervalMs;
    }

    public void setCheckpointIntervalMs(int checkpointIntervalMs) {
        this.checkpointIntervalMs = checkpointIntervalMs;
    }

    public String getHeartbeatTopic() {
        return heartbeatTopic;
    }

    public void setHeartbeatTopic(String heartbeatTopic) {
        this.heartbeatTopic = heartbeatTopic;
    }

    public String getCheckpointTopic() {
        return checkpointTopic;
    }

    public void setCheckpointTopic(String checkpointTopic) {
        this.checkpointTopic = checkpointTopic;
    }

    public int getEachQueueBufferSize() {
        return eachQueueBufferSize;
    }

    public void setEachQueueBufferSize(int eachQueueBufferSize) {
        this.eachQueueBufferSize = eachQueueBufferSize;
    }

    public ConsumeFromWhere getConsumeFromWhere() {
        return consumeFromWhere;
    }

    public void setConsumeFromWhere(String consumeFromWhereValue) {
        ConsumeFromWhere consumeFromWhere = ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET;
        try {
            consumeFromWhere = ConsumeFromWhere.valueOf(consumeFromWhereValue);
        } catch (Exception e) {
            log.warn("parse consumeFromWhere [" + consumeFromWhereValue + "] error, use CONSUME_FROM_MAX_OFFSET");
        }
        this.consumeFromWhere = consumeFromWhere;
    }

    public long getConsumeFromTimestamp() {
        return consumeFromTimestamp;
    }

    public void setConsumeFromTimestamp(long consumeFromTimestamp) {
        this.consumeFromTimestamp = consumeFromTimestamp;
    }

    public int getSyncTps() {
        return syncTps;
    }

    public void setSyncTps(int syncTps) {
        this.syncTps = syncTps;
    }

    public int getMaxTask() {
        return maxTask;
    }

    public void setMaxTask(int maxTask) {
        this.maxTask = maxTask;
    }

    public int getPullMaxNum() {
        return pullMaxNum;
    }

    public void setPullMaxNum(int pullMaxNum) {
        this.pullMaxNum = pullMaxNum;
    }

    public static final String SYSTEM_CONSUMER_PREFIX = "CID_RMQ_SYS_REPLICATOR_";
    public String generateTaskIdWithIndexAsConsumerGroup() {
        // todo need use replicatorTaskIdWithIndex for consumerGroup ???
        return SYSTEM_CONSUMER_PREFIX + connectorId;
//        return "SYS_ROCKETMQ_REPLICATOR_" + connectorId + "_" + taskId;
    }

    public String generateSourceString() {
        return this.srcCloud + "_" + this.srcCluster + "_" + this.srcRegion;
    }

    public String generateDestinationString() {
        return this.destCloud + "_" + this.destCluster + "_" + this.destRegion;
    }

    public String getSourceConverter() {
        return sourceConverter;
    }

    public void setSourceConverter(String sourceConverter) {
        this.sourceConverter = sourceConverter;
    }

    public boolean isSrcAclEnable() {
        return srcAclEnable;
    }

    public void setSrcAclEnable(boolean srcAclEnable) {
        this.srcAclEnable = srcAclEnable;
    }

    public String getSrcAccessKey() {
        return srcAccessKey;
    }

    public void setSrcAccessKey(String srcAccessKey) {
        this.srcAccessKey = srcAccessKey;
    }

    public String getSrcSecretKey() {
        return srcSecretKey;
    }

    public void setSrcSecretKey(String srcSecretKey) {
        this.srcSecretKey = srcSecretKey;
    }

    public boolean isDestAclEnable() {
        return destAclEnable;
    }

    public void setDestAclEnable(boolean destAclEnable) {
        this.destAclEnable = destAclEnable;
    }

    public String getDestAccessKey() {
        return destAccessKey;
    }

    public void setDestAccessKey(String destAccessKey) {
        this.destAccessKey = destAccessKey;
    }

    public String getDestSecretKey() {
        return destSecretKey;
    }

    public void setDestSecretKey(String destSecretKey) {
        this.destSecretKey = destSecretKey;
    }

    public boolean isAutoCreateInnerConsumergroup() {
        return autoCreateInnerConsumergroup;
    }

    public void setAutoCreateInnerConsumergroup(boolean autoCreateInnerConsumergroup) {
        this.autoCreateInnerConsumergroup = autoCreateInnerConsumergroup;
    }

    public void setConsumeFromWhere(ConsumeFromWhere consumeFromWhere) {
        this.consumeFromWhere = consumeFromWhere;
    }

    public long getCommitOffsetIntervalMs() {
        return commitOffsetIntervalMs;
    }

    public void setCommitOffsetIntervalMs(long commitOffsetIntervalMs) {
        this.commitOffsetIntervalMs = commitOffsetIntervalMs;
    }

    @Override
    public String toString() {
        return "ReplicatorConnectorConfig{" +
                "taskId='" + taskId + '\'' +
                ", connectorId='" + connectorId + '\'' +
                ", srcCloud='" + srcCloud + '\'' +
                ", srcRegion='" + srcRegion + '\'' +
                ", srcCluster='" + srcCluster + '\'' +
                ", srcInstanceId='" + srcInstanceId + '\'' +
                ", srcTopicTags='" + srcTopicTags + '\'' +
                ", srcEndpoint='" + srcEndpoint + '\'' +
                ", srcAclEnable=" + srcAclEnable +
                ", srcAccessKey='" + srcAccessKey + '\'' +
                ", srcSecretKey='" + srcSecretKey + '\'' +
                ", destCloud='" + destCloud + '\'' +
                ", destRegion='" + destRegion + '\'' +
                ", destCluster='" + destCluster + '\'' +
                ", destInstanceId='" + destInstanceId + '\'' +
                ", destTopic='" + destTopic + '\'' +
                ", destEndpoint='" + destEndpoint + '\'' +
                ", destAclEnable=" + destAclEnable +
                ", destAccessKey='" + destAccessKey + '\'' +
                ", destSecretKey='" + destSecretKey + '\'' +
                ", consumeFromWhere=" + consumeFromWhere +
                ", consumeFromTimestamp=" + consumeFromTimestamp +
                ", failoverStrategy=" + failoverStrategy +
                ", enableHeartbeat=" + enableHeartbeat +
                ", enableCheckpoint=" + enableCheckpoint +
                ", enableRetrySync=" + enableRetrySync +
                ", srcRetryGids='" + srcRetryGids + '\'' +
                ", destRetryTopic='" + destRetryTopic + '\'' +
                ", enableDlqSync=" + enableDlqSync +
                ", srcDlqGids='" + srcDlqGids + '\'' +
                ", destDlqTopic='" + destDlqTopic + '\'' +
                ", syncGids='" + syncGids + '\'' +
                ", dividedNormalQueues='" + dividedNormalQueues + '\'' +
                ", dividedRetryQueues='" + dividedRetryQueues + '\'' +
                ", dividedDlqQueues='" + dividedDlqQueues + '\'' +
                ", syncTps=" + syncTps +
                ", heartbeatIntervalMs=" + heartbeatIntervalMs +
                ", checkpointIntervalMs=" + checkpointIntervalMs +
                ", commitOffsetIntervalMs=" + commitOffsetIntervalMs +
                ", heartbeatTopic='" + heartbeatTopic + '\'' +
                ", checkpointTopic='" + checkpointTopic + '\'' +
                ", eachQueueBufferSize=" + eachQueueBufferSize +
                ", pullMaxNum=" + pullMaxNum +
                ", sourceConverter='" + sourceConverter + '\'' +
                '}';
    }
}
