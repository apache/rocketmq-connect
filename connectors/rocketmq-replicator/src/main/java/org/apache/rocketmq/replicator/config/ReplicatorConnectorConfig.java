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
package org.apache.rocketmq.replicator.config;

import com.google.common.base.Splitter;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;


/**
 * @author osgoo
 * @date 2022/6/16
 */
public class ReplicatorConnectorConfig {
    private Log log = LogFactory.getLog(ReplicatorConnectorConfig.class);
    // replicator task id
    private String taskId;
    // connector id
    private String connectorId;
    // src & dest
    private String srcCloud;
    private String srcRegion;
    private String srcCluster;
    private String srcInstanceId;
    private String srcTopicTags; // format topic-1,tag-a;topic-2,tag-b;topic-3,tag-c
    private String srcEndpoint;
    private boolean srcAclEnable;
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
    // consume from where
    private ConsumeFromWhere consumeFromWhere = ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET;
    // consume from timestamp
    private long consumeFromTimestamp = System.currentTimeMillis();
    // sourcetask replicate to mq failover strategy
    private FailoverStrategy failoverStrategy = FailoverStrategy.DISMISS;
    private boolean enableHeartbeat = true;

    private String dividedNormalQueues;
    // tps limit
    private int syncTps = 1000;
    //
    private int heartbeatIntervalMs = 1 * 1000;
    private String heartbeatTopic;
    public final static String DEFAULT_HEARTBEAT_TOPIC = "replicator_heartbeat";
    public final static String TOPIC_TAG_SPLITTER = ",";
    public final static String TOPIC_SPLITTER = ";";
    public final static String TASK_NAME_SPLITTER = "_";
    public final static String TASK_LINK_SPLITTER = "-";
    public final static String TOPIC_TAG_FROMAT_SPLITTER = "_";
    public final static String GID_SPLITTER = ",";
    public final static String ADMIN_GROUP = "replicator_admin_group";
    public final static int DEFAULT_SYNC_TPS = 1000;

    // poll config
    private int eachQueueBufferSize = 1000;
    private int pullMaxNum = 100;

    // converter
    private String sourceConverter;

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
    public final static String DEST_ACCESS_KEY = "dest.access.key";
    public final static String DEST_SECRET_KEY = "dest.secret.key";
    public final static String FAILOVER_STRATEGY = "failover.strategy";
    public final static String ENABLE_HEARTBEAT = "enable.heartbeat";
    public final static String ENABLE_CHECKPOINT = "enable.checkpoint";
    public final static String HEARTBEAT_INTERVALS_MS = "heartbeat.interval.ms";
    public final static String HEARTBEAT_TOPIC = "heartbeat.topic";
    public final static String CONSUME_FROM_WHERE = "consumefromwhere";
    public final static String CONSUME_FROM_TIMESTAMP = "consumefromtimestamp";
    public final static String DIVIDE_STRATEGY = "divide.strategy";
    public final static String DIVIDED_NORMAL_QUEUES = "divided.normalqueues";
    public final static String SYNC_TPS = "sync.tps";

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


    public String getDividedNormalQueues() {
        return dividedNormalQueues;
    }

    public void setDividedNormalQueues(String dividedNormalQueues) {
        this.dividedNormalQueues = dividedNormalQueues;
    }

    public int getHeartbeatIntervalMs() {
        return heartbeatIntervalMs;
    }

    public void setHeartbeatIntervalMs(int heartbeatIntervalMs) {
        this.heartbeatIntervalMs = heartbeatIntervalMs;
    }

    public String getHeartbeatTopic() {
        return heartbeatTopic;
    }

    public void setHeartbeatTopic(String heartbeatTopic) {
        this.heartbeatTopic = heartbeatTopic;
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

    public int getPullMaxNum() {
        return pullMaxNum;
    }

    public void setPullMaxNum(int pullMaxNum) {
        this.pullMaxNum = pullMaxNum;
    }

    public String generateTaskIdWithIndexAsConsumerGroup() {
        // todo need use replicatorTaskIdWithIndex for consumerGroup ???
        return "SYS_ROCKETMQ_REPLICATOR_" + connectorId;
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
                ", dividedNormalQueues='" + dividedNormalQueues + '\'' +
                ", syncTps=" + syncTps +
                ", heartbeatIntervalMs=" + heartbeatIntervalMs +
                ", heartbeatTopic='" + heartbeatTopic + '\'' +
                ", eachQueueBufferSize=" + eachQueueBufferSize +
                ", pullMaxNum=" + pullMaxNum +
                ", sourceConverter='" + sourceConverter + '\'' +
                '}';
    }
}
