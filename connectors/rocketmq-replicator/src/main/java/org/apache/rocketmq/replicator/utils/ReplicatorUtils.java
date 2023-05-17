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

package org.apache.rocketmq.replicator.utils;

import io.openmessaging.KeyValue;
import io.openmessaging.connector.api.data.RecordOffset;
import io.openmessaging.connector.api.data.RecordPartition;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.protocol.body.ClusterInfo;
import org.apache.rocketmq.replicator.common.LoggerName;
import org.apache.rocketmq.replicator.exception.ParamInvalidException;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;
import org.apache.rocketmq.tools.command.CommandUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;



public class ReplicatorUtils {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.REPLICATRO_RUNTIME);

    public static final String TOPIC_KEY = "topic";
    public static final String CONSUMER_GROUP_KEY = "consumerGroup";
    public static final String UPSTREAM_LASTTIMESTAMP_KEY = "upstreamLastTimestamp";
    public static final String DOWNSTREAM_LASTTIMESTAMP_KEY = "downstreamLastTimestamp";
    public static final String METADATA_KEY = "metadata";
    public static final String QUEUE_OFFSET = "queueOffset";
    public static final String TOPIC = "topic";

    public static String buildTopicWithNamespace(String topic, String instanceId) {
        if (StringUtils.isBlank(instanceId)) {
            return topic;
        }
        return instanceId + "%" + topic;
    }

    public static String buildConsumergroupWithNamespace(String consumerGroup, String instanceId) {
        if (StringUtils.isBlank(instanceId)) {
            return consumerGroup;
        }
        return instanceId + "%" + consumerGroup;
    }

    public static void checkNeedParams(String connectorName, KeyValue config, Set<String> neededParamKeys) {
        for (String needParamKey : neededParamKeys) {
            checkNeedParamNotEmpty(connectorName, config, needParamKey);
        }
    }

    public static void checkNeedParamNotEmpty(String connectorName, KeyValue config, String needParamKey) {
        if (StringUtils.isEmpty(config.getString(needParamKey, ""))) {
            log.error("Replicator connector " + connectorName + " do not set " + needParamKey);
            throw new ParamInvalidException("Replicator connector " + connectorName + " do not set " + needParamKey);
        }
    }

    public static List sortList(List configs, Comparator comparable) {
        Object[] sortedKv = configs.toArray();
        Arrays.sort(sortedKv, comparable);
        configs = Arrays.asList(sortedKv);
        return configs;
    }

    public static RecordPartition convertToRecordPartition(String topic, String consumerGroup) {
        Map<String, String> map = new HashMap<>();
        map.put(CONSUMER_GROUP_KEY, consumerGroup);
        map.put(TOPIC_KEY, topic);
        RecordPartition recordPartition = new RecordPartition(map);
        return recordPartition;
    }

    public static RecordPartition convertToRecordPartition(String topic, String brokerName, int queueId) {
        Map<String, String> map = new HashMap<>();
        map.put("topic", topic);
        map.put("brokerName", brokerName);
        map.put("queueId", queueId + "");
        RecordPartition recordPartition = new RecordPartition(map);
        return recordPartition;
    }

    public static RecordOffset convertToRecordOffset(Long offset) {
        Map<String, String> offsetMap = new HashMap<>();
        offsetMap.put(QUEUE_OFFSET, offset + "");
        RecordOffset recordOffset = new RecordOffset(offsetMap);
        return recordOffset;
    }

    public static void createTopic(DefaultMQAdminExt defaultMQAdminExt, TopicConfig topicConfig) {
        try {
            ClusterInfo clusterInfo = defaultMQAdminExt.examineBrokerClusterInfo();
            Map<String, Set<String>> clusterAddrTable = clusterInfo.getClusterAddrTable();
            Set<String> clusterNameSet = clusterAddrTable.keySet();
            for (String clusterName : clusterNameSet) {
                Set<String> masterSet = CommandUtil.fetchMasterAddrByClusterName(defaultMQAdminExt, clusterName);
                for (String addr : masterSet) {
                    defaultMQAdminExt.createAndUpdateTopicConfig(addr, topicConfig);
                    log.info("CreateTopic cluster {}, addr {}, masterSet {}, create topic {} success. ", clusterName, addr, masterSet, topicConfig);
                }
            }
        } catch (Exception e) {
            throw new RuntimeException("Create topic [" + topicConfig.getTopicName() + "] failed", e);
        }
    }

}
