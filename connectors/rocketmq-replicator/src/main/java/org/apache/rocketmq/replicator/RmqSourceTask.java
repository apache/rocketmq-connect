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
import com.alibaba.fastjson.JSONObject;
import io.openmessaging.KeyValue;
import io.openmessaging.connector.api.component.task.source.SourceTask;
import io.openmessaging.connector.api.component.task.source.SourceTaskContext;
import io.openmessaging.connector.api.data.ConnectRecord;
import io.openmessaging.connector.api.data.Field;
import io.openmessaging.connector.api.data.FieldType;
import io.openmessaging.connector.api.data.RecordOffset;
import io.openmessaging.connector.api.data.Schema;
import io.openmessaging.connector.api.data.SchemaBuilder;
import io.openmessaging.connector.api.storage.OffsetStorageReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.acl.common.AclClientRPCHook;
import org.apache.rocketmq.acl.common.SessionCredentials;
import org.apache.rocketmq.client.consumer.DefaultMQPullConsumer;
import org.apache.rocketmq.client.consumer.PullResult;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.replicator.common.Utils;
import org.apache.rocketmq.replicator.config.ConfigUtil;
import org.apache.rocketmq.replicator.config.DataType;
import org.apache.rocketmq.replicator.config.TaskConfig;
import org.apache.rocketmq.replicator.config.TaskTopicInfo;
import org.apache.rocketmq.replicator.schema.FieldName;
import org.apache.rocketmq.replicator.schema.SchemaEnum;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RmqSourceTask extends SourceTask {

    private static final Logger log = LoggerFactory.getLogger(RmqSourceTask.class);

    private final String taskId;
    private final TaskConfig config;
    private DefaultMQPullConsumer consumer;
    private volatile boolean started = false;
    private final long TIMEOUT = 1000 * 60 * 10;
    private final long WAIT_TIME = 1000 * 2;

    private Map<TaskTopicInfo, Long> mqOffsetMap;

    public RmqSourceTask() {
        this.config = new TaskConfig();
        this.taskId = Utils.createTaskId(Thread.currentThread().getName());
        mqOffsetMap = new HashMap<>();
    }

    @Override
    public List<ConnectRecord> poll() {

        if (this.config.getDataType() == DataType.COMMON_MESSAGE.ordinal()) {
            return pollCommonMessage();
        } else if (this.config.getDataType() == DataType.TOPIC_CONFIG.ordinal()) {
            return pollTopicConfig();
        } else if (this.config.getDataType() == DataType.BROKER_CONFIG.ordinal()) {
            return pollBrokerConfig();
        } else {
            return pollSubConfig();
        }
    }

    @Override
    public void start(KeyValue config) {
        ConfigUtil.load(config, this.config);
        RPCHook rpcHook = null;
        if (this.config.isSrcAclEnable()) {
            rpcHook = new AclClientRPCHook(new SessionCredentials(this.config.getSrcAccessKey(), this.config.getSrcSecretKey()));
        }
        this.consumer = new DefaultMQPullConsumer(rpcHook);
        this.consumer.setConsumerGroup(this.taskId);
        this.consumer.setNamesrvAddr(this.config.getSourceRocketmq());
        this.consumer.setInstanceName(Utils.createInstanceName(this.config.getSourceRocketmq()));
        List<TaskTopicInfo> topicList = JSONObject.parseArray(this.config.getTaskTopicList(), TaskTopicInfo.class);

        try {
            if (topicList == null) {
                throw new IllegalStateException("topicList is null");
            }

            this.consumer.start();

            List<TaskTopicInfo> topicListFilter = new ArrayList<>();
            for (TaskTopicInfo tti : topicList) {
                Set<MessageQueue> mqs = consumer.fetchSubscribeMessageQueues(tti.getTopic());
                for (MessageQueue mq : mqs) {
                    if (tti.getBrokerName().equals(mq.getBrokerName()) && tti.getQueueId() == mq.getQueueId()) {
                        topicListFilter.add(tti);
                        break;
                    }
                }
            }
            OffsetStorageReader offsetStorageReader = this.sourceTaskContext.offsetStorageReader();
            mqOffsetMap.putAll(getPositionMapWithCheck(topicListFilter, offsetStorageReader, this.TIMEOUT, TimeUnit.MILLISECONDS));
            started = true;
        } catch (Exception e) {
            log.error("Consumer of task {} start failed.", this.taskId, e);
            throw new IllegalStateException(String.format("Consumer of task %s start failed.", this.taskId));
        }
        log.info("RocketMQ source task started");
    }

    @Override
    public void stop() {

        if (started) {
            if (this.consumer != null) {
                this.consumer.shutdown();
            }
            started = false;
        }
    }

    private List<ConnectRecord> pollCommonMessage() {

        List<ConnectRecord> res = new ArrayList<>();
        if (started) {
            try {
                for (TaskTopicInfo taskTopicConfig : this.mqOffsetMap.keySet()) {
                    PullResult pullResult = consumer.pull(taskTopicConfig, "*",
                        this.mqOffsetMap.get(taskTopicConfig), 32);
                    switch (pullResult.getPullStatus()) {
                        case OFFSET_ILLEGAL: {
                            if (this.mqOffsetMap.get(taskTopicConfig) < pullResult.getNextBeginOffset()) {
                                this.mqOffsetMap.put(taskTopicConfig, pullResult.getNextBeginOffset());
                            }
                            break;
                        }
                        case FOUND: {
                            this.mqOffsetMap.put(taskTopicConfig, pullResult.getNextBeginOffset());
                            List<MessageExt> msgs = pullResult.getMsgFoundList();
                            for (MessageExt msg : msgs) {
                                ConnectRecord connectRecord = new ConnectRecord(
                                        Utils.offsetKey(taskTopicConfig),
                                        Utils.offsetValue(pullResult.getNextBeginOffset()),
                                        System.currentTimeMillis(),
                                        SchemaBuilder.string().name( FieldName.COMMON_MESSAGE.getKey()).build(),
                                        new String(msg.getBody(), StandardCharsets.UTF_8)
                                );
                                final Map<String, String> properties = msg.getProperties();
                                final Set<String> keys = properties.keySet();
                                keys.forEach(key -> connectRecord.addExtension(key, properties.get(key)));
                                res.add(connectRecord);
                            }
                            break;
                        }
                        default:
                            break;
                    }
                }
            } catch (Exception e) {
                log.error("Rocketmq replicator task poll error, current config: {}", JSON.toJSONString(config), e);
            }
        } else {
            if (System.currentTimeMillis() % 1000 == 0) {
                log.warn("Rocketmq replicator task is not started.");
            }
        }
        return res;
    }

    private List<ConnectRecord> pollTopicConfig() {
        DefaultMQAdminExt srcMQAdminExt;
        return new ArrayList<>();
    }

    private List<ConnectRecord> pollBrokerConfig() {
        return new ArrayList<>();
    }

    private List<ConnectRecord> pollSubConfig() {
        return new ArrayList<>();
    }

    public Map<TaskTopicInfo, Long> getPositionMapWithCheck(List<TaskTopicInfo> taskList,
        OffsetStorageReader positionStorageReader, long timeout, TimeUnit unit) {
        unit = unit == null ? TimeUnit.MILLISECONDS : unit;

        Map<TaskTopicInfo, Long> positionMap = getPositionMap(taskList, positionStorageReader);

        long msecs = unit.toMillis(timeout);
        long startTime = msecs <= 0L ? 0L : System.currentTimeMillis();
        long waitTime;
        boolean waitPositionReady;
        do {
            try {
                Thread.sleep(this.WAIT_TIME);
            } catch (InterruptedException e) {
                log.error("Thread sleep error.", e);
            }

            Map<TaskTopicInfo, Long> positionMapCmp = getPositionMap(taskList, positionStorageReader);
            waitPositionReady = true;
            for (Map.Entry<TaskTopicInfo, Long> positionEntry : positionMap.entrySet()) {
                if (positionMapCmp.getOrDefault(positionEntry.getKey(), 0L) != positionEntry.getValue().longValue()) {
                    waitPositionReady = false;
                    positionMap = positionMapCmp;
                    break;
                }
            }

            waitTime = msecs - (System.currentTimeMillis() - startTime);
        }
        while (!waitPositionReady && waitTime > 0L);

        return positionMap;
    }

    public Map<TaskTopicInfo, Long> getPositionMap(List<TaskTopicInfo> taskList,
        OffsetStorageReader offsetStorageReader) {
        Map<TaskTopicInfo, Long> positionMap = new HashMap<>();
        for (TaskTopicInfo tti : taskList) {
            RecordOffset positionInfo = offsetStorageReader.readOffset(Utils.offsetKey(tti));
            if (positionInfo != null && null != positionInfo.getOffset()) {
                Map<String, ?> offset = positionInfo.getOffset();
                Object lastRecordedOffset = offset.get(RmqConstants.NEXT_POSITION);
                long skipLeft = Long.parseLong(String.valueOf(lastRecordedOffset));
                positionMap.put(tti, skipLeft);
            } else {
                positionMap.put(tti, 0L);
            }
        }

        return positionMap;
    }

}

