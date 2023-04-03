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

package org.apache.rocketmq.connect.runtime.connectorwrapper;

import com.alibaba.fastjson.JSON;
import io.openmessaging.KeyValue;
import io.openmessaging.connector.api.component.task.sink.ErrorRecordReporter;
import io.openmessaging.connector.api.component.task.sink.SinkTaskContext;
import io.openmessaging.connector.api.data.RecordOffset;
import io.openmessaging.connector.api.data.RecordPartition;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.client.consumer.DefaultLitePullConsumer;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.connect.runtime.common.ConnectKeyValue;
import org.apache.rocketmq.connect.runtime.common.LoggerName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static org.apache.rocketmq.connect.runtime.connectorwrapper.WorkerSinkTask.BROKER_NAME;
import static org.apache.rocketmq.connect.runtime.connectorwrapper.WorkerSinkTask.QUEUE_ID;
import static org.apache.rocketmq.connect.runtime.connectorwrapper.WorkerSinkTask.QUEUE_OFFSET;
import static org.apache.rocketmq.connect.runtime.connectorwrapper.WorkerSinkTask.TOPIC;

/**
 * worker sink task context
 */
public class WorkerSinkTaskContext implements SinkTaskContext {

    private static final Logger log = LoggerFactory.getLogger(LoggerName.ROCKETMQ_RUNTIME);
    /**
     * The configs of current sink task.
     */
    private final ConnectKeyValue taskConfig;
    private final Map<MessageQueue, Long> messageQueuesOffset;

    private final Set<MessageQueue> pausedQueues;

    private final WorkerSinkTask workerSinkTask;

    private final DefaultLitePullConsumer consumer;

    public WorkerSinkTaskContext(ConnectKeyValue taskConfig, WorkerSinkTask workerSinkTask, DefaultLitePullConsumer consumer) {
        this.taskConfig = taskConfig;
        this.workerSinkTask = workerSinkTask;
        this.consumer = consumer;
        this.pausedQueues = new HashSet<>();
        this.messageQueuesOffset = new ConcurrentHashMap<>();
    }

    public Set<MessageQueue> getPausedQueues() {
        return pausedQueues;
    }

    @Override
    public ErrorRecordReporter errorRecordReporter() {
        return workerSinkTask.errorRecordReporter();
    }

    @Override
    public void resetOffset(RecordPartition recordPartition, RecordOffset recordOffset) {
        if (null == recordPartition || null == recordPartition.getPartition() || null == recordOffset || null == recordOffset.getOffset()) {
            log.warn("recordPartition {} info is null or recordOffset {} info is null", recordPartition, recordOffset);
            return;
        }
        String brokerName = (String) recordPartition.getPartition().get(BROKER_NAME);
        String topic = (String) recordPartition.getPartition().get(TOPIC);
        Integer queueId = Integer.valueOf((String) recordPartition.getPartition().get(QUEUE_ID));
        if (StringUtils.isEmpty(brokerName) || StringUtils.isEmpty(topic) || null == queueId) {
            log.warn("brokerName is null or queueId is null or queueName is null, brokerName {}, queueId {} queueId {}", brokerName, queueId, topic);
            return;
        }
        MessageQueue messageQueue = new MessageQueue(topic, brokerName, queueId);
        Long offset = Long.valueOf((String) recordOffset.getOffset().get(QUEUE_OFFSET));
        if (null == offset) {
            log.warn("resetOffset, offset is null");
            return;
        }
        messageQueuesOffset.put(messageQueue, offset);
    }

    @Override
    public void resetOffset(Map<RecordPartition, RecordOffset> offsets) {
        if (MapUtils.isEmpty(offsets)) {
            log.warn("resetOffset, offsets {} is null", offsets);
            return;
        }
        for (Map.Entry<RecordPartition, RecordOffset> entry : offsets.entrySet()) {
            if (null == entry || null == entry.getKey() || null == entry.getKey().getPartition() || null == entry.getValue() || null == entry.getValue().getOffset()) {
                log.warn("recordPartition {} info is null or recordOffset {} info is null, entry {}", entry);
                continue;
            }
            RecordPartition recordPartition = entry.getKey();
            String brokerName = (String) recordPartition.getPartition().get(BROKER_NAME);
            String topic = (String) recordPartition.getPartition().get(TOPIC);
            Integer queueId = Integer.valueOf((String) recordPartition.getPartition().get(QUEUE_ID));
            if (StringUtils.isEmpty(brokerName) || StringUtils.isEmpty(topic) || null == queueId) {
                log.warn("brokerName is null or queueId is null or queueName is null, brokerName {}, queueId {} queueId {}", brokerName, queueId, topic);
                continue;
            }
            MessageQueue messageQueue = new MessageQueue(topic, brokerName, queueId);
            RecordOffset recordOffset = entry.getValue();
            Long offset = Long.valueOf((String) recordOffset.getOffset().get(QUEUE_OFFSET));
            if (null == offset) {
                log.warn("resetOffset, offset is null");
                continue;
            }
            messageQueuesOffset.put(messageQueue, offset);
        }
    }

    @Override
    public void pause(List<RecordPartition> recordPartitions) {
        if (recordPartitions == null || recordPartitions.size() == 0) {
            log.warn("recordPartitions is null or recordPartitions.size() is zero. recordPartitions {}", JSON.toJSONString(recordPartitions));
            return;
        }
        Set<MessageQueue> queues = new HashSet<>();
        for (RecordPartition recordPartition : recordPartitions) {
            if (null == recordPartition || null == recordPartition.getPartition()) {
                log.warn("recordPartition {} info is null", recordPartition);
                continue;
            }
            String brokerName = (String) recordPartition.getPartition().get(BROKER_NAME);
            String topic = (String) recordPartition.getPartition().get(TOPIC);
            Integer queueId = Integer.valueOf((String) recordPartition.getPartition().get(QUEUE_ID));
            if (StringUtils.isEmpty(brokerName) || StringUtils.isEmpty(topic) || null == queueId) {
                log.warn("brokerName is null or queueId is null or queueName is null, brokerName {}, queueId {} queueId {}", brokerName, queueId, topic);
                continue;
            }
            MessageQueue messageQueue = new MessageQueue(topic, brokerName, queueId);
            queues.add(messageQueue);
        }

        pausedQueues.addAll(queues);
        if (workerSinkTask.shouldPause()) {
            log.debug("{} Connector is paused, so not pausing consumer's partitions {}", this, queues);
        } else {
            consumer.pause(queues);
            log.debug("{} Pausing partitions {}. Connector is not paused.", this, queues);
        }
    }

    @Override
    public void resume(List<RecordPartition> recordPartitions) {
        if (recordPartitions == null || recordPartitions.size() == 0) {
            log.warn("recordPartitions is null or recordPartitions.size() is zero. recordPartitions {}", JSON.toJSONString(recordPartitions));
            return;
        }
        Set<MessageQueue> queues = new HashSet<>();
        for (RecordPartition recordPartition : recordPartitions) {
            if (null == recordPartition || null == recordPartition.getPartition()) {
                log.warn("recordPartition {} info is null", recordPartition);
                continue;
            }
            String brokerName = (String) recordPartition.getPartition().get(BROKER_NAME);
            String topic = (String) recordPartition.getPartition().get(TOPIC);
            Integer queueId = Integer.valueOf((String) recordPartition.getPartition().get(QUEUE_ID));
            if (StringUtils.isEmpty(brokerName) || StringUtils.isEmpty(topic) || null == queueId) {
                log.warn("brokerName is null or queueId is null or queueName is null, brokerName {}, queueId {} queueId {}", brokerName, queueId, topic);
                continue;
            }
            MessageQueue messageQueue = new MessageQueue(topic, brokerName, queueId);
            queues.add(messageQueue);
        }
        pausedQueues.removeAll(queues);
        if (workerSinkTask.shouldPause()) {
            log.debug("{} Connector is paused, so not resuming consumer's partitions {}", this, queues);
        } else {
            consumer.resume(queues);
            log.debug("{} Resuming partitions: {}", this, queues);
        }
    }

    @Override
    public Set<RecordPartition> assignment() {
        return this.workerSinkTask.getRecordPartitions();
    }

    @Override
    public String getConnectorName() {
        return workerSinkTask.id().connector();
    }

    @Override
    public String getTaskName() {
        return workerSinkTask.id().toString();
    }

    /**
     * Get the configurations of current task.
     *
     * @return the configuration of current task.
     */
    @Override
    public KeyValue configs() {
        return taskConfig;
    }

    public Map<MessageQueue, Long> queuesOffsets() {
        return this.messageQueuesOffset;
    }

    public void cleanQueuesOffsets() {
        this.messageQueuesOffset.clear();
    }


}
