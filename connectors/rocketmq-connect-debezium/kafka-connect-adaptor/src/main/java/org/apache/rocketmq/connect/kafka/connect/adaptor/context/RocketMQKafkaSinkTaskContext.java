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
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.rocketmq.connect.kafka.connect.adaptor.context;

import io.openmessaging.connector.api.data.RecordOffset;
import io.openmessaging.connector.api.data.RecordPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.ErrantRecordReporter;
import org.apache.kafka.connect.sink.SinkTaskContext;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * rocketmq kafka sink task context
 */
public class RocketMQKafkaSinkTaskContext implements SinkTaskContext {
    public static final String BROKER_NAME = "brokerName";
    public static final String QUEUE_ID = "queueId";
    public static final String TOPIC = "topic";
    public static final String QUEUE_OFFSET = "queueOffset";

    private final io.openmessaging.connector.api.component.task.sink.SinkTaskContext sinkContext;
    private final Map<String, String> taskConfig;
    private final ErrantRecordReporter errantRecordReporter;

    public RocketMQKafkaSinkTaskContext(io.openmessaging.connector.api.component.task.sink.SinkTaskContext context, Map<String, String> taskConfig) {
        this.sinkContext = context;
        this.taskConfig = taskConfig;
        this.errantRecordReporter = new RocketMQKafkaErrantRecordReporter(context);
    }

    @Override
    public Map<String, String> configs() {
        return taskConfig;
    }

    @Override
    public void offset(Map<TopicPartition, Long> map) {
        Map<RecordPartition, RecordOffset> offsets = new HashMap<>();
        map.forEach((topicPartition, offset) -> {
            offsets.put(convertToRecordPartition(topicPartition), convertToRecordOffset(offset));
        });
        sinkContext.resetOffset(offsets);
    }

    @Override
    public void offset(TopicPartition topicPartition, long l) {
        sinkContext.resetOffset(convertToRecordPartition(topicPartition), convertToRecordOffset(l));
    }

    @Override
    public void timeout(long l) {

    }

    @Override
    public Set<TopicPartition> assignment() {
        Set<TopicPartition> topicPartitions = new HashSet<>();
        Set<RecordPartition> recordPartitions = sinkContext.assignment();
        recordPartitions.forEach(partition -> {
            topicPartitions.add(convertToTopicPartition(partition.getPartition()));
        });
        return topicPartitions;
    }

    @Override
    public void pause(TopicPartition... topicPartitions) {
        List<RecordPartition> partitions = new ArrayList<>();
        for (TopicPartition topicPartition : topicPartitions) {
            RecordPartition recordPartition = convertToRecordPartition(topicPartition);
            if (recordPartition != null) {
                partitions.add(recordPartition);
            }
        }
        sinkContext.pause(partitions);
    }

    @Override
    public void resume(TopicPartition... topicPartitions) {
        List<RecordPartition> partitions = new ArrayList<>();
        for (TopicPartition topicPartition : topicPartitions) {
            RecordPartition recordPartition = convertToRecordPartition(topicPartition);
            if (recordPartition != null) {
                partitions.add(recordPartition);
            }
        }
        sinkContext.resume(partitions);
    }

    @Override
    public void requestCommit() {
    }


    @Override
    public ErrantRecordReporter errantRecordReporter() {
        return errantRecordReporter;
    }


    /**
     * convert to kafka topic partition
     *
     * @param partitionMap
     * @return
     */
    public TopicPartition convertToTopicPartition(Map<String, ?> partitionMap) {
        if (partitionMap.containsKey(TOPIC) && partitionMap.containsKey(QUEUE_ID)) {
            return new TopicPartition(partitionMap.get(TOPIC).toString(), Integer.valueOf(partitionMap.get(QUEUE_ID).toString()));
        }
        return null;
    }

    /**
     * convert to rocketmq record partition
     *
     * @param topicPartition
     * @return
     */
    public RecordPartition convertToRecordPartition(TopicPartition topicPartition) {
        if (topicPartition != null) {
            return new RecordPartition(Collections.singletonMap(topicPartition.topic(), topicPartition.partition()));
        }
        return null;
    }

    private RecordOffset convertToRecordOffset(long l) {
        return new RecordOffset(Collections.singletonMap(QUEUE_OFFSET, l));
    }


}
