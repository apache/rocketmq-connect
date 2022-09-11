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

package org.apache.rocketmq.connect.runtime.config;

import com.google.common.base.Splitter;
import io.openmessaging.connector.api.errors.ConnectException;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.connect.runtime.common.ConnectKeyValue;
import org.apache.rocketmq.connect.runtime.errors.DeadLetterQueueConfig;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * sink connector config
 */
public class SinkConnectorConfig extends ConnectorConfig {

    public static final String SEMICOLON = ";";


    public static final String CONNECT_TOPICNAMES = "connect.topicnames";

    public static final String TASK_GROUP_ID = "task.group.id";


    /**
     * dead letter queue config
     */
    public static final String DLQ_PREFIX = "errors.deadletterqueue.";
    public static final String DLQ_TOPIC_NAME_CONFIG = DLQ_PREFIX + "topic.name";

    public static final String DLQ_TOPIC_READ_QUEUE_NUMS = DLQ_PREFIX + "read.queue.nums";
    public static final short DLQ_TOPIC_READ_QUEUE_NUMS_DEFAULT = 8;

    public static final String DLQ_TOPIC_WRITE_QUEUE_NUMS = DLQ_PREFIX + "write.queue.nums";
    public static final short DLQ_TOPIC_WRITE_QUEUE_NUMS_DEFAULT = 8;

    public static final String DLQ_CONTEXT_PROPERTIES_ENABLE_CONFIG = DLQ_PREFIX + "context.properties.enable";
    public static final boolean DLQ_CONTEXT_PROPERTIES_ENABLE_DEFAULT = false;

    public SinkConnectorConfig(ConnectKeyValue config) {
        super(config);
    }

    public static MessageQueue parseMessageQueueList(String messageQueueStr) {
        List<String> messageQueueStrList = Splitter.on(SEMICOLON).omitEmptyStrings().trimResults().splitToList(messageQueueStr);
        if (CollectionUtils.isEmpty(messageQueueStrList) || messageQueueStrList.size() != 3) {
            return null;
        }
        return new MessageQueue(messageQueueStrList.get(0), messageQueueStrList.get(1), Integer.valueOf(messageQueueStrList.get(2)));
    }

    public DeadLetterQueueConfig parseDeadLetterQueueConfig() {
        return new DeadLetterQueueConfig(super.config);
    }

    public Set<String> parseTopicList() {
        String messageQueueStr = this.config.getString(SinkConnectorConfig.CONNECT_TOPICNAMES);
        if (StringUtils.isBlank(messageQueueStr)) {
            return null;
        }
        List<String> topicList = Splitter.on(SEMICOLON).omitEmptyStrings().trimResults().splitToList(messageQueueStr);
        return new HashSet<>(topicList);
    }

    /**
     * validate
     */
    @Override
    public void validate() {
        super.validate();
        if (!config.containsKey(CONNECT_TOPICNAMES)) {
            throw new ConnectException("Config connect.topicnames cannot be empty");
        }
    }
}
