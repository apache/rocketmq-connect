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

package org.apache.rocketmq.connect.runtime.errors;

import org.apache.rocketmq.connect.runtime.common.ConnectKeyValue;

import static org.apache.rocketmq.connect.runtime.config.ConnectorConfig.ERRORS_LOG_ENABLE_CONFIG;
import static org.apache.rocketmq.connect.runtime.config.ConnectorConfig.ERRORS_LOG_ENABLE_DEFAULT;
import static org.apache.rocketmq.connect.runtime.config.ConnectorConfig.ERRORS_LOG_INCLUDE_MESSAGES_CONFIG;
import static org.apache.rocketmq.connect.runtime.config.ConnectorConfig.ERRORS_LOG_INCLUDE_MESSAGES_DEFAULT;
import static org.apache.rocketmq.connect.runtime.config.ConnectorConfig.ERRORS_RETRY_MAX_DELAY_CONFIG;
import static org.apache.rocketmq.connect.runtime.config.ConnectorConfig.ERRORS_RETRY_MAX_DELAY_DEFAULT;
import static org.apache.rocketmq.connect.runtime.config.ConnectorConfig.ERRORS_RETRY_TIMEOUT_CONFIG;
import static org.apache.rocketmq.connect.runtime.config.ConnectorConfig.ERRORS_RETRY_TIMEOUT_DEFAULT;
import static org.apache.rocketmq.connect.runtime.config.ConnectorConfig.ERRORS_TOLERANCE_CONFIG;
import static org.apache.rocketmq.connect.runtime.config.ConnectorConfig.ERRORS_TOLERANCE_DEFAULT;
import static org.apache.rocketmq.connect.runtime.config.SinkConnectorConfig.DLQ_CONTEXT_PROPERTIES_ENABLE_CONFIG;
import static org.apache.rocketmq.connect.runtime.config.SinkConnectorConfig.DLQ_CONTEXT_PROPERTIES_ENABLE_DEFAULT;
import static org.apache.rocketmq.connect.runtime.config.SinkConnectorConfig.DLQ_TOPIC_NAME_CONFIG;
import static org.apache.rocketmq.connect.runtime.config.SinkConnectorConfig.DLQ_TOPIC_READ_QUEUE_NUMS;
import static org.apache.rocketmq.connect.runtime.config.SinkConnectorConfig.DLQ_TOPIC_READ_QUEUE_NUMS_DEFAULT;
import static org.apache.rocketmq.connect.runtime.config.SinkConnectorConfig.DLQ_TOPIC_WRITE_QUEUE_NUMS;
import static org.apache.rocketmq.connect.runtime.config.SinkConnectorConfig.DLQ_TOPIC_WRITE_QUEUE_NUMS_DEFAULT;

/**
 * dead letter queue config
 */
public class DeadLetterQueueConfig {

    private ConnectKeyValue config;

    /**
     * config
     *
     * @param config
     */
    public DeadLetterQueueConfig(ConnectKeyValue config) {
        this.config = config;
    }


    /**
     * get dlq topic name
     *
     * @return
     */
    public String dlqTopicName() {
        return config.getString(DLQ_TOPIC_NAME_CONFIG, "");
    }


    /**
     * get dlq context headers enabled
     *
     * @return
     */
    public Boolean isDlqContextHeadersEnabled() {
        return config.getProperties().containsKey(DLQ_CONTEXT_PROPERTIES_ENABLE_CONFIG) ?
                Boolean.valueOf(config.getProperties().get(DLQ_CONTEXT_PROPERTIES_ENABLE_CONFIG)) : DLQ_CONTEXT_PROPERTIES_ENABLE_DEFAULT;
    }


    /**
     * get dlq topic read queue nums
     *
     * @return
     */
    public Integer dlqTopicReadQueueNums() {
        return config.getInt(DLQ_TOPIC_READ_QUEUE_NUMS, DLQ_TOPIC_READ_QUEUE_NUMS_DEFAULT);

    }

    /**
     * get dlq topic write queue nums
     *
     * @return
     */
    public Integer dlqTopicWriteQueueNums() {
        return config.getInt(DLQ_TOPIC_WRITE_QUEUE_NUMS, DLQ_TOPIC_WRITE_QUEUE_NUMS_DEFAULT);
    }

    /**
     * include error log
     *
     * @return
     */
    public boolean includeRecordDetailsInErrorLog() {
        return config.getProperties().containsKey(ERRORS_LOG_INCLUDE_MESSAGES_CONFIG) ?
                Boolean.valueOf(config.getProperties().get(ERRORS_LOG_INCLUDE_MESSAGES_CONFIG)) : ERRORS_LOG_INCLUDE_MESSAGES_DEFAULT;
    }


    public boolean enableErrantRecordReporter() {
        String dqlTopic = dlqTopicName();
        boolean enableErrorLog = enableErrorLog();
        boolean enableDqlTopic = !dqlTopic.isEmpty();
        return enableErrorLog || enableDqlTopic;
    }

    private boolean enableErrorLog() {
        return config.getProperties().containsKey(ERRORS_LOG_ENABLE_CONFIG) ?
                Boolean.valueOf(config.getProperties().get(ERRORS_LOG_ENABLE_CONFIG)) : ERRORS_LOG_ENABLE_DEFAULT;

    }

    public long errorRetryTimeout() {
        return config.getLong(ERRORS_RETRY_TIMEOUT_CONFIG, ERRORS_RETRY_TIMEOUT_DEFAULT);
    }

    public long errorMaxDelayInMillis() {
        return config.getLong(ERRORS_RETRY_MAX_DELAY_CONFIG, ERRORS_RETRY_MAX_DELAY_DEFAULT);
    }

    public ToleranceType errorToleranceType() {
        String tolerance = config.getString(ERRORS_TOLERANCE_CONFIG);
        for (ToleranceType type : ToleranceType.values()) {
            if (type.name().equalsIgnoreCase(tolerance)) {
                return type;
            }
        }
        return ERRORS_TOLERANCE_DEFAULT;
    }
}
