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

/**
 * dead letter queue config
 */
public class DeadLetterQueueConfig {
    /**
     * dead letter queue config
     */
    public static final String DLQ_PREFIX = "errors.deadletterqueue.";
    public static final String DLQ_TOPIC_NAME_CONFIG = DLQ_PREFIX + "topic.name";


    public static final String DLQ_TOPIC_READ_QUEUE_NUMS = DLQ_PREFIX + "read.queue.nums";
    public static final short DLQ_TOPIC_READ_QUEUE_NUMS_DEFAULT = 16;

    public static final String DLQ_TOPIC_WRITE_QUEUE_NUMS = DLQ_PREFIX + "write.queue.nums";
    public static final short DLQ_TOPIC_WRITE_QUEUE_NUMS_DEFAULT = 16;

    public static final String DLQ_CONTEXT_HEADERS_ENABLE_CONFIG = DLQ_PREFIX + "context.headers.enable";
    public static final boolean DLQ_CONTEXT_HEADERS_ENABLE_DEFAULT = false;

    public static final String ERRORS_LOG_INCLUDE_MESSAGES_CONFIG = "errors.log.include.messages";
    public static final boolean ERRORS_LOG_INCLUDE_MESSAGES_DEFAULT = false;

    public static final String ERRORS_LOG_ENABLE_CONFIG = "errors.log.enable";
    public static final boolean ERRORS_LOG_ENABLE_DEFAULT = false;


    public static final String ERRORS_RETRY_TIMEOUT_CONFIG = "errors.retry.timeout";
    public static final int ERRORS_RETRY_TIMEOUT_DEFAULT = 0;

    public static final String ERRORS_RETRY_MAX_DELAY_CONFIG = "errors.retry.delay.max.ms";
    public static final String ERRORS_RETRY_MAX_DELAY_DISPLAY = "Maximum Delay Between Retries for Errors";
    public static final int ERRORS_RETRY_MAX_DELAY_DEFAULT = 60000;

    public static final String ERRORS_TOLERANCE_CONFIG = "errors.tolerance";
    public static final ToleranceType ERRORS_TOLERANCE_DEFAULT = ToleranceType.NONE;

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
        return config.getProperties().containsKey(DLQ_CONTEXT_HEADERS_ENABLE_CONFIG) ?
                Boolean.valueOf(config.getProperties().get(DLQ_CONTEXT_HEADERS_ENABLE_CONFIG)) : DLQ_CONTEXT_HEADERS_ENABLE_DEFAULT;
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
        return config.getLong(ERRORS_RETRY_TIMEOUT_CONFIG);
    }

    public long errorMaxDelayInMillis() {
        return config.getLong(ERRORS_RETRY_MAX_DELAY_CONFIG);
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
