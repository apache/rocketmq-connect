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

import io.openmessaging.connector.api.data.ConnectRecord;
import io.openmessaging.connector.api.errors.ConnectException;
import org.apache.rocketmq.connect.runtime.common.ConnectKeyValue;
import org.apache.rocketmq.connect.runtime.errors.ToleranceType;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Define keys for connector and task configs.
 */
public class ConnectorConfig {
    /**
     * The full class name of a specific connector implements.
     */
    public static final String CONNECTOR_CLASS = "connector.class";
    public static final String TASK_CLASS = "task.class";
    public static final String CONNECTOR_ID = "connector.id";

    public static final String TASK_ID = "task.id";
    public static final String MAX_TASK = "max.tasks";
    public static final int TASKS_MAX_DEFAULT = 1;
    public static final String TASK_TYPE = "task.type";
    public static final String CONNECTOR_DIRECT_ENABLE = "connector.direct.enable";
    public static final String SOURCE_TASK_CLASS = "source.task.class";
    public static final String SINK_TASK_CLASS = "sink.task.class";
    /**
     * The full class name of record converter. Which is used to parse {@link ConnectRecord} to/from byte[].
     */
    public static final String VALUE_CONVERTER = "value.converter";
    public static final String KEY_CONVERTER = "key.converter";
    public static final String IS_KEY = "isKey";
    public static final String TRANSFORMS = "transforms";
    public static final String CONNECT_TIMESTAMP = "connect.timestamp";
    public static final String CONNECT_SCHEMA = "connect.schema";
    public static final String HASH_FUNC = "consistentHashFunc";
    public static final String VIRTUAL_NODE = "virtualNode";
    public static final String ERRORS_LOG_INCLUDE_MESSAGES_CONFIG = "errors.log.include.messages";
    public static final boolean ERRORS_LOG_INCLUDE_MESSAGES_DEFAULT = false;
    public static final String ERRORS_LOG_ENABLE_CONFIG = "errors.log.enable";
    public static final boolean ERRORS_LOG_ENABLE_DEFAULT = false;
    // The default is 0, which means no retries will be attempted. Use -1 for infinite retries.
    public static final String ERRORS_RETRY_TIMEOUT_CONFIG = "errors.retry.timeout";
    public static final int ERRORS_RETRY_TIMEOUT_DEFAULT = 0;
    public static final String ERRORS_RETRY_MAX_DELAY_CONFIG = "errors.retry.delay.max.ms";
    public static final int ERRORS_RETRY_MAX_DELAY_DEFAULT = 60000;
    public static final String ERRORS_TOLERANCE_CONFIG = "errors.tolerance";
    public static final ToleranceType ERRORS_TOLERANCE_DEFAULT = ToleranceType.NONE;
    /**
     * The required key for all configurations.
     */
    public static final Set<String> REQUEST_CONFIG = new HashSet<String>() {
        {
            add(CONNECTOR_CLASS);
        }
    };
    /**
     * Maximum allowed message size in bytes, the default value is 4M.
     */
    public static final int MAX_MESSAGE_SIZE = Integer.parseInt(System.getProperty("rocketmq.runtime.max.message.size", "4194304"));

    public static final String CONSUME_FROM_WHERE = "consume.from.where";

    protected ConnectKeyValue config;

    public ConnectorConfig(ConnectKeyValue config) {
        this.config = config;
        validate();
    }

    /**
     * original config
     *
     * @return
     */
    public Map<String, String> originalConfig() {
        return config.getProperties();
    }

    /**
     * validate
     */
    public void validate() {
        if (!config.containsKey(CONNECTOR_CLASS)) {
            throw new ConnectException("Config connector.class cannot be empty");
        }
    }
}
