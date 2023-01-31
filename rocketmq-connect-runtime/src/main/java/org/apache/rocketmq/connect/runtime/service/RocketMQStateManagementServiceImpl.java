/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rocketmq.connect.runtime.service;

import io.openmessaging.connector.api.data.RecordConverter;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.connect.runtime.config.WorkerConfig;
import org.apache.rocketmq.connect.runtime.store.MemoryBasedKeyValueStore;

/**
 * RocketMQ state management service
 */
public class RocketMQStateManagementServiceImpl extends AbstractStateManagementService {


    /**
     * initialize cb config
     *
     * @param config
     */
    @Override
    public void initialize(WorkerConfig config, RecordConverter converter) {
        super.initialize(config, converter);
        /**connector status store*/
        this.connectorStatusStore = new MemoryBasedKeyValueStore<>();

        /**task status store*/
        this.taskStatusStore = new MemoryBasedKeyValueStore<>();
    }

    @Override
    protected void setEnabledCompactTopic() {
        this.enabledCompactTopic = true;
    }

    /**
     * Start dependent services (if needed)
     */
    @Override
    public void start() {
        dataSynchronizer.start();
    }

    /**
     * Stop dependent services (if needed)
     */
    @Override
    public void stop() {
        dataSynchronizer.stop();
    }

    /**
     * sync send online config
     */
    @Override
    protected void replicaTargetState() {
        // No-op
    }

    @Override
    public void persist() {
        // No-op
    }

    @Override
    protected void process(String key, byte[] value) {
        if (StringUtils.isEmpty(key)) {
            log.error("State change message is illegal, key is empty, the message will be skipped ");
            return;
        }
        if (key.startsWith(CONNECTOR_STATUS_PREFIX)) {
            readConnectorStatus(key, value);
        } else if (key.startsWith(TASK_STATUS_PREFIX)) {
            readTaskStatus(key, value);
        } else {
            log.warn("Discarding record with invalid key {}", key);
        }
    }
}