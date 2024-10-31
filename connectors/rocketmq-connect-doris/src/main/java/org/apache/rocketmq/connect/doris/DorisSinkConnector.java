/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.rocketmq.connect.doris;

import io.openmessaging.KeyValue;
import io.openmessaging.connector.api.component.task.Task;
import io.openmessaging.connector.api.component.task.sink.SinkConnector;
import java.util.ArrayList;
import java.util.List;
import org.apache.rocketmq.connect.doris.cfg.DorisSinkConnectorConfig;
import org.apache.rocketmq.connect.doris.utils.ConfigCheckUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DorisSinkConnector extends SinkConnector {
    private static final Logger LOG = LoggerFactory.getLogger(DorisSinkConnector.class);
    private KeyValue keyValue;

    @Override
    public void start(KeyValue keyValue) {
        this.keyValue = DorisSinkConnectorConfig.convertToLowercase(keyValue);
        DorisSinkConnectorConfig.setDefaultValues(this.keyValue);
        ConfigCheckUtils.validateConfig(this.keyValue);
    }

    /**
     * stop DorisSinkConnector
     */
    @Override
    public void stop() {
        LOG.info("doris sink connector stop");
    }

    @Override
    public Class<? extends Task> taskClass() {
        return DorisSinkTask.class;
    }

    @Override
    public List<KeyValue> taskConfigs(final int maxTasks) {
        List<KeyValue> taskConfigs = new ArrayList<>(maxTasks);
        for (int i = 0; i < maxTasks; i++) {
            keyValue.put("task_id", i + "");
            taskConfigs.add(this.keyValue);
        }
        return taskConfigs;
    }

}
