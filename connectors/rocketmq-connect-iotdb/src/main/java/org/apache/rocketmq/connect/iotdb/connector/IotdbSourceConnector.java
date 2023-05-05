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

package org.apache.rocketmq.connect.iotdb.connector;

import io.openmessaging.KeyValue;
import io.openmessaging.connector.api.component.task.Task;
import io.openmessaging.connector.api.component.task.source.SourceConnector;
import io.openmessaging.internal.DefaultKeyValue;
import java.util.ArrayList;
import java.util.List;
import org.apache.rocketmq.connect.iotdb.config.IotdbConfig;
import org.apache.rocketmq.connect.iotdb.config.IotdbConstant;
import org.apache.rocketmq.connect.iotdb.exception.IotdbRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IotdbSourceConnector extends SourceConnector {

    private Logger logger = LoggerFactory.getLogger(IotdbSourceConnector.class);

    private KeyValue keyValue;

    private IotdbConfig config;

    @Override
    public List<KeyValue> taskConfigs(int maxTasks) {
        this.config = new IotdbConfig();
        this.config.load(keyValue);
        List<KeyValue> configs = new ArrayList<>();
        final String paths = config.getIotdbPaths();
        String[] pathArr = null;
        try {
            pathArr = paths.split(",");
        } catch (Exception e) {
            logger.error("The format of iotdbPaths is incorrect. Please modify the format and restart the iotdb source connector", e);
            throw e;
        }
        for (int i = 0; i < pathArr.length; i++) {
            KeyValue resultKeyValue = new DefaultKeyValue();
            resultKeyValue.put(IotdbConstant.IOTDB_HOST, this.keyValue.getString(IotdbConstant.IOTDB_HOST));
            resultKeyValue.put(IotdbConstant.IOTDB_PORT, this.keyValue.getInt(IotdbConstant.IOTDB_PORT));
            resultKeyValue.put(IotdbConstant.IOTDB_PATH, pathArr[i]);
            if (maxTasks < 1) {
                configs.add(resultKeyValue);
                continue;
            }
            for (int j = 0; j < maxTasks; j++) {
                configs.add(resultKeyValue);
            }

        }
        return configs;
    }

    @Override
    public Class<? extends Task> taskClass() {
        return IotdbSourceTask.class;
    }

    @Override
    public void start(KeyValue config) {
        for (String requestKey : IotdbConfig.REQUEST_CONFIG) {
            if (!config.containsKey(requestKey)) {
                throw new IotdbRuntimeException("Request config key: " + requestKey);
            }
        }
        this.keyValue = config;
    }

    @Override
    public void stop() {
        this.keyValue = null;
    }
}
