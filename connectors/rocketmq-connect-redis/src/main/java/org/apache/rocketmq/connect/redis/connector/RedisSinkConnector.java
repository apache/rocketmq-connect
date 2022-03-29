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

package org.apache.rocketmq.connect.redis.connector;

import io.openmessaging.KeyValue;
import io.openmessaging.connector.api.Task;
import io.openmessaging.connector.api.sink.SinkConnector;
import org.apache.rocketmq.connect.redis.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.ArrayList;
import java.util.List;

/**
 * author: doubleDimple
 */
public class RedisSinkConnector extends SinkConnector {

    private static final Logger LOGGER = LoggerFactory.getLogger(RedisSinkConnector.class);

    private volatile boolean configValid = false;
    private volatile boolean adminStarted;
    private KeyValue keyValue;

    @Override
    public String verifyAndSetConfig(KeyValue config) {
        this.keyValue = config;
        String msg = Config.checkConfig(keyValue);
        if (msg != null) {
            return msg;
        }
        this.configValid = true;
        return null;
    }

    @Override
    public void start() {
        LOGGER.info("the redisSinkConnector is start...");
    }

    @Override
    public void stop() {

    }

    @Override
    public void pause() {

    }

    @Override
    public void resume() {

    }

    @Override
    public Class<? extends Task> taskClass() {
        return RedisSinkTask.class;
    }

    @Override
    public List<KeyValue> taskConfigs() {
        List<KeyValue> keyValues = new ArrayList<>();
        keyValues.add(this.keyValue);
        return keyValues;
    }
}
