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

import java.util.ArrayList;
import java.util.List;

import io.openmessaging.KeyValue;
import io.openmessaging.connector.api.Task;
import io.openmessaging.connector.api.source.SourceConnector;
import org.apache.rocketmq.connect.redis.common.Config;

public class RedisSourceConnector extends SourceConnector {
    private KeyValue keyValue;

    @Override public String verifyAndSetConfig(KeyValue keyValue) {
        this.keyValue = keyValue;
        String msg = Config.checkConfig(keyValue);
        if (msg != null) {
            return msg;
        }
        return null;
    }

    @Override public void start() {

    }


    @Override public void stop() {

    }


    @Override public void pause() {

    }


    @Override public void resume() {

    }


    @Override public Class<? extends Task> taskClass() {
        return RedisSourceTask.class;
    }


    @Override public List<KeyValue> taskConfigs() {
        List<KeyValue> keyValues = new ArrayList<>();
        keyValues.add(this.keyValue);
        return keyValues;
    }
}
