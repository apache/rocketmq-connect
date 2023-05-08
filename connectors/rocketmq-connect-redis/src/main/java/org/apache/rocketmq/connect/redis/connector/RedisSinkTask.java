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

import com.alibaba.fastjson.JSON;
import io.openmessaging.KeyValue;
import io.openmessaging.connector.api.component.task.sink.SinkTask;
import io.openmessaging.connector.api.data.ConnectRecord;
import io.openmessaging.connector.api.data.FieldType;
import io.openmessaging.connector.api.data.Struct;
import io.openmessaging.connector.api.errors.ConnectException;
import org.apache.rocketmq.connect.redis.builder.AbstractCommandExec;
import org.apache.rocketmq.connect.redis.builder.CommandExecFactory;
import org.apache.rocketmq.connect.redis.common.Config;
import org.apache.rocketmq.connect.redis.common.JedisUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.util.List;
import java.util.Map;

public class RedisSinkTask extends SinkTask {
    
    private static final Logger LOGGER = LoggerFactory.getLogger(RedisSinkTask.class);
    
    private Config redisConfig;
    
    private JedisPool pool;
    
    @Override
    public void put(final List<ConnectRecord> records) throws ConnectException {
        if (records == null || records.size() == 0) {
            return;
        }
        Jedis jedis = this.pool.getResource();
        // 读取每一条记录，并且按照source的操作反向插入redis
        for (ConnectRecord record : records) {
            if (record == null || record.getSchema().getFieldType() != FieldType.STRUCT) {
                continue;
            }
            final Struct structData = (Struct) record.getData();
            final String command = structData.getString("command");
            final String key = structData.getString("key");
            final String value = structData.getString("value");
            final Map params = JSON.parseObject(structData.getString("params"), Map.class);
            LOGGER.debug("command: {}, key: {}, value: {}, params: {}", command, key, value, params);
            AbstractCommandExec commandExec = CommandExecFactory.findExec(command);
            if (null == commandExec) {
                LOGGER.error("command: {} not support", command);
                continue;
            }
            String execResult = commandExec.exec(jedis, key, value, params);
            LOGGER.info("commandExec key={}, value={}, params={}, result: {}", key, value, params, execResult);
        }
    }
    
    @Override
    public void start(final KeyValue keyValue) {
        this.redisConfig = new Config();
        this.redisConfig.load(keyValue);
        this.pool = JedisUtil.getJedisPool(this.redisConfig);
        LOGGER.info("task config msg: {}", this.redisConfig.toString());
    }
    
    @Override
    public void stop() {
        this.pool.close();
    }
    
    @Override
    public void validate(final KeyValue config) {
        super.validate(config);
    }
}
