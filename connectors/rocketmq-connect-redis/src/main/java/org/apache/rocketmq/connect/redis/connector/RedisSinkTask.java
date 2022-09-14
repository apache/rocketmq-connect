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

import com.alibaba.fastjson.JSONObject;
import io.openmessaging.KeyValue;
import io.openmessaging.connector.api.common.QueueMetaData;
import io.openmessaging.connector.api.data.EntryType;
import io.openmessaging.connector.api.data.Field;
import io.openmessaging.connector.api.data.Schema;
import io.openmessaging.connector.api.data.SinkDataEntry;
import io.openmessaging.connector.api.sink.SinkTask;
import org.apache.rocketmq.connect.redis.config.Config;
import org.apache.rocketmq.connect.redis.converter.KVEntryConverter;
import org.apache.rocketmq.connect.redis.converter.RedisEntryConverter;
import org.apache.rocketmq.connect.redis.handler.DefaultRedisEventHandler;
import org.apache.rocketmq.connect.redis.handler.RedisEventHandler;
import org.apache.rocketmq.connect.redis.processor.DefaultRedisEventProcessor;
import org.apache.rocketmq.connect.redis.processor.RedisEventProcessor;
import org.apache.rocketmq.connect.redis.sink.RedisUpdater;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * author doubleDimple
 */
public class RedisSinkTask extends SinkTask {

    private static final Logger LOGGER = LoggerFactory.getLogger(RedisSinkTask.class);

    private RedisUpdater updater;

    /**
     * listening and handle Redis event.
     */
    private RedisEventProcessor eventProcessor;
    private Config config;
    /**
     * convert kVEntry to list of sourceDataEntry
     */
    private KVEntryConverter kvEntryConverter;

    public RedisEventProcessor getEventProcessor() {
        return eventProcessor;
    }

    public void setEventProcessor(RedisEventProcessor eventProcessor) {
        this.eventProcessor = eventProcessor;
    }

    public Config getConfig() {
        return config;
    }

    @Override
    public void put(Collection<SinkDataEntry> sinkDataEntries) {
             //save data from MQ to redis
            for (SinkDataEntry sinkDataEntry : sinkDataEntries) {
                Map<Field, Object[]> fieldMap = new HashMap<>();
                Object[] payloads = sinkDataEntry.getPayload();

                Schema schema = sinkDataEntry.getSchema();
                EntryType entryType = sinkDataEntry.getEntryType();

                List<Field> fields = schema.getFields();
                Boolean parseError = false;
                if (!fields.isEmpty()) {
                    for (Field field : fields) {
                        Object fieldValue = payloads[field.getIndex()];
                        Object[] value = JSONObject.parseArray((String)fieldValue).toArray();
                        if (value.length == 2) {
                            fieldMap.put(field, value);
                        } else {
                            LOGGER.error("parseArray error, fieldValue:{}", fieldValue);
                            parseError = true;
                        }
                    }
                }
                if (!parseError) {
                    Boolean isSuccess = updater.push(fieldMap, entryType);
                    if (!isSuccess) {
                        LOGGER.error("push data error, entryType:{}, fieldMap:{}", fieldMap, entryType);
                    }
                }
            }
    }

    @Override
    public void commit(Map<QueueMetaData, Long> offsets) {

    }

    @Override
    public void start(KeyValue keyValue) {
        this.kvEntryConverter = new RedisEntryConverter();

        this.config = new Config();
        this.config.load(keyValue);
        LOGGER.info("task config msg: {}", this.config.toString());

        this.eventProcessor = new DefaultRedisEventProcessor(config);
        RedisEventHandler eventHandler = new DefaultRedisEventHandler(this.config);
        this.eventProcessor.registEventHandler(eventHandler);
        try {
            this.eventProcessor.start();
            LOGGER.info("Redis task start.");
        } catch (IOException e) {
            e.printStackTrace();
            LOGGER.error("processor start error: [{}]", e.getMessage());
            this.stop();
        }
    }

    @Override
    public void stop() {
        if (this.eventProcessor != null) {
            try {
                this.eventProcessor.stop();
                LOGGER.info("Redis task is stopped.");
            } catch (IOException e) {
                LOGGER.error("processor stop error: {}", e);
            }
        }
    }

    @Override
    public void pause() {

    }

    @Override
    public void resume() {

    }
}
