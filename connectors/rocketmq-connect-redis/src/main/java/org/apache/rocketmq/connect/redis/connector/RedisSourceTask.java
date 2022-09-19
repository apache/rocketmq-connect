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

import io.openmessaging.connector.api.component.task.source.SourceTask;
import io.openmessaging.connector.api.data.ConnectRecord;
import io.openmessaging.connector.api.data.RecordOffset;
import io.openmessaging.connector.api.data.RecordPartition;
import io.openmessaging.connector.api.storage.OffsetStorageReader;
import java.io.IOException;
import io.openmessaging.KeyValue;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.rocketmq.connect.redis.common.Config;
import org.apache.rocketmq.connect.redis.common.Options;
import org.apache.rocketmq.connect.redis.converter.KVEntryConverter;
import org.apache.rocketmq.connect.redis.converter.RedisEntryConverter;
import org.apache.rocketmq.connect.redis.handler.DefaultRedisEventHandler;
import org.apache.rocketmq.connect.redis.handler.RedisEventHandler;
import org.apache.rocketmq.connect.redis.pojo.KVEntry;
import org.apache.rocketmq.connect.redis.processor.DefaultRedisEventProcessor;
import org.apache.rocketmq.connect.redis.processor.RedisEventProcessor;
import org.apache.rocketmq.connect.redis.processor.RedisEventProcessorCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RedisSourceTask extends SourceTask {
    private static final Logger LOGGER = LoggerFactory.getLogger(RedisSourceTask.class);
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
    public List<ConnectRecord> poll() {
        try {
            KVEntry event = this.eventProcessor.poll();
            if (event == null) {
                return null;
            }
            event.queueName(Options.REDIS_QUEUE.name());
            final OffsetStorageReader reader = this.sourceTaskContext.offsetStorageReader();
            final RecordOffset recordOffset = reader.readOffset(buildRecordPartition());
            if (recordOffset != null) {
                final Map<String, ?> offset = recordOffset.getOffset();
                final Object obj = offset.get(Options.REDIS_OFFSET.name());
                if (obj != null) {
                    if (event.getOffset() <= Long.valueOf(obj.toString())) {
                        return new ArrayList<>();
                    }
                }
            }
            List<ConnectRecord> res = this.kvEntryConverter.kVEntryToConnectRecord(event);
            LOGGER.info("send data entries: {}", res);
            return res;
        } catch (InterruptedException e) {
            LOGGER.error("redis task interrupted. {}", e);
            this.stop();
        } catch (Exception e) {
            LOGGER.error("redis task error. {}", e);
            this.stop();
        }
        return null;
    }


    @Override
    public void start(KeyValue keyValue) {
        this.kvEntryConverter = new RedisEntryConverter();

        this.config = new Config();
        this.config.load(keyValue);
        LOGGER.info("task config msg: {}", this.config.toString());

        final OffsetStorageReader reader = this.sourceTaskContext.offsetStorageReader();
        final RecordOffset recordOffset = reader.readOffset(buildRecordPartition());
        Long offset = 0L;
        if (recordOffset != null && recordOffset.getOffset().size() > 0) {
            offset = (Long) recordOffset.getOffset().get(Options.REDIS_OFFSET.name());
        }

        LOGGER.info("task load connector runtime position: {}", offset);

        this.eventProcessor = new DefaultRedisEventProcessor(config);
        RedisEventHandler eventHandler = new DefaultRedisEventHandler(this.config);
        this.eventProcessor.registerEventHandler(eventHandler);
        this.eventProcessor.registerProcessorCallback(new DefaultRedisEventProcessorCallback());
        try {
            this.eventProcessor.start(offset);
            LOGGER.info("Redis task start.");
        } catch (IOException e) {
            LOGGER.error("processor start error: {}", e);
            this.stop();
        }
    }


    @Override public void stop() {
        if (this.eventProcessor != null) {
            try {
                this.eventProcessor.stop();
                LOGGER.info("Redis task is stopped.");
            } catch (IOException e) {
                LOGGER.error("processor stop error: {}", e);
            }
        }
    }

    private class DefaultRedisEventProcessorCallback implements RedisEventProcessorCallback {
        @Override public void onStop(RedisEventProcessor eventProcessor) {
            stop();
        }
    }

    private RecordPartition buildRecordPartition() {
        Map<String, String> partitionMap = new HashMap<>();
        partitionMap.put(Options.REDIS_PARTITION.name(), Options.REDIS_PARTITION.name());
        RecordPartition  recordPartition = new RecordPartition(partitionMap);
        return recordPartition;
    }

}
