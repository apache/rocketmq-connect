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

package org.apache.rocketmq.connect.hive.replicator.source;

import io.openmessaging.KeyValue;
import io.openmessaging.connector.api.data.RecordOffset;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import org.apache.rocketmq.connect.hive.config.HiveConfig;
import org.apache.rocketmq.connect.hive.config.HiveRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HiveReplicator {

    private static final Logger LOGGER = LoggerFactory.getLogger(HiveReplicator.class);

    private HiveConfig config;

    private HiveQuery query;

    private BlockingQueue<HiveRecord> queue = new LinkedBlockingQueue<>();

    public HiveReplicator(HiveConfig config) {
        this.config = config;
    }

    public void start(RecordOffset recordOffset, KeyValue keyValue) {
        query = new HiveQuery(this);
        CompletableFuture.runAsync(() -> query.start(recordOffset, keyValue));
        LOGGER.info("HiveReplicator start success");
    }

    public void stop() {

    }

    public HiveConfig getConfig() {
        return this.config;
    }

    public void commit(HiveRecord data) {
        queue.add(data);
    }

    public BlockingQueue<HiveRecord> getQueue() {
        return this.queue;
    }
}
