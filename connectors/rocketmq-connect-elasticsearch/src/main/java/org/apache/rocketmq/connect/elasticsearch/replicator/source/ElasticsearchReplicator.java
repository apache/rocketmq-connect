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

package org.apache.rocketmq.connect.elasticsearch.replicator.source;

import io.openmessaging.KeyValue;
import io.openmessaging.connector.api.data.RecordOffset;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import org.apache.rocketmq.connect.elasticsearch.config.ElasticsearchConfig;
import org.elasticsearch.search.SearchHit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ElasticsearchReplicator {

    private static final Logger LOGGER = LoggerFactory.getLogger(ElasticsearchReplicator.class);

    private ElasticsearchConfig config;

    private ElasticsearchQuery query;

    private BlockingQueue<SearchHit> queue = new LinkedBlockingQueue<>();

    public ElasticsearchReplicator(ElasticsearchConfig config) {
        this.config = config;
    }

    public void start(RecordOffset recordOffset, KeyValue keyValue) {
        query = new ElasticsearchQuery(this);
        query.start(recordOffset, keyValue);
        LOGGER.info("ElasticsearchReplicator start succeed");
    }

    public void stop() {
        query.stop();
    }

    public ElasticsearchConfig getConfig() {
        return this.config;
    }

    public void commit(SearchHit data) {
        queue.add(data);
    }

    public BlockingQueue<SearchHit> getQueue() {
        return this.queue;
    }
}
