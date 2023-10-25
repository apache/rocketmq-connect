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

package org.apache.rocketmq.connect.neo4j.sink;

import java.util.List;

import org.apache.rocketmq.connect.neo4j.config.Neo4jSinkConfig;
import org.apache.rocketmq.connect.neo4j.helper.LabelTypeEnum;
import org.apache.rocketmq.connect.neo4j.helper.Neo4jClient;
import org.apache.rocketmq.connect.neo4j.sink.write.NodeWriteProcessor;
import org.apache.rocketmq.connect.neo4j.sink.write.Processor;
import org.apache.rocketmq.connect.neo4j.sink.write.RelationshipWriteProcessor;

import io.openmessaging.KeyValue;
import io.openmessaging.connector.api.component.task.sink.SinkTask;
import io.openmessaging.connector.api.data.ConnectRecord;
import io.openmessaging.connector.api.errors.ConnectException;

public class Neo4jSinkTask extends SinkTask {
    private Neo4jSinkConfig config;
    private Neo4jClient client;
    private Processor processor;

    @Override
    public void put(List<ConnectRecord> sinkRecords) throws ConnectException {
        if (sinkRecords == null || sinkRecords.size() < 1) {
            return;
        }
        for (ConnectRecord connectRecord : sinkRecords) {
            processor.write(connectRecord);
        }
    }

    @Override
    public void start(KeyValue keyValue) {
        this.config = new Neo4jSinkConfig();
        this.config.load(keyValue);
        LabelTypeEnum labelTypeEnum = LabelTypeEnum.nameOf(config.getLabelType());
        if (labelTypeEnum == null) {
            throw new RuntimeException("labelType only support node or relationship");
        }
        this.client = new Neo4jClient(this.config);
        if (!client.ping()) {
            throw new RuntimeException("Cannot connect to neo4j server!");
        }
        if (LabelTypeEnum.node == labelTypeEnum) {
            processor = new NodeWriteProcessor(config, client);
        } else {
            processor = new RelationshipWriteProcessor(config, client);
        }
    }

    @Override
    public void stop() {
        this.client = null;
    }
}