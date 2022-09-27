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

package org.apache.connect.mongo.connector;

import io.openmessaging.KeyValue;
import io.openmessaging.connector.api.component.task.source.SourceTask;
import io.openmessaging.connector.api.data.ConnectRecord;
import io.openmessaging.connector.api.data.RecordOffset;
import io.openmessaging.connector.api.data.RecordPartition;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.connect.mongo.SourceTaskConfig;
import org.apache.connect.mongo.replicator.Constants;
import org.apache.connect.mongo.replicator.Position;
import org.apache.connect.mongo.replicator.ReplicaSet;
import org.apache.connect.mongo.replicator.ReplicaSetManager;
import org.apache.connect.mongo.replicator.ReplicaSetsContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MongoSourceTask extends SourceTask {

    private Logger logger = LoggerFactory.getLogger(this.getClass());

    private SourceTaskConfig sourceTaskConfig;

    private ReplicaSetManager replicaSetManager;

    private ReplicaSetsContext replicaSetsContext;

    @Override
    public List<ConnectRecord> poll() {

        return replicaSetsContext.poll();
    }

    @Override
    public void start(KeyValue config) {
        try {
            sourceTaskConfig = new SourceTaskConfig();
            sourceTaskConfig.load(config);

            replicaSetsContext = new ReplicaSetsContext(sourceTaskConfig);

            replicaSetManager = ReplicaSetManager.create(sourceTaskConfig.getMongoAddr());

            replicaSetManager.getReplicaConfigByName().forEach((replicaSetName, replicaSetConfig) -> {
                final RecordOffset recordOffset = this.sourceTaskContext.offsetStorageReader().readOffset(this.buildRecordPartition(replicaSetName));
                if (recordOffset != null && recordOffset.getOffset().size() > 0) {
                    final Map<String, Object> offset = (Map<String, Object>) recordOffset.getOffset();
                    Position position = new Position();
                    position.setTimeStamp((int) offset.get(Constants.TIMESTAMP));
                    replicaSetConfig.setPosition(position);
                } else {
                    Position position = new Position();
                    position.setTimeStamp(sourceTaskConfig.getPositionTimeStamp());
                    replicaSetConfig.setPosition(position);
                }
                replicaSetConfig.setMaxTask(config.getInt(Constants.MAX_TASK));
                ReplicaSet replicaSet = new ReplicaSet(replicaSetConfig, replicaSetsContext);
                replicaSetsContext.addReplicaSet(replicaSet);
                replicaSet.start();
            });

        } catch (Throwable throwable) {
            logger.error("task start error", throwable);
            stop();
        }
    }

    private RecordPartition buildRecordPartition(String replicaSetName) {
        Map<String, String> partitionMap = new HashMap<>();
        partitionMap.put(Constants.REPLICA_SET_NAME, replicaSetName);
        RecordPartition  recordPartition = new RecordPartition(partitionMap);
        return recordPartition;
    }

    @Override
    public void stop() {
        logger.info("shut down.....");
        replicaSetsContext.shutdown();
    }

}
