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

package org.apache.connect.mongo;

import io.openmessaging.KeyValue;
import io.openmessaging.connector.api.component.task.source.SourceTask;
import io.openmessaging.connector.api.component.task.source.SourceTaskContext;
import io.openmessaging.connector.api.storage.OffsetStorageReader;
import io.openmessaging.internal.DefaultKeyValue;
import java.lang.reflect.Field;
import java.util.List;
import org.apache.commons.lang3.StringUtils;
import org.apache.connect.mongo.connector.MongoSourceTask;
import org.apache.connect.mongo.replicator.ReplicaSet;
import org.apache.connect.mongo.replicator.ReplicaSetConfig;
import org.apache.connect.mongo.replicator.ReplicaSetsContext;
import org.junit.Assert;
import org.junit.Test;

public class MongoSourceTaskTest {

    @Test
    public void testEmptyContextStart() throws NoSuchFieldException, IllegalAccessException {
        MongoSourceTask mongoSourceTask = new MongoSourceTask();
        DefaultKeyValue defaultKeyValue = new DefaultKeyValue();
        defaultKeyValue.put("mongoAddr", "test/127.0.0.1:27027");
        defaultKeyValue.put("positionTimeStamp", "11111111");
        defaultKeyValue.put("positionInc", "111");
        defaultKeyValue.put("serverSelectionTimeoutMS", "10");
        defaultKeyValue.put("dataSync", "true");

        Field context = SourceTask.class.getDeclaredField("sourceTaskContext");
        context.setAccessible(true);
        context.set(mongoSourceTask, emptyTaskContext());
        mongoSourceTask.start(defaultKeyValue);

        Field replicaSetsContext = MongoSourceTask.class.getDeclaredField("replicaSetsContext");
        replicaSetsContext.setAccessible(true);
        ReplicaSetsContext setsContext = (ReplicaSetsContext) replicaSetsContext.get(mongoSourceTask);

        Field replicaSets = ReplicaSetsContext.class.getDeclaredField("replicaSets");
        replicaSets.setAccessible(true);
        List<ReplicaSet> replicaSetList = (List<ReplicaSet>) replicaSets.get(setsContext);
        Assert.assertTrue(replicaSetList.size() == 1);
        ReplicaSet replicaSet = replicaSetList.get(0);
        Field replicaSetConfig = ReplicaSet.class.getDeclaredField("replicaSetConfig");
        replicaSetConfig.setAccessible(true);
        ReplicaSetConfig replicaSetConfig1 = (ReplicaSetConfig) replicaSetConfig.get(replicaSet);
        Assert.assertTrue(StringUtils.equals(replicaSetConfig1.getReplicaSetName(), "test"));
        Assert.assertTrue(StringUtils.equals(replicaSetConfig1.getHost(), "127.0.0.1:27027"));
        Assert.assertTrue(replicaSetConfig1.getPosition().getTimeStamp() == 11111111);
    }

    private SourceTaskContext emptyTaskContext() {
        return new SourceTaskContext() {

            @Override public OffsetStorageReader offsetStorageReader() {
                return new TestPositionStorageReader();
            }

            @Override public String getConnectorName() {
                return "mongoSourceConnector";
            }

            @Override public String getTaskName() {
                return "mongoSourceTask";
            }

            @Override
            public KeyValue configs() {
                return new DefaultKeyValue();
            }
        };
    }

    @Test
    public void testContextStart() throws NoSuchFieldException, IllegalAccessException {
        MongoSourceTask mongoSourceTask = new MongoSourceTask();
        DefaultKeyValue defaultKeyValue = new DefaultKeyValue();
        defaultKeyValue.put("mongoAddr", "test/127.0.0.1:27017");
        defaultKeyValue.put("serverSelectionTimeoutMS", "10");

        Field context = SourceTask.class.getDeclaredField("sourceTaskContext");
        context.setAccessible(true);
        context.set(mongoSourceTask, taskContext());
        mongoSourceTask.start(defaultKeyValue);

        Field replicaSetsContext = MongoSourceTask.class.getDeclaredField("replicaSetsContext");
        replicaSetsContext.setAccessible(true);
        ReplicaSetsContext setsContext = (ReplicaSetsContext) replicaSetsContext.get(mongoSourceTask);

        Field replicaSets = ReplicaSetsContext.class.getDeclaredField("replicaSets");
        replicaSets.setAccessible(true);
        List<ReplicaSet> replicaSetList = (List<ReplicaSet>) replicaSets.get(setsContext);
        Assert.assertTrue(replicaSetList.size() == 1);
        ReplicaSet replicaSet = replicaSetList.get(0);
        Field replicaSetConfig = ReplicaSet.class.getDeclaredField("replicaSetConfig");
        replicaSetConfig.setAccessible(true);
        ReplicaSetConfig replicaSetConfig1 = (ReplicaSetConfig) replicaSetConfig.get(replicaSet);
        Assert.assertTrue(StringUtils.equals(replicaSetConfig1.getReplicaSetName(), "test"));
        Assert.assertTrue(StringUtils.equals(replicaSetConfig1.getHost(), "127.0.0.1:27017"));
        Assert.assertTrue(replicaSetConfig1.getPosition().getTimeStamp() == 0);
    }

    private SourceTaskContext taskContext() {
        return new SourceTaskContext() {

            @Override public OffsetStorageReader offsetStorageReader() {
                return new TestPositionStorageReader();
            }

            @Override public String getConnectorName() {
                return "mongoSourceConnector";
            }

            @Override public String getTaskName() {
                return "mongoSourceTask";
            }

            @Override
            public KeyValue configs() {
                return new DefaultKeyValue();
            }
        };
    }
}
