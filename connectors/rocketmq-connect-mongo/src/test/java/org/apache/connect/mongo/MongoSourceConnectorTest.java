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

import io.openmessaging.connector.api.data.ConnectRecord;
import io.openmessaging.connector.api.data.RecordOffset;
import io.openmessaging.connector.api.data.RecordPosition;
import io.openmessaging.connector.api.data.Struct;
import io.openmessaging.internal.DefaultKeyValue;
import java.lang.reflect.Field;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.LinkedBlockingQueue;
import org.apache.connect.mongo.connector.MongoSourceConnector;
import org.apache.connect.mongo.connector.MongoSourceTask;
import org.apache.connect.mongo.replicator.Constants;
import org.apache.connect.mongo.replicator.Position;
import org.apache.connect.mongo.replicator.ReplicaSetConfig;
import org.apache.connect.mongo.replicator.ReplicaSetsContext;
import org.apache.connect.mongo.replicator.event.OperationType;
import org.apache.connect.mongo.replicator.event.ReplicationEvent;
import org.bson.BsonTimestamp;
import org.bson.Document;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class MongoSourceConnectorTest {

    private MongoSourceConnector mongoSourceConnector;
    private DefaultKeyValue keyValue;
    private SourceTaskConfig sourceTaskConfig;

    @Before
    public void before() {
        mongoSourceConnector = new MongoSourceConnector();
        keyValue = new DefaultKeyValue();
        sourceTaskConfig = new SourceTaskConfig();

    }

    @Test
    public void takeClass() {
        Assert.assertEquals(mongoSourceConnector.taskClass(), MongoSourceTask.class);
    }


    @Test
    public void testPoll() throws Exception {
        LinkedBlockingQueue<ConnectRecord> entries = new LinkedBlockingQueue<>();
        ReplicaSetsContext context = new ReplicaSetsContext(sourceTaskConfig);
        Field dataEntryQueue = ReplicaSetsContext.class.getDeclaredField("connectRecordQueue");
        dataEntryQueue.setAccessible(true);
        dataEntryQueue.set(context, entries);
        ReplicationEvent event = new ReplicationEvent();
        event.setOperationType(OperationType.INSERT);
        event.setNamespace("test.person");
        event.setTimestamp(new BsonTimestamp(1565609506, 1));
        event.setDocument(new Document("testKey", "testValue"));
        event.setH(324243242L);
        event.setEventData(Optional.ofNullable(new Document("testEventKey", "testEventValue")));
        event.setObjectId(Optional.empty());
        event.setReplicaSetName("testReplicaName");
        event.setCollectionName("testCollectName");
        final ReplicaSetConfig name = new ReplicaSetConfig("", "testReplicaName", "localhost:27027");
        Position position = new Position();
        position.setTimeStamp(0);
        name.setPosition(position);
        context.publishEvent(event, name);
        List<ConnectRecord> connectRecords = context.poll();
        Assert.assertTrue(connectRecords.size() == 1);

        ConnectRecord connectRecord = connectRecords.get(0);
        final Struct data = (Struct) connectRecord.getData();

        Assert.assertEquals("test.person", connectRecord.getExtension(Constants.NAMESPACE));
        Assert.assertEquals("testReplicaName", connectRecord.getExtension(Constants.REPLICA_SET_NAME));

        final RecordPosition recordPosition = connectRecord.getPosition();
        final RecordOffset offsetMap = recordPosition.getOffset();
        Assert.assertEquals(1565609506, offsetMap.getOffset().get(Constants.TIMESTAMP));

        final List<io.openmessaging.connector.api.data.Field> fields = connectRecord.getSchema().getFields();
        Assert.assertEquals(1, fields.size());


    }

}
