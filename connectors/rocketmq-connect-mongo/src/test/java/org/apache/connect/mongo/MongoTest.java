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

import com.alibaba.fastjson.JSONObject;
import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import io.openmessaging.connector.api.data.ConnectRecord;
import io.openmessaging.connector.api.data.RecordPartition;
import io.openmessaging.connector.api.data.RecordPosition;
import io.openmessaging.connector.api.data.Schema;
import io.openmessaging.connector.api.data.Struct;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.connect.mongo.initsync.InitSync;
import org.apache.connect.mongo.replicator.Constants;
import org.apache.connect.mongo.replicator.Position;
import org.apache.connect.mongo.replicator.ReplicaSet;
import org.apache.connect.mongo.replicator.ReplicaSetConfig;
import org.apache.connect.mongo.replicator.ReplicaSetsContext;
import org.apache.connect.mongo.replicator.event.Document2EventConverter;
import org.apache.connect.mongo.replicator.event.OperationType;
import org.apache.connect.mongo.replicator.event.ReplicationEvent;
import org.bson.BsonTimestamp;
import org.bson.Document;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class MongoTest {

    private MongoClient mongoClient;

    @Before
    public void before() {
        MongoClientSettings.Builder builder = MongoClientSettings.builder();
        builder.applyConnectionString(new ConnectionString("mongodb://127.0.0.1:27017"));
        mongoClient = MongoClients.create(builder.build());
    }

    @Test
    public void testConvertEvent() {
        Document oplog = new Document();
        BsonTimestamp timestamp = new BsonTimestamp(1565074665, 10);
        oplog.put(Constants.TIMESTAMP, timestamp);
        oplog.put(Constants.NAMESPACE, "test.person");
        oplog.put(Constants.HASH, 11111L);
        oplog.put(Constants.OPERATION_TYPE, "i");
        Document document = new Document();
        document.put("test", "test");
        oplog.put(Constants.OPERATION, document);
        ReplicationEvent event = Document2EventConverter.convert(oplog, "testR");
        Assert.assertEquals(timestamp, event.getTimestamp());
        Assert.assertEquals("test.person", event.getNamespace());
        Assert.assertTrue(11111L == event.getH());
        Assert.assertEquals(OperationType.INSERT, event.getOperationType());
        Assert.assertEquals(document, event.getEventData().get());
        Assert.assertEquals("testR", event.getReplicaSetName());

    }

    @Test
    public void testInitSyncCopy() throws NoSuchFieldException, IllegalAccessException {
        MongoCollection<Document> collection = mongoClient.getDatabase("test").getCollection("person");
        collection.deleteMany(new Document());
        int count = 10;
        List<String> documents = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            Document document = new Document();
            document.put("name", "test" + i);
            document.put("age", i);
            document.put("sex", i % 2 == 0 ? "boy" : "girl");
            collection.insertOne(document);
            documents.add(document.getObjectId("_id").toHexString());
        }
        SourceTaskConfig sourceTaskConfig = new SourceTaskConfig();
        Map<String, List<String>> insterest = new HashMap<>();
        List<String> collections = new ArrayList<>();
        collections.add("*");
        insterest.put("test", collections);
        sourceTaskConfig.setInterestDbAndCollection(JSONObject.toJSONString(insterest));
        ReplicaSetConfig replicaSetConfig = new ReplicaSetConfig("", "test", "localhost");
        Position position = new Position();
        replicaSetConfig.setPosition(position);
        ReplicaSetsContext replicaSetsContext = new ReplicaSetsContext(sourceTaskConfig);
        ReplicaSet replicaSet = new ReplicaSet(replicaSetConfig, replicaSetsContext);
        Field running = ReplicaSet.class.getDeclaredField("running");
        running.setAccessible(true);
        running.set(replicaSet, new AtomicBoolean(true));
        InitSync initSync = new InitSync(replicaSetConfig, mongoClient, replicaSetsContext, replicaSet);
        initSync.start();
        int syncCount = 0;
        while (syncCount < count) {
            Collection<ConnectRecord> connectRecords = replicaSetsContext.poll();
            Assert.assertTrue(connectRecords.size() > 0);
            for (ConnectRecord connectRecord : connectRecords) {
                final Struct data = (Struct) connectRecord.getData();
                final Object[] values = data.getValues();
                Schema schema = connectRecord.getSchema();
                Assert.assertTrue(schema.getFields().size() == 4);
                Assert.assertTrue(values.length == 4);
                final Map<String, ?> partition = connectRecord.getPosition().getPartition().getPartition();
                Assert.assertEquals("test", partition.get(Constants.REPLICA_SET_NAME));
                syncCount++;
            }

        }

        Assert.assertTrue(syncCount == count);
    }

    @Test
    public void testCompareBsonTimestamp() {
        BsonTimestamp lt = new BsonTimestamp(11111111, 1);
        BsonTimestamp gt = new BsonTimestamp(11111111, 2);
        Assert.assertTrue(lt.compareTo(gt) < 0);

        lt = new BsonTimestamp(11111111, 1);
        gt = new BsonTimestamp(22222222, 1);
        Assert.assertTrue(lt.compareTo(gt) < 0);

    }
}
