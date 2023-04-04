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

import com.mongodb.MongoClientSettings;
import com.mongodb.MongoTimeoutException;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.internal.MongoClientImpl;
import java.lang.reflect.Field;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.StringUtils;
import org.apache.connect.mongo.replicator.MongoClientFactory;
import org.apache.connect.mongo.replicator.ReplicaSetConfig;
import org.bson.Document;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class MongoFactoryTest {

    private ReplicaSetConfig replicaSetConfig;

    private SourceTaskConfig sourceTaskConfig;

    private MongoClientFactory mongoClientFactory;

    private MongoClientImpl client;

    @Before
    public void before() {
        this.replicaSetConfig = new ReplicaSetConfig("shardName1", "rep1", "127.0.0.1:27017");
        this.sourceTaskConfig = new SourceTaskConfig();
        this.mongoClientFactory = new MongoClientFactory(sourceTaskConfig);
    }

    @After
    public void after() {
        client.close();
    }

    @Test
    public void testCreateMongoClientWithSSL() {
        sourceTaskConfig.setSsl(true);
        MongoClientSettings settings = getSettings();
        System.out.println(settings.getSslSettings());
        Assert.assertTrue(settings.getSslSettings().isEnabled());
    }

    @Test
    public void testCreateMongoClientWithTSL() {
        sourceTaskConfig.setTsl(true);
        MongoClientSettings settings = getSettings();
        System.out.println(settings.getSslSettings());
        Assert.assertTrue(settings.getSslSettings().isEnabled());
    }

    @Test
    public void testCreateMongoClientWithserverSelectionTimeoutMS() {
        try {
            replicaSetConfig.setReplicaSetName("testReplicatSet");
            sourceTaskConfig.setServerSelectionTimeoutMS(150);
            System.out.println(getSettings().getClusterSettings());
            Assert.assertTrue(getSettings().getClusterSettings().getServerSelectionTimeout(TimeUnit.MILLISECONDS) == 150);
        } catch (MongoTimeoutException exception) {
            Assert.assertTrue(StringUtils.startsWith(exception.getMessage(), "Timed out after 100 ms while waiting for a server that matches"));
        }
    }

    @Test
    public void testCreateMongoClientWithConnectTimeoutMS() {
        sourceTaskConfig.setConnectTimeoutMS(1200);
        System.out.println(getSettings().getSocketSettings());
        Assert.assertTrue(getSettings().getSocketSettings().getConnectTimeout(TimeUnit.MILLISECONDS) == 1200);

    }

    @Test
    public void testCreateMongoClientWithSocketTimeoutMS() {
        sourceTaskConfig.setSocketTimeoutMS(1100);
        System.out.println(getSettings().getSocketSettings());
        Assert.assertTrue(getSettings().getSocketSettings().getReadTimeout(TimeUnit.MILLISECONDS) == 1100);
    }

    @Test
    public void testCreateMongoClientWithInvalidHostNameAllowed() {
        sourceTaskConfig.setSslInvalidHostNameAllowed(true);
        System.out.println(getSettings().getSslSettings());
        Assert.assertTrue(getSettings().getSslSettings().isInvalidHostNameAllowed());

        sourceTaskConfig.setSslInvalidHostNameAllowed(false);
        System.out.println(getSettings().getSslSettings());
        Assert.assertFalse(getSettings().getSslSettings().isInvalidHostNameAllowed());
    }

    @Test
    public void testCreateMongoClientWithInvalidHostNameAllowedTsl() {
        sourceTaskConfig.setTlsAllowInvalidHostnames(true);
        System.out.println(getSettings().getSslSettings());
        Assert.assertTrue(getSettings().getSslSettings().isInvalidHostNameAllowed());

        sourceTaskConfig.setTlsAllowInvalidHostnames(false);
        System.out.println(getSettings().getSslSettings());
        Assert.assertFalse(getSettings().getSslSettings().isInvalidHostNameAllowed());
    }

    @Test
    public void testCreateMongoClientWithTlsinsecure() {
        sourceTaskConfig.setTlsInsecure(true);
        System.out.println(getSettings().getSslSettings());
        Assert.assertTrue(getSettings().getSslSettings().isInvalidHostNameAllowed());

        sourceTaskConfig.setTlsInsecure(false);
        System.out.println(getSettings().getSslSettings());
        Assert.assertFalse(getSettings().getSslSettings().isInvalidHostNameAllowed());
    }

    @Test
    public void testCreateMongoClientWithCompression() {
        sourceTaskConfig.setCompressors("zlib");
        System.out.println(getSettings().getCompressorList());
        Assert.assertTrue(getSettings().getCompressorList().get(0).getName().equals("zlib"));
    }

    @Test
    public void testCreateMongoClientWithCompressionLevel() {
        sourceTaskConfig.setCompressors("zlib");
        sourceTaskConfig.setZlibCompressionLevel("7");
        System.out.println(getSettings().getCompressorList());
        Assert.assertTrue(getSettings().getCompressorList().get(0).getName().equals("zlib"));
        Assert.assertTrue(getSettings().getCompressorList().get(0).getProperty("level", 0) == 7);
    }

    @Test
    public void testCreateMongoClientWithAuth() {
        sourceTaskConfig.setMongoUserName("test");
        sourceTaskConfig.setMongoPassWord("123456");
        System.out.println(getSettings().getCredential());
        Assert.assertTrue(getSettings().getCredential().getSource().equals("admin"));
        Assert.assertTrue(getSettings().getCredential().getUserName().equals("test"));
        Assert.assertTrue(new String(getSettings().getCredential().getPassword()).equals("123456"));
    }

    private MongoClientSettings getSettings() {
        try {
            client = (MongoClientImpl) mongoClientFactory.createMongoClient(replicaSetConfig);
            Field field = MongoClientImpl.class.getDeclaredField("settings");
            field.setAccessible(true);
            return (MongoClientSettings) field.get(client);
        } catch (Exception e) {

        }
        return null;
    }

}
