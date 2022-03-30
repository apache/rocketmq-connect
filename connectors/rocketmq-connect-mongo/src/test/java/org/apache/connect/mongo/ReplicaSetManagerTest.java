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

import java.util.Map;
import org.apache.connect.mongo.replicator.ReplicaSetConfig;
import org.apache.connect.mongo.replicator.ReplicaSetManager;
import org.junit.Assert;
import org.junit.Test;

public class ReplicaSetManagerTest {

    @Test(expected = IllegalArgumentException.class)
    public void testCreatReplicaSetsExceptionWithoutMongoAddr() {
        ReplicaSetManager.create("");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCreatReplicaSetsExceptioWithoutReplicaSetName() {
        ReplicaSetManager.create("127.0.0.1:27081");
    }

    @Test
    public void testCreatReplicaSetsSpecialReplicaSetName() {
        ReplicaSetManager replicaSetManager = ReplicaSetManager.create("replicaName1/127.0.0.1:27081,127.0.0.1:27082,127.0.0.1:27083");
        Map<String, ReplicaSetConfig> replicaSetConfigMap = replicaSetManager.getReplicaConfigByName();
        Assert.assertTrue(replicaSetConfigMap.size() == 1);
        Assert.assertNotNull(replicaSetConfigMap.get("replicaName1"));
        Assert.assertEquals("127.0.0.1:27081,127.0.0.1:27082,127.0.0.1:27083", replicaSetConfigMap.get("replicaName1").getHost());
        Assert.assertEquals("replicaName1", replicaSetConfigMap.get("replicaName1").getReplicaSetName());
    }

    @Test
    public void testCreatReplicaSetsSpecialShardNameAndReplicaSetName() {
        ReplicaSetManager replicaSetManager = ReplicaSetManager.create("shardName1=replicaName1/127.0.0.1:27081,127.0.0.1:27082,127.0.0.1:27083");
        Map<String, ReplicaSetConfig> replicaSetConfigMap = replicaSetManager.getReplicaConfigByName();
        Assert.assertTrue(replicaSetConfigMap.size() == 1);
        Assert.assertNotNull(replicaSetConfigMap.get("replicaName1"));
        Assert.assertEquals("127.0.0.1:27081,127.0.0.1:27082,127.0.0.1:27083", replicaSetConfigMap.get("replicaName1").getHost());
        Assert.assertEquals("replicaName1", replicaSetConfigMap.get("replicaName1").getReplicaSetName());
        Assert.assertEquals("shardName1", replicaSetConfigMap.get("replicaName1").getShardName());
    }

    @Test
    public void testCreatReplicaSetsMutiMongoAddr() {
        ReplicaSetManager replicaSetManager = ReplicaSetManager.create("shardName1=replicaName1/127.0.0.1:27081,127.0.0.1:27082,127.0.0.1:27083;shardName2=replicaName2/127.0.0.1:27281,127.0.0.1:27282,127.0.0.1:27283");
        Map<String, ReplicaSetConfig> replicaSetConfigMap = replicaSetManager.getReplicaConfigByName();
        Assert.assertTrue(replicaSetConfigMap.size() == 2);
        Assert.assertNotNull(replicaSetConfigMap.get("replicaName1"));
        Assert.assertEquals("127.0.0.1:27081,127.0.0.1:27082,127.0.0.1:27083", replicaSetConfigMap.get("replicaName1").getHost());
        Assert.assertEquals("replicaName1", replicaSetConfigMap.get("replicaName1").getReplicaSetName());
        Assert.assertEquals("shardName1", replicaSetConfigMap.get("replicaName1").getShardName());

        Assert.assertNotNull(replicaSetConfigMap.get("replicaName2"));
        Assert.assertEquals("127.0.0.1:27281,127.0.0.1:27282,127.0.0.1:27283", replicaSetConfigMap.get("replicaName2").getHost());
        Assert.assertEquals("replicaName2", replicaSetConfigMap.get("replicaName2").getReplicaSetName());
        Assert.assertEquals("shardName2", replicaSetConfigMap.get("replicaName2").getShardName());
    }

}
