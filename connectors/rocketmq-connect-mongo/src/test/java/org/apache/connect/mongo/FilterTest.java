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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.connect.mongo.initsync.CollectionMeta;
import org.apache.connect.mongo.replicator.OperationFilter;
import org.apache.connect.mongo.replicator.event.OperationType;
import org.apache.connect.mongo.replicator.event.ReplicationEvent;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class FilterTest {

    private SourceTaskConfig sourceTaskConfig;
    private Map<String, List<String>> insterest;

    @Before
    public void init() {
        sourceTaskConfig = new SourceTaskConfig();
        insterest = new HashMap<>();
    }

    @Test
    public void testSpecialDb() {
        List<String> collections = new ArrayList<>();
        collections.add("person");
        insterest.put("test", collections);
        sourceTaskConfig.setInterestDbAndCollection(JSONObject.toJSONString(insterest));
        OperationFilter operationFilter = new OperationFilter(sourceTaskConfig);
        Assert.assertTrue(operationFilter.filterMeta(new CollectionMeta("test", "person")));
        Assert.assertFalse(operationFilter.filterMeta(new CollectionMeta("test", "person01")));
    }

    @Test
    public void testBlankDb() {
        OperationFilter operationFilter = new OperationFilter(sourceTaskConfig);
        Assert.assertTrue(operationFilter.filterMeta(new CollectionMeta("test", "test")));
        Assert.assertTrue(operationFilter.filterMeta(new CollectionMeta("test1", "test01")));
    }

    @Test
    public void testAsterisk() {
        List<String> collections = new ArrayList<>();
        collections.add("*");
        insterest.put("test", collections);
        sourceTaskConfig.setInterestDbAndCollection(JSONObject.toJSONString(insterest));
        OperationFilter operationFilter = new OperationFilter(sourceTaskConfig);
        Assert.assertTrue(operationFilter.filterMeta(new CollectionMeta("test", "testsad")));
        Assert.assertTrue(operationFilter.filterMeta(new CollectionMeta("test", "tests032")));
    }

    @Test
    public void testFilterEvent() {
        OperationFilter operationFilter = new OperationFilter(sourceTaskConfig);
        ReplicationEvent replicationEvent = new ReplicationEvent();
        replicationEvent.setOperationType(OperationType.NOOP);
        Assert.assertFalse(operationFilter.filterEvent(replicationEvent));
        replicationEvent.setOperationType(OperationType.DB_COMMAND);
        Assert.assertTrue(operationFilter.filterEvent(replicationEvent));

    }

}
