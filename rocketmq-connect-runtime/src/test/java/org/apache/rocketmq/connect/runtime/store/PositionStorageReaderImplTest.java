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

package org.apache.rocketmq.connect.runtime.store;

import io.openmessaging.connector.api.data.RecordOffset;
import io.openmessaging.connector.api.data.RecordPartition;
import org.apache.rocketmq.connect.runtime.config.WorkerConfig;
import org.apache.rocketmq.connect.runtime.connectorwrapper.NameServerMocker;
import org.apache.rocketmq.connect.runtime.connectorwrapper.ServerResponseMocker;
import org.apache.rocketmq.connect.runtime.converter.record.json.JsonConverter;
import org.apache.rocketmq.connect.runtime.service.PositionManagementService;
import org.apache.rocketmq.connect.runtime.service.PositionManagementServiceImpl;
import org.assertj.core.util.Maps;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class PositionStorageReaderImplTest {

    private PositionStorageReaderImpl positionStorageReader;

    private PositionManagementService positionManagementService;

    private ExtendRecordPartition extendRecordPartition;

    private String NAMESPACE = "testNameSpace";

    private RecordPartition recordPartition;

    private RecordOffset recordOffset;

    private WorkerConfig connectConfig;

    private ServerResponseMocker nameServerMocker;

    private ServerResponseMocker brokerMocker;

    @Before
    public void before() {
        nameServerMocker = NameServerMocker.startByDefaultConf(9876, 10911);
        brokerMocker = ServerResponseMocker.startServer(10911, "Hello World".getBytes(StandardCharsets.UTF_8));

        connectConfig = new WorkerConfig();
        connectConfig.setNamesrvAddr("localhost:9876");

        positionManagementService = new PositionManagementServiceImpl();
        Map<String, String> map = Maps.newHashMap("ip_port", "127.0.0.13306");
        extendRecordPartition = new ExtendRecordPartition(NAMESPACE, map);
        Map<String, Object> partition = new HashMap<>();
        partition.put("topic", "testTopic");
        partition.put("brokerName", "mockBroker");
        partition.put("queueId", 0);
        recordPartition = new RecordPartition(partition);

        Map<String, Long> offset = new HashMap<>();
        offset.put("queueOffset", 0L);
        recordOffset = new RecordOffset(offset);
        positionManagementService.initialize(connectConfig, new JsonConverter(), new JsonConverter());

        positionManagementService.start();
        positionManagementService.putPosition(extendRecordPartition, recordOffset);
        positionStorageReader = new PositionStorageReaderImpl(NAMESPACE, positionManagementService);
    }

    @After
    public void after() {
        positionManagementService.stop();
        brokerMocker.shutdown();
        nameServerMocker.shutdown();
    }

    @Test
    public void readOffsetTest() {
        final RecordOffset recordOffset = positionStorageReader.readOffset(extendRecordPartition);
        Assert.assertEquals(0L, recordOffset.getOffset().get("queueOffset"));
    }

    @Test
    public void readOffsetsTest() {
        Collection<RecordPartition> partitions = new ArrayList<>();
        partitions.add(extendRecordPartition);
        final Map<RecordPartition, RecordOffset> map = positionStorageReader.readOffsets(partitions);
        final RecordOffset offset = map.get(extendRecordPartition);
        Assert.assertEquals(0L, offset.getOffset().get("queueOffset"));


    }

}
