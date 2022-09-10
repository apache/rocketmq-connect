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

package org.apache.rocketmq.connect.runtime.service.memory;

import io.openmessaging.connector.api.data.RecordOffset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.rocketmq.connect.runtime.config.WorkerConfig;
import org.apache.rocketmq.connect.runtime.converter.record.json.JsonConverter;
import org.apache.rocketmq.connect.runtime.service.PositionManagementService;
import org.apache.rocketmq.connect.runtime.store.ExtendRecordPartition;
import org.assertj.core.api.Assertions;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class FilePositionManagementServiceImplTest {

    private PositionManagementService filePositionManagementService = new FilePositionManagementServiceImpl();

    private WorkerConfig workerConfig = new WorkerConfig();

    @Before
    public void before() {
        filePositionManagementService.initialize(workerConfig, new JsonConverter(),new JsonConverter());
        filePositionManagementService.start();
    }

    @After
    public void after() {
        filePositionManagementService.stop();
    }

    @Test
    public void persistTest() {
        Assertions.assertThatCode(() -> filePositionManagementService.persist()).doesNotThrowAnyException();
    }

    @Test
    public void loadTest() {
        Assertions.assertThatCode(() -> filePositionManagementService.load()).doesNotThrowAnyException();
    }

    @Test
    public void putPositionTest() {
        Map<String, String> partition = new HashMap<>();
        partition.put("topic", "testTopic");
        partition.put("brokerName", "mockBroker");
        partition.put("queueId", "0");
        ExtendRecordPartition extendRecordPartition = new ExtendRecordPartition("testConnector", partition);
        Map<String, Long> offset = new HashMap<>();
        offset.put("queueOffset", 123L);
        RecordOffset recordOffset = new RecordOffset(offset);
        filePositionManagementService.putPosition(extendRecordPartition, recordOffset);
        final RecordOffset position = filePositionManagementService.getPosition(extendRecordPartition);
        Assert.assertEquals(123L, position.getOffset().get("queueOffset"));

        Map<ExtendRecordPartition, RecordOffset> positions  = new HashMap<>();
        Map<String, Long> offset2 = new HashMap<>();
        offset2.put("queueOffset", 124L);
        RecordOffset recordOffset2 = new RecordOffset(offset2);
        positions.put(extendRecordPartition, recordOffset2);
        filePositionManagementService.putPosition(positions);
        final RecordOffset position2 = filePositionManagementService.getPosition(extendRecordPartition);
        Assert.assertEquals(124L, position2.getOffset().get("queueOffset"));

        List<ExtendRecordPartition> partitions = new ArrayList<>();
        partitions.add(extendRecordPartition);
        Assertions.assertThatCode(() ->  filePositionManagementService.removePosition(partitions)).doesNotThrowAnyException();

    }
}
