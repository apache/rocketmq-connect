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
import org.apache.rocketmq.connect.runtime.service.local.LocalPositionManagementServiceImpl;
import org.apache.rocketmq.connect.runtime.utils.datasync.DataSynchronizerCallback;
import org.assertj.core.api.Assertions;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

public class PositionStorageWriterTest {

    private PositionStorageWriter positionStorageWriter;

    private PositionManagementService positionManagementService;

    private WorkerConfig connectConfig;

    private ServerResponseMocker nameServerMocker;

    private ServerResponseMocker brokerMocker;

    private RecordPartition recordPartition;

    private RecordOffset recordOffset;

    @Before
    public void before() {
        nameServerMocker = NameServerMocker.startByDefaultConf(9876, 10911);
        brokerMocker = ServerResponseMocker.startServer(10911, "Hello World".getBytes(StandardCharsets.UTF_8));

        Map<String, Object> partition = new HashMap<>();
        partition.put("topic", "testTopic");
        partition.put("brokerName", "mockBroker");
        partition.put("queueId", 0);
        recordPartition = new RecordPartition(partition);

        Map<String, Long> offset = new HashMap<>();
        offset.put("queueOffset", 0L);
        recordOffset = new RecordOffset(offset);

        connectConfig = new WorkerConfig();
        connectConfig.setNamesrvAddr("localhost:9876");

        positionManagementService = new LocalPositionManagementServiceImpl();
        positionManagementService.initialize(connectConfig, new JsonConverter(), new JsonConverter());

        positionStorageWriter = new PositionStorageWriter("testNameSpace", positionManagementService);
    }

    @After
    public void after() throws IOException {
        positionManagementService.stop();
        positionStorageWriter.close();
        brokerMocker.shutdown();
        nameServerMocker.shutdown();
    }


    @Test
    public void writeOffsetTest() {
        Assertions.assertThatCode(() -> positionStorageWriter.writeOffset(recordPartition, recordOffset)).doesNotThrowAnyException();

        Map<RecordPartition, RecordOffset> positions = new HashMap<>();
        positions.put(recordPartition, recordOffset);
        Assertions.assertThatCode(() -> positionStorageWriter.writeOffset(positions)).doesNotThrowAnyException();
    }

    @Test
    public void flushTest() {
        Assertions.assertThatCode(() -> positionStorageWriter.beginFlush()).doesNotThrowAnyException();
        Assertions.assertThatCode(() -> {
            positionStorageWriter.doFlush(new DataSynchronizerCallback() {
                @Override
                public void onCompletion(Throwable error, Object key, Object result) {

                }
            });
        }).doesNotThrowAnyException();

        Assertions.assertThatCode(() -> positionStorageWriter.cancelFlush()).doesNotThrowAnyException();
    }

}
