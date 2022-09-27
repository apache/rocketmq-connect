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

package org.apache.rocketmq.connect.runtime.connectorwrapper;

import io.openmessaging.connector.api.data.RecordOffset;
import io.openmessaging.connector.api.data.RecordPartition;
import org.apache.rocketmq.client.consumer.DefaultLitePullConsumer;
import org.apache.rocketmq.connect.runtime.common.ConnectKeyValue;
import org.assertj.core.api.Assertions;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@RunWith(MockitoJUnitRunner.class)
public class WorkerSinkTaskContextTest {

    private WorkerSinkTaskContext workerSinkTaskContext;

    private ConnectKeyValue connectKeyValue = new ConnectKeyValue();

    @Mock
    private WorkerSinkTask workerSinkTask;

    private DefaultLitePullConsumer consumer = new DefaultLitePullConsumer();

    private RecordPartition recordPartition;

    private RecordOffset recordOffset;

    @Before
    public void before() {
        Map<String, String> partition = new HashMap<>();
        recordPartition = new RecordPartition(partition);
        partition.put("queueId", "0");
        partition.put("brokerName", "broker_a");
        partition.put("topic", "TEST_TOPIC");
        Map<String, String> offset = new HashMap<>();
        offset.put("queueOffset", "0");
        recordOffset = new RecordOffset(offset);

        workerSinkTaskContext = new WorkerSinkTaskContext(connectKeyValue, workerSinkTask, consumer);
    }

    @Test
    public void resetOffsetTest() {
        Assertions.assertThatCode(() -> workerSinkTaskContext.resetOffset(recordPartition, recordOffset)).doesNotThrowAnyException();

        Map<RecordPartition, RecordOffset> offsets = new HashMap<>();
        offsets.put(recordPartition, recordOffset);
        Assertions.assertThatCode(() -> workerSinkTaskContext.resetOffset(offsets)).doesNotThrowAnyException();
    }

    @Test
    public void pauseTest() {
        List<RecordPartition> recordPartitions = new ArrayList<>();
        recordPartitions.add(recordPartition);

        Assertions.assertThatCode(() -> workerSinkTaskContext.pause(recordPartitions)).doesNotThrowAnyException();
    }

    @Test
    public void resumeTest() {
        List<RecordPartition> recordPartitions = new ArrayList<>();
        recordPartitions.add(recordPartition);
        Assertions.assertThatCode(() -> workerSinkTaskContext.resume(recordPartitions)).doesNotThrowAnyException();
    }
}
