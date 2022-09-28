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
import io.openmessaging.connector.api.data.RecordPosition;
import org.apache.rocketmq.connect.runtime.connectorwrapper.RecordOffsetManagement.SubmittedPosition;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@RunWith(MockitoJUnitRunner.class)
public class RecordOffsetManagementTest {

    private RecordOffsetManagement recordOffsetManagement = new RecordOffsetManagement();

    private SubmittedPosition submittedPosition;

    @Mock
    private RecordPosition position;

    private RecordOffsetManagement.CommittableOffsets committableOffsets;

    @Before
    public void before() {
        submittedPosition = recordOffsetManagement.submitRecord(position);
        Map<RecordPartition, RecordOffset> offsets = new HashMap<>();
        RecordOffset recordOffset = new RecordOffset(new HashMap<>());
        RecordPartition recordPartition = new RecordPartition(new HashMap<>());
        offsets.put(recordPartition, recordOffset);
        committableOffsets = new RecordOffsetManagement.CommittableOffsets(offsets, 10, 10, 10, 50, recordPartition);
    }

    @Test
    public void submitRecordTest() {
        final SubmittedPosition result = recordOffsetManagement.submitRecord(position);
        assert result != null;
    }

    @Test
    public void awaitAllMessagesTest() {
        final boolean flag = recordOffsetManagement.awaitAllMessages(1000, TimeUnit.MILLISECONDS);
        assert flag == false;
    }

    @Test
    public void committableOffsetsTest() {
        recordOffsetManagement.submitRecord(position);

        final RecordOffsetManagement.CommittableOffsets offsets = recordOffsetManagement.committableOffsets();
        assert offsets != null;
    }

    @Test
    public void ackTest() {
        submittedPosition.ack();
    }

    @Test
    public void removeTest() {
        final boolean remove = submittedPosition.remove();
        assert remove == true;
    }

    @Test
    public void updatedWithTest() {
        final RecordOffsetManagement.CommittableOffsets offsets = committableOffsets.updatedWith(committableOffsets);
        assert offsets != null;
    }
}
