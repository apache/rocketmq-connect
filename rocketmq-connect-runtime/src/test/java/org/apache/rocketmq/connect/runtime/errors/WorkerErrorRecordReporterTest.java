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

package org.apache.rocketmq.connect.runtime.errors;

import io.openmessaging.KeyValue;
import io.openmessaging.connector.api.data.ConnectRecord;
import io.openmessaging.connector.api.data.RecordConverter;
import io.openmessaging.connector.api.data.RecordOffset;
import io.openmessaging.connector.api.data.RecordPartition;
import io.openmessaging.internal.DefaultKeyValue;
import org.apache.rocketmq.connect.runtime.config.WorkerConfig;
import org.apache.rocketmq.connect.runtime.converter.record.StringConverter;
import org.apache.rocketmq.connect.runtime.metrics.ConnectMetrics;
import org.apache.rocketmq.connect.runtime.utils.ConnectorTaskId;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class WorkerErrorRecordReporterTest {

    private WorkerErrorRecordReporter workerErrorRecordReporter;

    private RetryWithToleranceOperator retryWithToleranceOperator;

    private RecordConverter recordConverter;

    private ConnectRecord connectRecord;

    private RecordPartition recordPartition;

    private RecordOffset recordOffset;

    @Before
    public void before() {
        Map<String, Object> offset = new HashMap<>();
        recordOffset = new RecordOffset(offset);
        retryWithToleranceOperator = new RetryWithToleranceOperator(1000, 2000, ToleranceType.ALL, new ErrorMetricsGroup(new ConnectorTaskId("test-connect",1), new ConnectMetrics(new
                WorkerConfig())));
        recordConverter = new StringConverter();
        workerErrorRecordReporter = new WorkerErrorRecordReporter(retryWithToleranceOperator, recordConverter);
        Map<String, Object> partition = new HashMap<>();
        partition.put("queueId", 0);
        partition.put("topic", "DEFAULT_TOPIC");
        partition.put("queueOffset", 0L);
        partition.put("brokerName", "mockBrokerName");
        recordPartition = new RecordPartition(partition);
        connectRecord = new ConnectRecord(recordPartition, recordOffset, System.currentTimeMillis());
        KeyValue extensions = new DefaultKeyValue();
        extensions.put("extension1", "test1");
        connectRecord.setExtensions(extensions);
    }

    @Test
    public void reportTest() {
        workerErrorRecordReporter.report(connectRecord, new RuntimeException());
    }
}
