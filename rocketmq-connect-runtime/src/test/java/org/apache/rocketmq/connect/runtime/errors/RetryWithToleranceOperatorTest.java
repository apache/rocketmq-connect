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

import io.openmessaging.connector.api.data.ConnectRecord;
import io.openmessaging.connector.api.data.RecordOffset;
import io.openmessaging.connector.api.data.RecordPartition;
import org.apache.rocketmq.connect.runtime.config.WorkerConfig;
import org.apache.rocketmq.connect.runtime.metrics.ConnectMetrics;
import org.apache.rocketmq.connect.runtime.utils.ConnectorTaskId;
import org.assertj.core.api.Assertions;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class RetryWithToleranceOperatorTest {

    private RetryWithToleranceOperator retryWithToleranceOperator = new RetryWithToleranceOperator(1000, 1000, ToleranceType.ALL, new ErrorMetricsGroup(new ConnectorTaskId("test-connect",1), new ConnectMetrics(new
            WorkerConfig())));

    private RecordPartition recordPartition;

    private RecordOffset recordOffset;
    private Operation operation = new Operation() {
        @Override
        public Object call() throws Exception {
            return true;
        }
    };

    @Before
    public void before() {
        Map<String, ?> partition = new HashMap<>();
        recordPartition = new RecordPartition(partition);
        Map<String, ?> offset = new HashMap<>();
        recordOffset = new RecordOffset(offset);
    }

    @Test
    public void executeFailedTest() {
        ConnectRecord connectRecord = new ConnectRecord(recordPartition, recordOffset, System.currentTimeMillis());
        Assertions.assertThatCode(() -> retryWithToleranceOperator.executeFailed(ErrorReporter.Stage.CONSUME, this.getClass(),
                connectRecord, new RuntimeException())).doesNotThrowAnyException();
    }

    @Test
    public void executeTest() {
        final Object result = retryWithToleranceOperator.execute(operation, ErrorReporter.Stage.CONSUME, this.getClass());
        Assert.assertTrue((Boolean) result);
    }

    @Test
    public void execAndRetryTest() throws Exception {
        final Object result = retryWithToleranceOperator.execAndRetry(operation);
        Assert.assertTrue((Boolean) result);
    }

    @Test
    public void execAndHandleErrorTest() {
        operation = () -> {
            int i = 0;
            if (i == 0) {
                throw new RuntimeException();
            }
            return true;
        };
        final Object result = retryWithToleranceOperator.execAndHandleError(operation, RuntimeException.class);
        Assert.assertNull(result);
    }

    @Test
    public void withinToleranceLimitsTest() {
        final boolean result = retryWithToleranceOperator.withinToleranceLimits();
        Assert.assertTrue(result);
    }

    @Test
    public void backoffTest() {
        Assertions.assertThatCode(() -> retryWithToleranceOperator.backoff(2, System.currentTimeMillis() + 1000)).doesNotThrowAnyException();
    }

}
