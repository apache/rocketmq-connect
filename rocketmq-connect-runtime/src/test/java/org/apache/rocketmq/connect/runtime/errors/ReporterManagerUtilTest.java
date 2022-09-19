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

import io.openmessaging.connector.api.data.RecordConverter;
import java.util.List;
import org.apache.rocketmq.connect.runtime.common.ConnectKeyValue;
import org.apache.rocketmq.connect.runtime.config.WorkerConfig;
import org.apache.rocketmq.connect.runtime.converter.record.StringConverter;
import org.junit.Assert;
import org.junit.Test;

public class ReporterManagerUtilTest {

    private ConnectKeyValue connectKeyValue = new ConnectKeyValue();

    @Test
    public void createRetryWithToleranceOperatorTest() {
        final RetryWithToleranceOperator operator = ReporterManagerUtil.createRetryWithToleranceOperator(connectKeyValue);
        Assert.assertNotNull(operator);

    }

    @Test
    public void createWorkerErrorRecordReporterTest() {
        RetryWithToleranceOperator retryWithToleranceOperator = new RetryWithToleranceOperator(100, 100, ToleranceType.ALL);
        RecordConverter converter = new StringConverter();
        connectKeyValue.put("errors.log.enable", "true");
        connectKeyValue.put("errors.deadletterqueue.topic.name", "TEST_TOPIC");
        final WorkerErrorRecordReporter reporter = ReporterManagerUtil.createWorkerErrorRecordReporter(connectKeyValue, retryWithToleranceOperator, converter);
        Assert.assertNotNull(reporter);
    }

    @Test
    public void sinkTaskReportersTest() {
        WorkerConfig workerConfig = new WorkerConfig();
        final List<ErrorReporter> connector = ReporterManagerUtil.sinkTaskReporters("testConnector", connectKeyValue, workerConfig);
        Assert.assertEquals(1, connector.size());
    }

    @Test
    public void sourceTaskReportersTest() {
        final List<ErrorReporter> connector = ReporterManagerUtil.sourceTaskReporters("testSourceConnector", connectKeyValue);
        Assert.assertEquals(1, connector.size());
    }
}
