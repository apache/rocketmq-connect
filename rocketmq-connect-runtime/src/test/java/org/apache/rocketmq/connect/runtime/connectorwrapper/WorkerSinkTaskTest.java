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

import io.openmessaging.KeyValue;
import io.openmessaging.connector.api.component.task.sink.SinkTask;
import io.openmessaging.connector.api.data.ConnectRecord;
import io.openmessaging.connector.api.data.RecordConverter;
import io.openmessaging.internal.DefaultKeyValue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.rocketmq.client.consumer.DefaultLitePullConsumer;
import org.apache.rocketmq.connect.runtime.common.ConnectKeyValue;
import org.apache.rocketmq.connect.runtime.config.ConnectorConfig;
import org.apache.rocketmq.connect.runtime.config.SinkConnectorConfig;
import org.apache.rocketmq.connect.runtime.config.WorkerConfig;
import org.apache.rocketmq.connect.runtime.connectorwrapper.status.WrapperStatusListener;
import org.apache.rocketmq.connect.runtime.connectorwrapper.testimpl.TestConverter;
import org.apache.rocketmq.connect.runtime.connectorwrapper.testimpl.TestSinkTask;
import org.apache.rocketmq.connect.runtime.controller.isolation.Plugin;
import org.apache.rocketmq.connect.runtime.errors.ErrorMetricsGroup;
import org.apache.rocketmq.connect.runtime.errors.RetryWithToleranceOperator;
import org.apache.rocketmq.connect.runtime.errors.ToleranceType;
import org.apache.rocketmq.connect.runtime.errors.WorkerErrorRecordReporter;
import org.apache.rocketmq.connect.runtime.metrics.ConnectMetrics;
import org.apache.rocketmq.connect.runtime.service.StateManagementService;
import org.apache.rocketmq.connect.runtime.service.local.LocalStateManagementServiceImpl;
import org.apache.rocketmq.connect.runtime.stats.ConnectStatsManager;
import org.apache.rocketmq.connect.runtime.stats.ConnectStatsService;
import org.apache.rocketmq.connect.runtime.utils.ConnectorTaskId;
import org.assertj.core.api.Assertions;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;


@RunWith(MockitoJUnitRunner.class)
public class WorkerSinkTaskTest {

    ExecutorService executorService = Executors.newSingleThreadScheduledExecutor();
    private WorkerSinkTask workerSinkTask;
    private WorkerConfig connectConfig = new WorkerConfig();
    private ConnectorTaskId connectorTaskId = new ConnectorTaskId("testConnector", 1);
    private SinkTask sinkTask = new TestSinkTask();
    private ConnectKeyValue connectKeyValue = new ConnectKeyValue();
    private RecordConverter recordConverter = new TestConverter();

    private DefaultLitePullConsumer defaultLitePullConsumer = new DefaultLitePullConsumer();
    private AtomicReference<WorkerState> workerState = new AtomicReference<>(WorkerState.STARTED);
    private ConnectStatsManager connectStatsManager = new ConnectStatsManager(connectConfig);
    private ConnectStatsService connectStatsService = new ConnectStatsService();
    private KeyValue keyValue = new DefaultKeyValue();
    @Mock
    private Plugin plugin;
    private TransformChain<ConnectRecord> transformChain;
    private RetryWithToleranceOperator retryWithToleranceOperator = new RetryWithToleranceOperator(1000, 1000, ToleranceType.ALL, new ErrorMetricsGroup(new ConnectorTaskId("connect", 1), new ConnectMetrics(new WorkerConfig())));
    private WorkerErrorRecordReporter workerErrorRecordReporter;
    private WrapperStatusListener wrapperStatusListener;

    private StateManagementService stateManagementService;

    @Before
    public void before() {
        connectKeyValue.put(SinkConnectorConfig.CONNECT_TOPICNAMES, "TEST_TOPIC");
        keyValue.put(ConnectorConfig.TRANSFORMS, "testTransform");
        keyValue.put("transforms-testTransform-class", "org.apache.rocketmq.connect.runtime.connectorwrapper.TestTransform");
        transformChain = new TransformChain<>(keyValue, plugin);
        workerErrorRecordReporter = new WorkerErrorRecordReporter(retryWithToleranceOperator, recordConverter);
        workerSinkTask = new WorkerSinkTask(
                connectConfig,
                connectorTaskId,
                sinkTask,
                WorkerSinkTaskTest.class.getClassLoader(),
                connectKeyValue,
                recordConverter,
                recordConverter,
                defaultLitePullConsumer,
                workerState,
                connectStatsManager,
                connectStatsService,
                transformChain,
                retryWithToleranceOperator,
                workerErrorRecordReporter,
                new WrapperStatusListener(new LocalStateManagementServiceImpl(), "workId"),
                new ConnectMetrics(new WorkerConfig())
        );
    }

    @After
    public void after() {
        executorService.shutdown();
    }

    @Test
    public void executeTest() throws InterruptedException {
        Assertions.assertThatCode(() -> executorService.submit(() -> workerSinkTask.run())).doesNotThrowAnyException();
        TimeUnit.SECONDS.sleep(5);
        workerSinkTask.close();
    }
}
