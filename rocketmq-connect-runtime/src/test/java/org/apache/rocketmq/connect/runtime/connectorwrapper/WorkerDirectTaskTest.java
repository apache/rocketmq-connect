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

import com.google.common.collect.Lists;
import io.openmessaging.KeyValue;
import io.openmessaging.connector.api.component.task.sink.SinkTask;
import io.openmessaging.connector.api.component.task.source.SourceTask;
import io.openmessaging.connector.api.data.ConnectRecord;
import io.openmessaging.internal.DefaultKeyValue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.rocketmq.connect.runtime.common.ConnectKeyValue;
import org.apache.rocketmq.connect.runtime.config.WorkerConfig;
import org.apache.rocketmq.connect.runtime.connectorwrapper.status.WrapperStatusListener;
import org.apache.rocketmq.connect.runtime.connectorwrapper.testimpl.TestSinkTask;
import org.apache.rocketmq.connect.runtime.connectorwrapper.testimpl.TestSourceTask;
import org.apache.rocketmq.connect.runtime.controller.isolation.Plugin;
import org.apache.rocketmq.connect.runtime.errors.ErrorMetricsGroup;
import org.apache.rocketmq.connect.runtime.errors.RetryWithToleranceOperator;
import org.apache.rocketmq.connect.runtime.errors.ToleranceType;
import org.apache.rocketmq.connect.runtime.metrics.ConnectMetrics;
import org.apache.rocketmq.connect.runtime.service.PositionManagementService;
import org.apache.rocketmq.connect.runtime.service.local.LocalPositionManagementServiceImpl;
import org.apache.rocketmq.connect.runtime.service.StateManagementService;
import org.apache.rocketmq.connect.runtime.service.local.LocalStateManagementServiceImpl;
import org.apache.rocketmq.connect.runtime.stats.ConnectStatsManager;
import org.apache.rocketmq.connect.runtime.stats.ConnectStatsService;
import org.apache.rocketmq.connect.runtime.utils.ConnectorTaskId;
import org.assertj.core.api.Assertions;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class WorkerDirectTaskTest {

    private WorkerDirectTask workerDirectTask;

    private WorkerConfig workerConfig;

    private ConnectorTaskId connectorTaskId;

    private SourceTask sourceTask;

    private ClassLoader classLoader;

    private SinkTask sinkTask;

    private ConnectKeyValue connectKeyValue;

    private PositionManagementService positionManagementService;

    private AtomicReference<WorkerState> workerState = new AtomicReference<>();

    private ConnectStatsManager connectStatsManager;

    private ConnectStatsService connectStatsService;

    TransformChain<ConnectRecord> transformChain;

    private KeyValue keyValue;

    private Plugin plugin;

    private RetryWithToleranceOperator retryWithToleranceOperator;

    private ErrorMetricsGroup errorMetricsGroup;

    private ConnectMetrics connectMetrics;

    private WrapperStatusListener wrapperStatusListener;

    private StateManagementService stateManagementService;

    private ExecutorService executorService = Executors.newSingleThreadScheduledExecutor();

    @Before
    public void before() {
        workerConfig = new WorkerConfig();
        connectorTaskId = new ConnectorTaskId("testConnector", 1);
        sourceTask = new TestSourceTask();
        classLoader = this.getClass().getClassLoader();
        sinkTask = new TestSinkTask();
        connectKeyValue = new ConnectKeyValue();
        positionManagementService = new LocalPositionManagementServiceImpl();
        workerState.set(WorkerState.STARTED);
        connectStatsManager = new ConnectStatsManager(workerConfig);
        connectStatsService = new ConnectStatsService();
        keyValue = new DefaultKeyValue();
        plugin = new Plugin(Lists.newArrayList());
        transformChain = new TransformChain<>(keyValue, plugin);
        connectMetrics = new ConnectMetrics(workerConfig);
        errorMetricsGroup = new ErrorMetricsGroup(connectorTaskId, connectMetrics);
        stateManagementService = new LocalStateManagementServiceImpl();
        wrapperStatusListener = new WrapperStatusListener(stateManagementService, "defaultWorker1");
        retryWithToleranceOperator = new RetryWithToleranceOperator(1000, 1000, ToleranceType.ALL, errorMetricsGroup);

        workerDirectTask = new WorkerDirectTask(workerConfig,
            connectorTaskId,
            sourceTask,
            classLoader,
            sinkTask,
            connectKeyValue,
            positionManagementService,
            workerState,
            connectStatsManager,
            connectStatsService,
            transformChain,
            retryWithToleranceOperator,
            wrapperStatusListener,
            connectMetrics);

        workerDirectTask.doInitializeAndStart();
        Runnable runnable = () -> workerDirectTask.execute();
        executorService.submit(runnable);

    }

    @After
    public void after() {
        workerDirectTask.close();
    }

    @Test
    public void initializeAndStartTest() {
        Assertions.assertThatCode(() -> workerDirectTask.initializeAndStart()).doesNotThrowAnyException();
    }
}
