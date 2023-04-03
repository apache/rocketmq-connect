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
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.rocketmq.connect.runtime.connectorwrapper;

import io.openmessaging.connector.api.component.connector.ConnectorContext;
import io.openmessaging.connector.api.data.ConnectRecord;
import io.openmessaging.internal.DefaultKeyValue;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.connect.runtime.common.ConnectKeyValue;
import org.apache.rocketmq.connect.runtime.config.ConnectorConfig;
import org.apache.rocketmq.connect.runtime.config.WorkerConfig;
import org.apache.rocketmq.connect.runtime.connectorwrapper.status.WrapperStatusListener;
import org.apache.rocketmq.connect.runtime.connectorwrapper.testimpl.TestConnector;
import org.apache.rocketmq.connect.runtime.connectorwrapper.testimpl.TestConverter;
import org.apache.rocketmq.connect.runtime.connectorwrapper.testimpl.TestPositionManageServiceImpl;
import org.apache.rocketmq.connect.runtime.connectorwrapper.testimpl.TestSinkTask;
import org.apache.rocketmq.connect.runtime.connectorwrapper.testimpl.TestSourceTask;
import org.apache.rocketmq.connect.runtime.controller.distributed.DistributedConnectController;
import org.apache.rocketmq.connect.runtime.controller.isolation.DelegatingClassLoader;
import org.apache.rocketmq.connect.runtime.controller.isolation.Plugin;
import org.apache.rocketmq.connect.runtime.controller.isolation.PluginClassLoader;
import org.apache.rocketmq.connect.runtime.converter.record.json.JsonConverter;
import org.apache.rocketmq.connect.runtime.errors.ErrorMetricsGroup;
import org.apache.rocketmq.connect.runtime.errors.ReporterManagerUtil;
import org.apache.rocketmq.connect.runtime.errors.RetryWithToleranceOperator;
import org.apache.rocketmq.connect.runtime.metrics.ConnectMetrics;
import org.apache.rocketmq.connect.runtime.service.ConfigManagementService;
import org.apache.rocketmq.connect.runtime.service.PositionManagementService;
import org.apache.rocketmq.connect.runtime.service.StateManagementService;
import org.apache.rocketmq.connect.runtime.service.local.LocalStateManagementServiceImpl;
import org.apache.rocketmq.connect.runtime.stats.ConnectStatsManager;
import org.apache.rocketmq.connect.runtime.stats.ConnectStatsService;
import org.apache.rocketmq.connect.runtime.utils.ConnectorTaskId;
import org.apache.rocketmq.connect.runtime.utils.TestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class WorkerTest {

    @Mock
    private PositionManagementService positionManagementService;

    @Mock
    private PositionManagementService offsetManagementService;

    @Mock
    private ConfigManagementService configManagementService;

    @Mock
    private DefaultMQProducer producer;

    private WorkerConfig connectConfig;

    private Worker worker;

    @Mock
    private Plugin plugin;

    @Mock
    private ConnectorContext connectorContext;

    @Mock
    private DistributedConnectController connectController;

    @Mock
    private ConnectStatsManager connectStatsManager;

    @Mock
    private ConnectStatsService connectStatsService;

    @Mock
    private DelegatingClassLoader delegatingClassLoader;

    @Mock
    private PluginClassLoader pluginClassLoader;

    private WrapperStatusListener wrapperStatusListener;

    private StateManagementService stateManagementService;

    private ServerResponseMocker nameServerMocker;

    private ServerResponseMocker brokerMocker;

    @Before
    public void init() {
        nameServerMocker = NameServerMocker.startByDefaultConf(9876, 10911);
        brokerMocker = ServerResponseMocker.startServer(10911, "Hello World".getBytes(StandardCharsets.UTF_8));
        when(plugin.currentThreadLoader()).thenReturn(pluginClassLoader);
        when(plugin.newConnector(any())).thenReturn(new TestConnector());
        when(plugin.delegatingLoader()).thenReturn(delegatingClassLoader);
        when(delegatingClassLoader.pluginClassLoader(any())).thenReturn(pluginClassLoader);
        Thread.currentThread().setContextClassLoader(pluginClassLoader);

        connectConfig = new WorkerConfig();
        connectConfig.setHttpPort(8081);
        connectConfig.setStorePathRootDir(System.getProperty("user.home") + File.separator + "testConnectorStore");
        connectConfig.setNamesrvAddr("localhost:9876");
        stateManagementService = new LocalStateManagementServiceImpl();
        stateManagementService.initialize(connectConfig, new TestConverter());
        worker = new Worker(connectConfig, positionManagementService, configManagementService, plugin, connectController, stateManagementService);

        Set<WorkerConnector> workingConnectors = new HashSet<>();
        for (int i = 0; i < 3; i++) {
            ConnectKeyValue connectKeyValue = new ConnectKeyValue();
            connectKeyValue.getProperties().put("key1", "TEST-CONN-" + i + "1");
            connectKeyValue.getProperties().put("key2", "TEST-CONN-" + i + "2");
            workingConnectors.add(new WorkerConnector("TEST-CONN-" + i, new TestConnector(), connectKeyValue, connectorContext, null, null));
        }
        worker.setWorkingConnectors(workingConnectors);
        assertThat(worker.getWorkingConnectors().size()).isEqualTo(3);
        TransformChain<ConnectRecord> transformChain = new TransformChain<ConnectRecord>(new DefaultKeyValue(), plugin);
        Set<Runnable> runnables = new HashSet<>();

        wrapperStatusListener = new WrapperStatusListener(stateManagementService, "worker1");
        for (int i = 0; i < 3; i++) {
            ConnectKeyValue connectKeyValue = new ConnectKeyValue();
            connectKeyValue.getProperties().put("key1", "TEST-TASK-" + i + "1");
            connectKeyValue.getProperties().put("key2", "TEST-TASK-" + i + "2");

            // create retry operator
            RetryWithToleranceOperator retryWithToleranceOperator = ReporterManagerUtil.createRetryWithToleranceOperator(connectKeyValue, new ErrorMetricsGroup(new ConnectorTaskId(), new ConnectMetrics(new WorkerConfig())));
            retryWithToleranceOperator.reporters(ReporterManagerUtil.sourceTaskReporters(new ConnectorTaskId("TEST-CONN", 1), connectKeyValue, new ErrorMetricsGroup(new ConnectorTaskId("TEST-CONN", 1), new ConnectMetrics(new WorkerConfig()))));
            final WorkerSourceTask task = new WorkerSourceTask(new WorkerConfig(),
                    new ConnectorTaskId("TEST-CONN-" + i, i),
                    new TestSourceTask(),
                    null,
                    connectKeyValue,
                    new TestPositionManageServiceImpl(),
                    new JsonConverter(),
                    new JsonConverter(),
                    producer,
                    new AtomicReference(WorkerState.STARTED),
                    connectStatsManager, connectStatsService,
                    transformChain,
                    retryWithToleranceOperator, wrapperStatusListener, new ConnectMetrics(new WorkerConfig()));
            runnables.add(task);
        }
        worker.setWorkingTasks(runnables);
        assertThat(worker.getWorkingTasks().size()).isEqualTo(3);

        worker.start();
    }

    @After
    public void destroy() throws InterruptedException {
        TimeUnit.SECONDS.sleep(2);
        worker.stop();
        TestUtils.deleteFile(new File(System.getProperty("user.home") + File.separator + "testConnectorStore"));
        brokerMocker.shutdown();
        nameServerMocker.shutdown();

    }

    @Test
    public void testStartConnectors() throws Exception {
        Map<String, ConnectKeyValue> connectorConfigs = new HashMap<>();
        for (int i = 0; i < 3; i++) {
            ConnectKeyValue connectKeyValue = new ConnectKeyValue();
            connectKeyValue.setTargetState(TargetState.STARTED);
            connectKeyValue.getProperties().put("key1", "TEST-CONN-" + i + "1");
            connectKeyValue.getProperties().put("key2", "TEST-CONN-" + i + "2");
            connectKeyValue.getProperties().put(ConnectorConfig.CONNECTOR_CLASS, TestConnector.class.getName());
            connectorConfigs.put("TEST-CONN-" + i, connectKeyValue);
        }

        worker.startConnectors(connectorConfigs, connectController);
        Set<WorkerConnector> connectors = worker.getWorkingConnectors();
        assertThat(connectors.size()).isEqualTo(3);
        for (WorkerConnector wc : connectors) {
            assertThat(wc.getConnectorName()).isIn("TEST-CONN-0", "TEST-CONN-1", "TEST-CONN-2");
        }
    }

    @Test
    public void testStartTasks() {
        Map<String, List<ConnectKeyValue>> taskConfigs = new HashMap<>();
        for (int i = 1; i < 4; i++) {
            List<ConnectKeyValue> connectKeyValues = new ArrayList<>();
            ConnectKeyValue connectKeyValue = new ConnectKeyValue();
            connectKeyValue.getProperties().put("key1", "TEST-CONN-" + i + "1");
            connectKeyValue.getProperties().put("key2", "TEST-CONN-" + i + "2");
            if (i == 1) {
                // start direct task
                connectKeyValue.getProperties().put(ConnectorConfig.TASK_TYPE, Worker.TaskType.DIRECT.name());
                connectKeyValue.getProperties().put(ConnectorConfig.SOURCE_TASK_CLASS, TestSourceTask.class.getName());
                connectKeyValue.getProperties().put(ConnectorConfig.SINK_TASK_CLASS, TestSinkTask.class.getName());
            } else {
                connectKeyValue.getProperties().put(ConnectorConfig.TASK_TYPE, Worker.TaskType.SOURCE.name());
            }
            connectKeyValue.getProperties().put(ConnectorConfig.TASK_CLASS, TestSourceTask.class.getName());
            connectKeyValue.getProperties().put(ConnectorConfig.VALUE_CONVERTER, TestConverter.class.getName());
            connectKeyValues.add(connectKeyValue);
            taskConfigs.put("TEST-CONN-" + i, connectKeyValues);
        }

        worker.startTasks(taskConfigs);

        Set<Runnable> sourceTasks = worker.getWorkingTasks();
        assertThat(sourceTasks.size()).isEqualTo(3);
        for (Runnable runnable : sourceTasks) {
            WorkerSourceTask workerSourceTask = null;
            WorkerSinkTask workerSinkTask = null;
            if (runnable instanceof WorkerSourceTask) {
                workerSourceTask = (WorkerSourceTask) runnable;
            } else {
                workerSinkTask = (WorkerSinkTask) runnable;
            }
            String connectorName = null != workerSourceTask ? workerSourceTask.id().connector() : workerSinkTask.id().connector();
            assertThat(connectorName).isIn("TEST-CONN-0", "TEST-CONN-1", "TEST-CONN-2", "TEST-CONN-3");
        }
    }
}

