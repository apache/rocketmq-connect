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

import com.alibaba.fastjson.JSON;
import io.netty.util.concurrent.DefaultThreadFactory;
import io.netty.util.internal.ConcurrentSet;
import io.openmessaging.connector.api.component.connector.Connector;
import io.openmessaging.connector.api.component.task.Task;
import io.openmessaging.connector.api.component.task.sink.SinkTask;
import io.openmessaging.connector.api.component.task.source.SourceTask;
import io.openmessaging.connector.api.data.ConnectRecord;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

import io.openmessaging.connector.api.data.RecordConverter;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.client.consumer.DefaultMQPullConsumer;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.connect.runtime.controller.AbstractConnectController;
import org.apache.rocketmq.connect.runtime.common.ConnectKeyValue;
import org.apache.rocketmq.connect.runtime.common.LoggerName;
import org.apache.rocketmq.connect.runtime.config.ConnectConfig;
import org.apache.rocketmq.connect.runtime.config.RuntimeConfigDefine;
import org.apache.rocketmq.connect.runtime.errors.ReporterManagerUtil;
import org.apache.rocketmq.connect.runtime.errors.RetryWithToleranceOperator;
import org.apache.rocketmq.connect.runtime.service.ConfigManagementService;
import org.apache.rocketmq.connect.runtime.service.DefaultConnectorContext;
import org.apache.rocketmq.connect.runtime.service.PositionManagementService;
import org.apache.rocketmq.connect.runtime.service.TaskPositionCommitService;
import org.apache.rocketmq.connect.runtime.stats.ConnectStatsManager;
import org.apache.rocketmq.connect.runtime.stats.ConnectStatsService;
import org.apache.rocketmq.connect.runtime.utils.ConnectUtil;
import org.apache.rocketmq.connect.runtime.utils.ConnectorTaskId;
import org.apache.rocketmq.connect.runtime.utils.Plugin;
import org.apache.rocketmq.connect.runtime.utils.PluginClassLoader;
import org.apache.rocketmq.connect.runtime.utils.ServiceThread;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A worker to schedule all connectors and tasks in a process.
 */
public class Worker {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.ROCKETMQ_RUNTIME);

    /**
     * Current running connectors.
     */
    private Set<WorkerConnector> workingConnectors = new ConcurrentSet<>();

    /**
     * Current tasks state.
     */
    private Map<Runnable, Long/*timestamp*/> pendingTasks = new ConcurrentHashMap<>();
    private Set<Runnable> runningTasks = new ConcurrentSet<>();
    private Map<Runnable, Long/*timestamp*/> stoppingTasks = new ConcurrentHashMap<>();
    private Set<Runnable> stoppedTasks = new ConcurrentSet<>();
    private Set<Runnable> cleanedStoppedTasks = new ConcurrentSet<>();
    private Set<Runnable> errorTasks = new ConcurrentSet<>();
    private Set<Runnable> cleanedErrorTasks = new ConcurrentSet<>();

    Map<String, List<ConnectKeyValue>> latestTaskConfigs = new HashMap<>();
    /**
     * Current running tasks to its Future map.
     */
    private Map<Runnable, Future> taskToFutureMap = new ConcurrentHashMap<>();
    /**
     * Thread pool for connectors and tasks.
     */
    private final ExecutorService taskExecutor;
    /**
     * Position management for source tasks.
     */
    private final PositionManagementService positionManagementService;
    /**
     * A scheduled task to commit source position of source tasks.
     */
    private final TaskPositionCommitService taskPositionCommitService;

    private final ConnectConfig connectConfig;
    private final Plugin plugin;

    private static final int MAX_START_TIMEOUT_MILLS = 5000;
    private static final long MAX_STOP_TIMEOUT_MILLS = 20000;

    /**
     * Atomic state variable
     */
    private AtomicReference<WorkerState> workerState;
    private StateMachineService stateMachineService = new StateMachineService();
    private final ConnectStatsManager connectStatsManager;
    private final ConnectStatsService connectStatsService;

    public Worker(ConnectConfig connectConfig,
                  PositionManagementService positionManagementService,
                  ConfigManagementService configManagementService,
                  Plugin plugin, AbstractConnectController connectController) {
        this.connectConfig = connectConfig;
        this.taskExecutor = Executors.newCachedThreadPool(new DefaultThreadFactory("task-Worker-Executor-"));
        this.positionManagementService = positionManagementService;
        this.taskPositionCommitService = new TaskPositionCommitService(
            this,
            positionManagementService);
        this.plugin = plugin;
        this.connectStatsManager = connectController.getConnectStatsManager();
        this.connectStatsService = connectController.getConnectStatsService();
    }

    public void start() {
        workerState = new AtomicReference<>(WorkerState.STARTED);
        taskPositionCommitService.start();
        stateMachineService.start();
    }

    /**
     * Start a collection of connectors with the given configs. If a connector is already started with the same configs,
     * it will not start again. If a connector is already started but not contained in the new configs, it will stop.
     *
     * @param connectorConfigs
     * @param connectController
     * @throws Exception
     */
    public synchronized void startConnectors(Map<String, ConnectKeyValue> connectorConfigs,
        AbstractConnectController connectController) throws Exception {
        Set<WorkerConnector> stoppedConnector = new HashSet<>();
        for (WorkerConnector workerConnector : workingConnectors) {
            try {
                String connectorName = workerConnector.getConnectorName();
                ConnectKeyValue keyValue = connectorConfigs.get(connectorName);
                if (null == keyValue || 0 != keyValue.getInt(RuntimeConfigDefine.CONFIG_DELETED)) {
                    workerConnector.stop();
                    log.info("Connector {} stop", workerConnector.getConnectorName());
                    stoppedConnector.add(workerConnector);
                } else if (!keyValue.equals(workerConnector.getKeyValue())) {
                    workerConnector.reconfigure(keyValue);
                }
            } catch (Exception e) {
                log.error("stop or reconfigure connector error, connectName: " + workerConnector.getConnectorName(), e);
            }
        }
        workingConnectors.removeAll(stoppedConnector);

        if (null == connectorConfigs || 0 == connectorConfigs.size()) {
            return;
        }
        Map<String, ConnectKeyValue> newConnectors = new HashMap<>();
        for (String connectorName : connectorConfigs.keySet()) {
            boolean isNewConnector = true;
            for (WorkerConnector workerConnector : workingConnectors) {
                if (workerConnector.getConnectorName().equals(connectorName)) {
                    isNewConnector = false;
                    break;
                }
            }
            if (isNewConnector) {
                newConnectors.put(connectorName, connectorConfigs.get(connectorName));
            }
        }

        for (String connectorName : newConnectors.keySet()) {
            try {
                ConnectKeyValue keyValue = newConnectors.get(connectorName);
                String connectorClass = keyValue.getString(RuntimeConfigDefine.CONNECTOR_CLASS);
                ClassLoader loader = plugin.getPluginClassLoader(connectorClass);
                final ClassLoader currentThreadLoader = plugin.currentThreadLoader();
                Class clazz;
                boolean isolationFlag = false;
                if (loader instanceof PluginClassLoader) {
                    clazz = ((PluginClassLoader) loader).loadClass(connectorClass, false);
                    isolationFlag = true;
                } else {
                    clazz = Class.forName(connectorClass);
                }
                final Connector connector = (Connector) clazz.getDeclaredConstructor().newInstance();
                WorkerConnector workerConnector = new WorkerConnector(connectorName, connector, connectorConfigs.get(connectorName), new DefaultConnectorContext(connectorName, connectController));
                if (isolationFlag) {
                    Plugin.compareAndSwapLoaders(loader);
                }
                workerConnector.initialize();
                workerConnector.start();
                log.info("Connector {} start", workerConnector.getConnectorName());
                Plugin.compareAndSwapLoaders(currentThreadLoader);
                this.workingConnectors.add(workerConnector);
            } catch (Exception e) {
                log.error("worker connector start exception. workerName: " + connectorName, e);
            }
        }
    }

    /**
     * Start a collection of tasks with the given configs. If a task is already started with the same configs, it will
     * not start again. If a task is already started but not contained in the new configs, it will stop.
     *
     * @param taskConfigs
     * @throws Exception
     */
    public void startTasks(Map<String, List<ConnectKeyValue>> taskConfigs) {
        synchronized (latestTaskConfigs) {
            this.latestTaskConfigs = taskConfigs;
        }
    }

    private boolean isConfigInSet(ConnectKeyValue keyValue, Set<Runnable> set) {
        for (Runnable runnable : set) {
            ConnectKeyValue taskConfig = null;
            if (runnable instanceof WorkerSourceTask) {
                taskConfig = ((WorkerSourceTask) runnable).currentTaskConfig();
            } else if (runnable instanceof WorkerSinkTask) {
                taskConfig = ((WorkerSinkTask) runnable).currentTaskConfig();
            } else if (runnable instanceof WorkerDirectTask) {
                taskConfig = ((WorkerDirectTask) runnable).currentTaskConfig();
            }
            if (keyValue.equals(taskConfig)) {
                return true;
            }
        }
        return false;
    }

    /**
     * We can choose to persist in-memory task status
     * so we can view history tasks
     */
    public void stop() {
        workerState.set(WorkerState.TERMINATED);
        try {
            taskExecutor.awaitTermination(5000, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            log.error("Task termination error.", e);
        }
        stateMachineService.shutdown();
    }

    public Set<WorkerConnector> getWorkingConnectors() {
        return workingConnectors;
    }

    public void setWorkingConnectors(
        Set<WorkerConnector> workingConnectors) {
        this.workingConnectors = workingConnectors;
    }

    /**
     * Beaware that we are not creating a defensive copy of these tasks
     * So developers should only use these references for read-only purposes.
     * These variables should be immutable
     *
     * @return
     */
    public Set<Runnable> getWorkingTasks() {
        return runningTasks;
    }

    public Set<Runnable> getErrorTasks() {
        return errorTasks;
    }

    public Set<Runnable> getPendingTasks() {
        return pendingTasks.keySet();
    }

    public Set<Runnable> getStoppedTasks() {
        return stoppedTasks;
    }

    public Set<Runnable> getStoppingTasks() {
        return stoppingTasks.keySet();
    }

    public Set<Runnable> getCleanedErrorTasks() {
        return cleanedErrorTasks;
    }

    public Set<Runnable> getCleanedStoppedTasks() {
        return cleanedStoppedTasks;
    }

    public void setWorkingTasks(Set<Runnable> workingTasks) {
        this.runningTasks = workingTasks;
    }

    public void maintainConnectorState() {

    }

    /**
     * maintain task state
     * @throws Exception
     */
    public void maintainTaskState() throws Exception {
        Map<String, List<ConnectKeyValue>> taskConfigs = new HashMap<>();
        synchronized (latestTaskConfigs) {
            taskConfigs.putAll(latestTaskConfigs);
        }

        //  STEP 1: check running tasks and put to error status
        checkRunningTasks(taskConfigs);

        // get new Tasks
        Map<String, List<ConnectKeyValue>> newTasks = newTasks(taskConfigs);

        //  STEP 2: try to create new tasks
        startTask(newTasks);

        //  STEP 3: check all pending state
        checkPendingTask();

        //  STEP 4 check stopping tasks
        checkStoppingTasks();

        //  STEP 5 check error tasks
        checkErrorTasks();

        //  STEP 6 check errorTasks and stopped tasks
        checkStoppedTasks();
    }

    /**
     * check running task
     * @param taskConfigs
     */
    private void checkRunningTasks(Map<String, List<ConnectKeyValue>> taskConfigs) {
        boolean needCommitPosition = false;
        //  STEP 1: check running tasks and put to error status
        for (Runnable runnable : runningTasks) {
            WorkerTask workerTask = (WorkerTask) runnable;
            String connectorName = workerTask.id().connector();
            ConnectKeyValue taskConfig = workerTask.currentTaskConfig();
            List<ConnectKeyValue> keyValues = taskConfigs.get(connectorName);
            WorkerTaskState state = ((WorkerTask) runnable).getState();
            switch (state){
                case ERROR:
                    errorTasks.add(runnable);
                    runningTasks.remove(runnable);
                    break;
                case RUNNING:
                    boolean needStop = true;
                    if (null != keyValues && keyValues.size() > 0) {
                        for (ConnectKeyValue keyValue : keyValues) {
                            if (keyValue.equals(taskConfig)) {
                                needStop = false;
                                break;
                            }
                        }
                    }
                    if (needStop) {
                        try {
                            workerTask.close();
                        } catch (Exception e) {
                            log.error("workerTask stop exception, workerTask: " + workerTask.currentTaskConfig(), e);
                        }
                        log.info("Task stopping, connector name {}, config {}", workerTask.id().connector(), workerTask.currentTaskConfig());
                        runningTasks.remove(runnable);
                        stoppingTasks.put(runnable, System.currentTimeMillis());
                        needCommitPosition = true;
                    }
                    break;
                default:
                    log.error("[BUG] Illegal State in when checking running tasks, {} is in {} state",
                            ((WorkerTask) runnable).id().connector(), state.toString());
                    break;

            }
        }
        //If some tasks are closed, synchronize the position.
        if (needCommitPosition) {
            taskPositionCommitService.commitTaskPosition();
        }
    }

    private void checkStoppedTasks() {
        for (Runnable runnable : stoppedTasks) {
            WorkerTask workerTask = (WorkerTask) runnable;
            workerTask.cleanup();
            Future future = taskToFutureMap.get(runnable);
            try {
                if (null != future) {
                    future.get(1000, TimeUnit.MILLISECONDS);
                } else {
                    log.error("[BUG] stopped Tasks reference not found in taskFutureMap");
                }
            } catch (ExecutionException e) {
                Throwable t = e.getCause();
                log.info("[BUG] Stopped Tasks should not throw any exception");
                t.printStackTrace();
            } catch (CancellationException e) {
                log.info("[BUG] Stopped Tasks throws PrintStackTrace");
                e.printStackTrace();
            } catch (TimeoutException e) {
                log.info("[BUG] Stopped Tasks should not throw any exception");
                e.printStackTrace();
            } catch (InterruptedException e) {
                log.info("[BUG] Stopped Tasks should not throw any exception");
                e.printStackTrace();
            } finally {
                future.cancel(true);
                taskToFutureMap.remove(runnable);
                stoppedTasks.remove(runnable);
                cleanedStoppedTasks.add(runnable);
            }
        }
    }

    private void checkErrorTasks() {
        for (Runnable runnable : errorTasks) {
            WorkerTask workerTask = (WorkerTask) runnable;
            Future future = taskToFutureMap.get(runnable);
            try {
                if (null != future) {
                    future.get(1000, TimeUnit.MILLISECONDS);
                } else {
                    log.error("[BUG] errorTasks reference not found in taskFutureMap");
                }
            } catch (ExecutionException e) {
                log.error("Execution exception , {}", e);
            } catch (CancellationException | TimeoutException | InterruptedException e) {
                log.error("error, {}", e);
            } finally {
                future.cancel(true);
                workerTask.cleanup();
                taskToFutureMap.remove(runnable);
                errorTasks.remove(runnable);
                cleanedErrorTasks.add(runnable);
            }
        }
    }

    private void checkStoppingTasks() {
        for (Map.Entry<Runnable, Long> entry : stoppingTasks.entrySet()) {
            Runnable runnable = entry.getKey();
            Long stopTimestamp = entry.getValue();
            Long currentTimeMillis = System.currentTimeMillis();
            Future future = taskToFutureMap.get(runnable);
            WorkerTaskState state = ((WorkerTask) runnable).getState();
            // exited normally
            switch (state){
                case STOPPED:
                    // concurrent modification Exception ? Will it pop that in the
                    if (null == future || !future.isDone()) {
                        log.error("[BUG] future is null or Stopped task should have its Future.isDone() true, but false");
                    }
                    stoppingTasks.remove(runnable);
                    stoppedTasks.add(runnable);
                    break;
                case ERROR:
                    stoppingTasks.remove(runnable);
                    errorTasks.add(runnable);
                    break;
                case STOPPING:
                    if (currentTimeMillis - stopTimestamp > MAX_STOP_TIMEOUT_MILLS) {
                        ((WorkerTask) runnable).timeout();
                        stoppingTasks.remove(runnable);
                        errorTasks.add(runnable);
                    }
                    break;
                default:
                    log.error("[BUG] Illegal State in when checking stopping tasks, {} is in {} state",
                            ((WorkerTask) runnable).id().connector(), state.toString());
            }
        }
    }

    private void checkPendingTask() {
        for (Map.Entry<Runnable, Long> entry : pendingTasks.entrySet()) {
            Runnable runnable = entry.getKey();
            Long startTimestamp = entry.getValue();
            Long currentTimeMillis = System.currentTimeMillis();
            WorkerTaskState state = ((WorkerTask) runnable).getState();
            switch (state){
                case ERROR:
                    errorTasks.add(runnable);
                    pendingTasks.remove(runnable);
                    break;
                case RUNNING:
                    runningTasks.add(runnable);
                    pendingTasks.remove(runnable);
                    break;
                case NEW:
                    log.info("[RACE CONDITION] we checked the pending tasks before state turns to PENDING");
                    break;
                case PENDING:
                    if (currentTimeMillis - startTimestamp > MAX_START_TIMEOUT_MILLS) {
                        ((WorkerTask) runnable).timeout();
                        pendingTasks.remove(runnable);
                        errorTasks.add(runnable);
                    }
                default:
                    log.error("[BUG] Illegal State in when checking pending tasks, {} is in {} state",
                            ((WorkerTask) runnable).id().connector(), state.toString());
                    break;
            }
        }
    }

    /**
     * start task
     * @param newTasks
     * @throws Exception
     */
    private void startTask(Map<String, List<ConnectKeyValue>> newTasks) throws Exception {
        for (String connectorName : newTasks.keySet()) {
            int taskId = 0;
            for (ConnectKeyValue keyValue : newTasks.get(connectorName)) {
                ConnectorTaskId id = new ConnectorTaskId(connectorName,taskId);
                String taskType = keyValue.getString(RuntimeConfigDefine.TASK_TYPE);
                if (TaskType.DIRECT.name().equalsIgnoreCase(taskType)) {
                    createDirectTask(id, keyValue);
                    continue;
                }
                try {
                    String taskClass = keyValue.getString(RuntimeConfigDefine.TASK_CLASS);
                    ClassLoader loader = plugin.getPluginClassLoader(taskClass);
                    final ClassLoader currentThreadLoader = plugin.currentThreadLoader();
                    Class taskClazz;
                    boolean isolationFlag = false;
                    if (loader instanceof PluginClassLoader) {
                        taskClazz = ((PluginClassLoader) loader).loadClass(taskClass, false);
                        isolationFlag = true;
                    } else {
                        taskClazz = Class.forName(taskClass);
                    }
                    final Task task = (Task) taskClazz.getDeclaredConstructor().newInstance();
                    final String converterClazzName = keyValue.getString(RuntimeConfigDefine.SOURCE_RECORD_CONVERTER);
                    RecordConverter recordConverter = null;
                    if (StringUtils.isNotEmpty(converterClazzName)) {
                        recordConverter = Class.forName(converterClazzName)
                                .asSubclass(RecordConverter.class)
                                .getDeclaredConstructor()
                                .newInstance();
                        recordConverter.configure(keyValue.getProperties());
                    }

                    if (isolationFlag) {
                        Plugin.compareAndSwapLoaders(loader);
                    }
                    if (task instanceof SourceTask) {

                        DefaultMQProducer producer = ConnectUtil.initDefaultMQProducer(connectConfig);
                        TransformChain<ConnectRecord> transformChain = new TransformChain<>(keyValue, plugin);
                        // create retry operator
                        RetryWithToleranceOperator retryWithToleranceOperator = ReporterManagerUtil.createRetryWithToleranceOperator(keyValue);
                        retryWithToleranceOperator.reporters(ReporterManagerUtil.sourceTaskReporters(connectorName, keyValue));

                        WorkerSourceTask workerSourceTask = new WorkerSourceTask(id,
                            (SourceTask) task, loader, keyValue, positionManagementService, recordConverter, producer, workerState, connectStatsManager, connectStatsService, transformChain, retryWithToleranceOperator);
                        Plugin.compareAndSwapLoaders(currentThreadLoader);
                        Future future = taskExecutor.submit(workerSourceTask);
                        taskToFutureMap.put(workerSourceTask, future);
                        this.pendingTasks.put(workerSourceTask, System.currentTimeMillis());

                    } else if (task instanceof SinkTask) {
                        log.info("sink task config keyValue is {}", keyValue.getProperties());
                        DefaultMQPullConsumer consumer = ConnectUtil.initDefaultMQPullConsumer(connectConfig, connectorName, keyValue, ++taskId);
                        Set<String> consumerGroupSet = ConnectUtil.fetchAllConsumerGroupList(connectConfig);
                        if (!consumerGroupSet.contains(consumer.getConsumerGroup())) {
                            ConnectUtil.createSubGroup(connectConfig, consumer.getConsumerGroup());
                        }
                        TransformChain<ConnectRecord> transformChain = new TransformChain<>(keyValue, plugin);
                        // create retry operator
                        RetryWithToleranceOperator retryWithToleranceOperator = ReporterManagerUtil.createRetryWithToleranceOperator(keyValue);
                        retryWithToleranceOperator.reporters(ReporterManagerUtil.sinkTaskReporters(connectorName, keyValue, connectConfig));

                        WorkerSinkTask workerSinkTask = new WorkerSinkTask(id,
                            (SinkTask) task, loader, keyValue, recordConverter, consumer, workerState, connectStatsManager, connectStatsService, transformChain,
                                retryWithToleranceOperator, ReporterManagerUtil.createWorkerErrorRecordReporter(keyValue, retryWithToleranceOperator, recordConverter));
                        Plugin.compareAndSwapLoaders(currentThreadLoader);
                        Future future = taskExecutor.submit(workerSinkTask);
                        taskToFutureMap.put(workerSinkTask, future);
                        this.pendingTasks.put(workerSinkTask, System.currentTimeMillis());
                    }
                } catch (Exception e) {
                    log.error("start worker task exception. config {}" + JSON.toJSONString(keyValue), e);
                }
                taskId++;
            }
        }
    }

    @NotNull
    private Map<String, List<ConnectKeyValue>> newTasks(Map<String, List<ConnectKeyValue>> taskConfigs) {
        Map<String, List<ConnectKeyValue>> newTasks = new HashMap<>();
        for (String connectorName : taskConfigs.keySet()) {
            for (ConnectKeyValue keyValue : taskConfigs.get(connectorName)) {
                boolean isNewTask = true;
                if (isConfigInSet(keyValue, runningTasks) || isConfigInSet(keyValue, pendingTasks.keySet()) || isConfigInSet(keyValue, errorTasks)) {
                    isNewTask = false;
                }
                if (isNewTask) {
                    if (!newTasks.containsKey(connectorName)) {
                        newTasks.put(connectorName, new ArrayList<>());
                    }
                    log.info("Add new tasks,connector name {}, config {}", connectorName, keyValue);
                    newTasks.get(connectorName).add(keyValue);
                }
            }
        }
        return newTasks;
    }

    private void createDirectTask(ConnectorTaskId taskId, ConnectKeyValue keyValue) throws Exception {
        String sourceTaskClass = keyValue.getString(RuntimeConfigDefine.SOURCE_TASK_CLASS);
        Task sourceTask = getTask(sourceTaskClass);

        String sinkTaskClass = keyValue.getString(RuntimeConfigDefine.SINK_TASK_CLASS);
        Task sinkTask = getTask(sinkTaskClass);

        WorkerDirectTask workerDirectTask = new WorkerDirectTask(taskId,
            (SourceTask) sourceTask, (SinkTask) sinkTask, keyValue, positionManagementService, workerState);

        Future future = taskExecutor.submit(workerDirectTask);
        taskToFutureMap.put(workerDirectTask, future);
        this.pendingTasks.put(workerDirectTask, System.currentTimeMillis());
    }

    private Task getTask(String taskClass) throws Exception {
        ClassLoader loader = plugin.getPluginClassLoader(taskClass);
        final ClassLoader currentThreadLoader = plugin.currentThreadLoader();
        Class taskClazz;
        boolean isolationFlag = false;
        if (loader instanceof PluginClassLoader) {
            taskClazz = ((PluginClassLoader) loader).loadClass(taskClass, false);
            isolationFlag = true;
        } else {
            taskClazz = Class.forName(taskClass);
        }
        final Task task = (Task) taskClazz.getDeclaredConstructor().newInstance();
        if (isolationFlag) {
            Plugin.compareAndSwapLoaders(loader);
        }

        Plugin.compareAndSwapLoaders(currentThreadLoader);
        return task;
    }

    public class StateMachineService extends ServiceThread {
        @Override
        public void run() {
            log.info(this.getServiceName() + " service started");
            while (!this.isStopped()) {
                this.waitForRunning(1000);
                try {
                    Worker.this.maintainConnectorState();
                    Worker.this.maintainTaskState();
                } catch (Exception e) {
                    log.error("RebalanceImpl#StateMachineService start connector or task failed", e);
                }
            }
            log.info(this.getServiceName() + " service end");
        }

        @Override
        public String getServiceName() {
            return StateMachineService.class.getSimpleName();
        }
    }

    public enum TaskType {
        SOURCE,
        SINK,
        DIRECT;
    }
}
