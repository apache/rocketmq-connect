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
import io.openmessaging.connector.api.data.RecordConverter;
import io.openmessaging.connector.api.errors.ConnectException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.client.consumer.DefaultLitePullConsumer;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.connect.runtime.common.ConnectKeyValue;
import org.apache.rocketmq.connect.runtime.common.LoggerName;
import org.apache.rocketmq.connect.runtime.config.ConnectorConfig;
import org.apache.rocketmq.connect.runtime.config.SinkConnectorConfig;
import org.apache.rocketmq.connect.runtime.config.WorkerConfig;
import org.apache.rocketmq.connect.runtime.connectorwrapper.status.ConnectorStatus;
import org.apache.rocketmq.connect.runtime.connectorwrapper.status.TaskStatus;
import org.apache.rocketmq.connect.runtime.connectorwrapper.status.WrapperStatusListener;
import org.apache.rocketmq.connect.runtime.controller.AbstractConnectController;
import org.apache.rocketmq.connect.runtime.controller.isolation.Plugin;
import org.apache.rocketmq.connect.runtime.errors.ErrorMetricsGroup;
import org.apache.rocketmq.connect.runtime.errors.ReporterManagerUtil;
import org.apache.rocketmq.connect.runtime.errors.RetryWithToleranceOperator;
import org.apache.rocketmq.connect.runtime.metrics.ConnectMetrics;
import org.apache.rocketmq.connect.runtime.service.ConfigManagementService;
import org.apache.rocketmq.connect.runtime.service.DefaultConnectorContext;
import org.apache.rocketmq.connect.runtime.service.PositionManagementService;
import org.apache.rocketmq.connect.runtime.service.StateManagementService;
import org.apache.rocketmq.connect.runtime.stats.ConnectStatsManager;
import org.apache.rocketmq.connect.runtime.stats.ConnectStatsService;
import org.apache.rocketmq.connect.runtime.utils.Callback;
import org.apache.rocketmq.connect.runtime.utils.ConnectUtil;
import org.apache.rocketmq.connect.runtime.utils.ConnectorTaskId;
import org.apache.rocketmq.connect.runtime.utils.ServiceThread;
import org.apache.rocketmq.connect.runtime.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.rocketmq.connect.runtime.connectorwrapper.status.AbstractStatus.State.PAUSED;
import static org.apache.rocketmq.connect.runtime.connectorwrapper.status.AbstractStatus.State.RUNNING;

/**
 * A worker to schedule all connectors and tasks in a process.
 */
public class Worker {
    public static final long CONNECTOR_GRACEFUL_SHUTDOWN_TIMEOUT_MS = TimeUnit.SECONDS.toMillis(5);
    private static final Logger log = LoggerFactory.getLogger(LoggerName.ROCKETMQ_RUNTIME);
    /**
     * Thread pool for connectors and tasks.
     */
    private final ExecutorService taskExecutor;
    /**
     * Position management for source tasks.
     */
    private final PositionManagementService positionManagementService;
    private final WorkerConfig workerConfig;
    private final Plugin plugin;
    private final ConnectStatsManager connectStatsManager;
    private final ConnectStatsService connectStatsService;
    private final WrapperStatusListener statusListener;
    private final ConcurrentMap<String, WorkerConnector> connectors = new ConcurrentHashMap<>();
    private final ExecutorService executor;
    private final ConfigManagementService configManagementService;
    private final ConnectMetrics connectMetrics;
    Map<String, List<ConnectKeyValue>> latestTaskConfigs = new HashMap<>();

    /**
     * Current tasks state.
     */
    private final Map<Runnable, Long/*timestamp*/> pendingTasks = new ConcurrentHashMap<>();
    private Set<Runnable> runningTasks = new ConcurrentSet<>();
    private final Map<Runnable, Long/*timestamp*/> stoppingTasks = new ConcurrentHashMap<>();
    private final Set<Runnable> stoppedTasks = new ConcurrentSet<>();
    private final Set<Runnable> cleanedStoppedTasks = new ConcurrentSet<>();
    private final Set<Runnable> errorTasks = new ConcurrentSet<>();
    private final Set<Runnable> cleanedErrorTasks = new ConcurrentSet<>();
    /**
     * Current running tasks to its Future map.
     */
    private final Map<Runnable, Future> taskToFutureMap = new ConcurrentHashMap<>();
    private final Optional<SourceTaskOffsetCommitter> sourceTaskOffsetCommitter;
    /**
     * Atomic state variable
     */
    private AtomicReference<WorkerState> workerState;
    private final StateMachineService stateMachineService = new StateMachineService();

    private final StateManagementService stateManagementService;


    public Worker(WorkerConfig workerConfig,
                  PositionManagementService positionManagementService,
                  ConfigManagementService configManagementService,
                  Plugin plugin,
                  AbstractConnectController connectController,
                  StateManagementService stateManagementService) {
        this.workerConfig = workerConfig;
        this.taskExecutor = Executors.newCachedThreadPool(new DefaultThreadFactory("task-Worker-Executor-"));
        this.configManagementService = configManagementService;
        this.positionManagementService = positionManagementService;
        this.plugin = plugin;
        this.connectStatsManager = connectController.getConnectStatsManager();
        this.connectStatsService = connectController.getConnectStatsService();
        this.sourceTaskOffsetCommitter = Optional.of(new SourceTaskOffsetCommitter(workerConfig));
        this.statusListener = new WrapperStatusListener(stateManagementService, workerConfig.getWorkerId());
        this.executor = Executors.newCachedThreadPool();
        this.connectMetrics = new ConnectMetrics(workerConfig);
        this.stateManagementService = stateManagementService;
    }

    public void start() {
        workerState = new AtomicReference<>(WorkerState.STARTED);
        stateMachineService.start();
    }


    /**
     * Stop asynchronously a collection of connectors that belong to this worker and await their
     * termination.
     *
     * @param ids the collection of connectors to be stopped.
     */
    public void stopAndAwaitConnectors(Collection<String> ids) {
        stopConnectors(ids);
        awaitStopConnectors(ids);
    }

    /**
     * Stop a connector that belongs to this worker and await its termination.
     *
     * @param connName the name of the connector to be stopped.
     */
    public void stopAndAwaitConnector(String connName) {
        stopConnector(connName);
        awaitStopConnectors(Collections.singletonList(connName));
    }


    private void awaitStopConnector(String connName, long timeout) {
        WorkerConnector connector = connectors.remove(connName);
        if (connector == null) {
            log.warn("Ignoring await stop request for non-present connector {}", connName);
            return;
        }

        if (!connector.awaitShutdown(timeout)) {
            log.error("Connector '{}' failed to properly shut down, has become unresponsive, and "
                    + "may be consuming external resources. Correct the configuration for "
                    + "this connector or remove the connector. After fixing the connector, it "
                    + "may be necessary to restart this worker to release any consumed "
                    + "resources.", connName);
            connector.cancel();
        } else {
            log.debug("Graceful stop of connector {} succeeded.", connName);
        }
    }

    private void awaitStopConnectors(Collection<String> ids) {
        long now = System.currentTimeMillis();
        long deadline = now + CONNECTOR_GRACEFUL_SHUTDOWN_TIMEOUT_MS;
        for (String id : ids) {
            long remaining = Math.max(0, deadline - System.currentTimeMillis());
            awaitStopConnector(id, remaining);
        }
    }

    /**
     * Stop asynchronously all the worker's connectors and await their termination.
     */
    public void stopAndAwaitConnectors() {
        stopAndAwaitConnectors(new ArrayList<>(connectors.keySet()));
    }


    /**
     * stop connectors
     *
     * @param ids
     */
    private void stopConnectors(Collection<String> ids) {
        for (String connectorName : ids) {
            stopConnector(connectorName);
        }
    }

    /**
     * Stop a connector managed by this worker.
     *
     * @param connName the connector name.
     */
    private void stopConnector(String connName) {
        WorkerConnector workerConnector = connectors.get(connName);
        log.info("Stopping connector {}", connName);
        if (workerConnector == null) {
            log.warn("Ignoring stop request for unowned connector {}", connName);
            return;
        }
        workerConnector.shutdown();
    }


    /**
     * check and stop connectors
     *
     * @param assigns
     */
    private void checkAndStopConnectors(Collection<String> assigns) {
        Set<String> connectors = this.connectors.keySet();
        if (CollectionUtils.isEmpty(assigns)) {
            // delete all
            for (String connector : connectors) {
                log.info("It may be that the load balancing assigns this connector to other nodes,connector {}", connector);
                stopAndAwaitConnector(connector);
            }
            return;
        }
        for (String connectorName : connectors) {
            if (!assigns.contains(connectorName)) {
                log.info("It may be that the load balancing assigns this connector to other nodes,connector {}", connectorName);
                stopAndAwaitConnector(connectorName);
            }
        }
    }


    /**
     * check and reconfigure connectors
     *
     * @param assigns
     */
    private void checkAndReconfigureConnectors(Map<String, ConnectKeyValue> assigns) {
        if (assigns == null || assigns.isEmpty()) {
            return;
        }
        for (String connectName : assigns.keySet()) {
            if (!connectors.containsKey(connectName)) {
                // new
                continue;
            }
            WorkerConnector connector = connectors.get(connectName);
            ConnectKeyValue oldConfig = connector.getKeyValue();
            ConnectKeyValue newConfig = assigns.get(connectName);
            if (!oldConfig.equals(newConfig)) {
                connector.reconfigure(newConfig);
            }
        }
    }


    /**
     * check and transition  to connectors
     *
     * @param assigns
     */
    private void checkAndTransitionToConnectors(Map<String, ConnectKeyValue> assigns) {
        if (assigns == null || assigns.isEmpty()) {
            return;
        }
        for (String connectName : assigns.keySet()) {
            if (!connectors.containsKey(connectName)) {
                // new
                continue;
            }
            WorkerConnector connector = connectors.get(connectName);
            ConnectKeyValue newConfig = assigns.get(connectName);
            connector.transitionTo(newConfig.getTargetState(), new Callback<TargetState>() {
                @Override
                public void onCompletion(Throwable error, TargetState result) {
                    if (error != null) {
                        log.error(error.getMessage());
                    } else {
                        if (newConfig.getTargetState() != result) {
                            log.info("Connector {} set target state {} successed!!", connectName, result);
                        }
                    }
                }
            });
        }
    }


    /**
     * check and new connectors
     *
     * @param assigns
     */
    private Map<String, ConnectKeyValue> checkAndNewConnectors(Map<String, ConnectKeyValue> assigns) {
        if (assigns == null || assigns.isEmpty()) {
            return new HashMap<>();
        }

        Map<String, ConnectKeyValue> newConnectors = new HashMap<>();
        for (String connectName : assigns.keySet()) {
            if (!connectors.containsKey(connectName)) {
                newConnectors.put(connectName, assigns.get(connectName));
            }
        }
        return newConnectors;
    }


    /**
     * assign connector
     * <p>
     * Start a collection of connectors with the given configs. If a connector is already started with the same configs,
     * it will not start again. If a connector is already started but not contained in the new configs, it will stop.
     *
     * @param connectorConfigs
     * @param connectController
     * @throws Exception
     */
    public synchronized void startConnectors(Map<String, ConnectKeyValue> connectorConfigs,
                                             AbstractConnectController connectController) throws Exception {

        // Step 1: Check and stop connectors
        checkAndStopConnectors(connectorConfigs.keySet());

        // Step 2: Check config update
        checkAndReconfigureConnectors(connectorConfigs);

        // Step 3: check new
        Map<String, ConnectKeyValue> newConnectors = checkAndNewConnectors(connectorConfigs);

        //Step 4: start connectors
        for (String connectorName : newConnectors.keySet()) {
            ClassLoader savedLoader = plugin.currentThreadLoader();
            try {
                ConnectKeyValue keyValue = newConnectors.get(connectorName);
                String connectorClass = keyValue.getString(ConnectorConfig.CONNECTOR_CLASS);
                ClassLoader connectorLoader = plugin.delegatingLoader().pluginClassLoader(connectorClass);
                savedLoader = Plugin.compareAndSwapLoaders(connectorLoader);

                // instance connector
                final Connector connector = plugin.newConnector(connectorClass);
                WorkerConnector workerConnector = new WorkerConnector(
                        connectorName,
                        connector,
                        connectorConfigs.get(connectorName),
                        new DefaultConnectorContext(connectorName, connectController),
                        statusListener,
                        savedLoader
                );
                // initinal target state
                executor.submit(workerConnector);
                workerConnector.transitionTo(keyValue.getTargetState(), new Callback<TargetState>() {
                    @Override
                    public void onCompletion(Throwable error, TargetState result) {
                        if (error != null) {
                            log.error(error.getMessage());
                        } else {
                            log.info("Start connector {} and set target state {} successed!!", connectorName, result);
                        }
                    }
                });
                log.info("Connector {} start", workerConnector.getConnectorName());
                Plugin.compareAndSwapLoaders(savedLoader);
                this.connectors.put(connectorName, workerConnector);
            } catch (Exception e) {
                Plugin.compareAndSwapLoaders(savedLoader);
                log.error("worker connector start exception. workerName: " + connectorName, e);
            } finally {
                // compare and swap
                Plugin.compareAndSwapLoaders(savedLoader);
            }
        }

        // Step 3: check and transition to connectors
        checkAndTransitionToConnectors(connectorConfigs);
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

        // stop and await connectors
        if (!connectors.isEmpty()) {
            log.warn("Shutting down connectors {} uncleanly; herder should have shut down connectors before the Worker is stopped", connectors.keySet());
            stopAndAwaitConnectors();
        }
        executor.shutdown();

        // stop connectors
        if (workerState != null) {
            workerState.set(WorkerState.TERMINATED);
        }
        Set<Runnable> runningTasks = this.runningTasks;
        for (Runnable task : runningTasks) {
            awaitStopTask((WorkerTask) task, 5000);
        }
        taskExecutor.shutdown();

        // close offset committer
        sourceTaskOffsetCommitter.ifPresent(committer -> committer.close(5000));

        stateMachineService.shutdown();

        try {
            // close metrics
            connectMetrics.close();
        } catch (Exception e) {

        }
    }

    private void awaitStopTask(WorkerTask task, long timeout) {
        if (task == null) {
            log.warn("Ignoring await stop request for non-present task {}", task.id());
            return;
        }
        if (!task.awaitStop(timeout)) {
            log.error("Graceful stop of task {} failed.", task.id());
            task.doClose();
        } else {
            log.debug("Graceful stop of task {} succeeded.", task.id());
        }
    }

    /**
     * get connectors
     *
     * @return
     */
    public Set<String> allocatedConnectors() {
        return new HashSet<>(connectors.keySet());
    }

    public ConcurrentMap<String, WorkerConnector> getConnectors() {
        return connectors;
    }

    /**
     * get connectors
     *
     * @return
     */
    public Map<String, List<ConnectKeyValue>> allocatedTasks() {
        return latestTaskConfigs;
    }


    public Set<WorkerConnector> getWorkingConnectors() {
        return new HashSet<>(connectors.values());
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

    public void setWorkingTasks(Set<Runnable> workingTasks) {
        this.runningTasks = workingTasks;
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

    public void maintainConnectorState() {

    }

    /**
     * maintain task state
     *
     * @throws Exception
     */
    public void maintainTaskState() throws Exception {
        Map<String, List<ConnectKeyValue>> connectorConfig = new HashMap<>();
        synchronized (latestTaskConfigs) {
            connectorConfig.putAll(latestTaskConfigs);
        }

        //  STEP 0 cleaned error Stopped Task
        clearErrorOrStopedTask();

        //  STEP 1: check running tasks and put to error status
        checkRunningTasks(connectorConfig);

        // get new Tasks
        Map<String, List<ConnectKeyValue>> newTasks = newTasks(connectorConfig);

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

    private void clearErrorOrStopedTask() {
        if (cleanedStoppedTasks.size() >= 10) {
            log.info("clean cleanedStoppedTasks from mem. {}", JSON.toJSONString(cleanedStoppedTasks));
            for (Runnable task : cleanedStoppedTasks) {
                taskToFutureMap.remove(task);
            }
            cleanedStoppedTasks.clear();
        }
        if (cleanedErrorTasks.size() >= 10) {
            log.info("clean cleanedErrorTasks from mem. {}", JSON.toJSONString(cleanedErrorTasks));
            for (Runnable task : cleanedErrorTasks) {
                taskToFutureMap.remove(task);
            }
            cleanedErrorTasks.clear();
        }

    }

    /**
     * check running task
     *
     * @param connectorConfig
     */
    private void checkRunningTasks(Map<String, List<ConnectKeyValue>> connectorConfig) {
        //  STEP 1: check running tasks and put to error status
        for (Runnable runnable : runningTasks) {
            WorkerTask workerTask = (WorkerTask) runnable;
            String connectorName = workerTask.id().connector();
            ConnectKeyValue taskConfig = workerTask.currentTaskConfig();
            List<ConnectKeyValue> taskConfigs = connectorConfig.get(connectorName);
            WorkerTaskState state = ((WorkerTask) runnable).getState();
            switch (state) {
                case ERROR:
                    errorTasks.add(runnable);
                    runningTasks.remove(runnable);
                    break;
                case RUNNING:
                    if (isNeedStop(taskConfig, taskConfigs)) {
                        try {
                            // remove committer offset
                            sourceTaskOffsetCommitter.ifPresent(commiter -> commiter.remove(workerTask.id()));
                            workerTask.doClose();
                        } catch (Exception e) {
                            log.error("workerTask stop exception, workerTask: " + workerTask.currentTaskConfig(), e);
                        }
                        log.info("Task stopping, connector name {}, config {}", workerTask.id().connector(), workerTask.currentTaskConfig());
                        runningTasks.remove(runnable);
                        stoppingTasks.put(runnable, System.currentTimeMillis());
                    } else {
                        //status redress
                        redressRunningStatus(workerTask);
                        // set target state
                        TargetState targetState = configManagementService.snapshot().targetState(connectorName);
                        if (targetState != null) {
                            workerTask.transitionTo(targetState);
                        }
                    }
                    break;
                default:
                    log.error("[BUG] Illegal State in when checking running tasks, {} is in {} state",
                            ((WorkerTask) runnable).id().connector(), state);
                    break;
            }
        }
    }

    /**
     * check is need stop
     *
     * @param taskConfig
     * @param keyValues
     * @return
     */
    private boolean isNeedStop(ConnectKeyValue taskConfig, List<ConnectKeyValue> keyValues) {
        if (CollectionUtils.isEmpty(keyValues)) {
            return true;
        }
        for (ConnectKeyValue keyValue : keyValues) {
            if (keyValue.equals(taskConfig)) {
                // not stop
                return false;
            }
        }
        return true;
    }

    /**
     * check stopped tasks
     */
    private void checkStoppedTasks() {
        for (Runnable runnable : stoppedTasks) {
            WorkerTask workerTask = (WorkerTask) runnable;
            Future future = taskToFutureMap.get(runnable);
            try {
                if (null != future) {
                    future.get(workerConfig.getMaxStartTimeoutMills(), TimeUnit.MILLISECONDS);
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
                // remove committer offset
                sourceTaskOffsetCommitter.ifPresent(commiter -> commiter.remove(workerTask.id()));
                workerTask.cleanup();
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
                    future.get(workerConfig.getMaxStopTimeoutMills(), TimeUnit.MILLISECONDS);
                } else {
                    log.error("[BUG] errorTasks reference not found in taskFutureMap");
                }
            } catch (ExecutionException e) {
                log.error("Execution exception , {}", e);
            } catch (CancellationException | TimeoutException | InterruptedException e) {
                log.error("error, {}", e);
            } finally {
                // remove committer offset
                sourceTaskOffsetCommitter.ifPresent(commiter -> commiter.remove(workerTask.id()));
                workerTask.cleanup();
                future.cancel(true);
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
            switch (state) {
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
                    if (currentTimeMillis - stopTimestamp > workerConfig.getMaxStopTimeoutMills()) {
                        ((WorkerTask) runnable).timeout();
                        stoppingTasks.remove(runnable);
                        errorTasks.add(runnable);
                    }
                    break;
                default:
                    log.error("[BUG] Illegal State in when checking stopping tasks, {} is in {} state",
                            ((WorkerTask) runnable).id().connector(), state);
            }
        }
    }

    private void checkPendingTask() {
        for (Map.Entry<Runnable, Long> entry : pendingTasks.entrySet()) {
            Runnable runnable = entry.getKey();
            Long startTimestamp = entry.getValue();
            Long currentTimeMillis = System.currentTimeMillis();
            WorkerTaskState state = ((WorkerTask) runnable).getState();
            switch (state) {
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
                    if (currentTimeMillis - startTimestamp > workerConfig.getMaxStartTimeoutMills()) {
                        ((WorkerTask) runnable).timeout();
                        pendingTasks.remove(runnable);
                        errorTasks.add(runnable);
                    }
                    break;
                default:
                    log.error("[BUG] Illegal State in when checking pending tasks, {} is in {} state",
                            ((WorkerTask) runnable).id().connector(), state);
                    break;
            }
        }
    }

    /**
     * start task
     *
     * @param newTasks
     * @throws Exception
     */
    private void startTask(Map<String, List<ConnectKeyValue>> newTasks) throws Exception {
        for (String connectorName : newTasks.keySet()) {
            for (ConnectKeyValue keyValue : newTasks.get(connectorName)) {
                int taskId = keyValue.getInt(ConnectorConfig.TASK_ID);
                ConnectorTaskId id = new ConnectorTaskId(connectorName, taskId);

                ErrorMetricsGroup errorMetricsGroup = new ErrorMetricsGroup(id, this.connectMetrics);

                String taskType = keyValue.getString(ConnectorConfig.TASK_TYPE);
                if (TaskType.DIRECT.name().equalsIgnoreCase(taskType)) {
                    createDirectTask(id, keyValue);
                    continue;
                }

                ClassLoader savedLoader = plugin.currentThreadLoader();
                try {
                    String connType = keyValue.getString(ConnectorConfig.CONNECTOR_CLASS);
                    ClassLoader connectorLoader = plugin.delegatingLoader().connectorLoader(connType);
                    savedLoader = Plugin.compareAndSwapLoaders(connectorLoader);
                    // new task
                    final Class<? extends Task> taskClass = plugin.currentThreadLoader().loadClass(keyValue.getString(ConnectorConfig.TASK_CLASS)).asSubclass(Task.class);
                    final Task task = plugin.newTask(taskClass);

                    /**
                     * create key/value converter
                     */
                    RecordConverter valueConverter = plugin.newConverter(keyValue, false, ConnectorConfig.VALUE_CONVERTER, workerConfig.getValueConverter(), Plugin.ClassLoaderUsage.CURRENT_CLASSLOADER);
                    RecordConverter keyConverter = plugin.newConverter(keyValue, true, ConnectorConfig.KEY_CONVERTER, workerConfig.getKeyConverter(), Plugin.ClassLoaderUsage.CURRENT_CLASSLOADER);

                    if (keyConverter == null) {
                        keyConverter = plugin.newConverter(keyValue, true, ConnectorConfig.KEY_CONVERTER, workerConfig.getValueConverter(), Plugin.ClassLoaderUsage.PLUGINS);
                        log.info("Set up the key converter {} for task {} using the worker config", keyConverter.getClass(), id);
                    } else {
                        log.info("Set up the key converter {} for task {} using the connector config", keyConverter.getClass(), id);
                    }
                    if (valueConverter == null) {
                        valueConverter = plugin.newConverter(keyValue, false, ConnectorConfig.VALUE_CONVERTER, workerConfig.getKeyConverter(), Plugin.ClassLoaderUsage.PLUGINS);
                        log.info("Set up the value converter {} for task {} using the worker config", valueConverter.getClass(), id);
                    } else {
                        log.info("Set up the value converter {} for task {} using the connector config", valueConverter.getClass(), id);
                    }

                    if (task instanceof SourceTask) {
                        DefaultMQProducer producer = ConnectUtil.initDefaultMQProducer(workerConfig);
                        TransformChain<ConnectRecord> transformChain = new TransformChain<>(keyValue, plugin);
                        // create retry operator
                        RetryWithToleranceOperator retryWithToleranceOperator = ReporterManagerUtil.createRetryWithToleranceOperator(keyValue, errorMetricsGroup);
                        retryWithToleranceOperator.reporters(ReporterManagerUtil.sourceTaskReporters(id, keyValue, errorMetricsGroup));

                        WorkerSourceTask workerSourceTask = new WorkerSourceTask(workerConfig, id,
                                (SourceTask) task, savedLoader, keyValue, positionManagementService, keyConverter, valueConverter, producer, workerState, connectStatsManager, connectStatsService, transformChain, retryWithToleranceOperator, statusListener, this.connectMetrics);

                        Future future = taskExecutor.submit(workerSourceTask);
                        // schedule offset committer
                        sourceTaskOffsetCommitter.ifPresent(committer -> committer.schedule(id, workerSourceTask));

                        taskToFutureMap.put(workerSourceTask, future);
                        this.pendingTasks.put(workerSourceTask, System.currentTimeMillis());

                    } else if (task instanceof SinkTask) {
                        log.info("sink task config keyValue is {}", keyValue.getProperties());
                        DefaultLitePullConsumer consumer = ConnectUtil.initDefaultLitePullConsumer(workerConfig, false);
                        // set consumer groupId
                        String groupId = keyValue.getString(SinkConnectorConfig.TASK_GROUP_ID);
                        if (StringUtils.isBlank(groupId)) {
                            groupId = ConnectUtil.SYS_TASK_CG_PREFIX + id.connector();
                        }
                        consumer.setConsumerGroup(groupId);
                        Set<String> consumerGroupSet = ConnectUtil.fetchAllConsumerGroupList(workerConfig);
                        if (!consumerGroupSet.contains(consumer.getConsumerGroup())) {
                            ConnectUtil.createSubGroup(workerConfig, consumer.getConsumerGroup());
                        }
                        TransformChain<ConnectRecord> transformChain = new TransformChain<>(keyValue, plugin);
                        // create retry operator
                        RetryWithToleranceOperator retryWithToleranceOperator = ReporterManagerUtil.createRetryWithToleranceOperator(keyValue, errorMetricsGroup);
                        retryWithToleranceOperator.reporters(ReporterManagerUtil.sinkTaskReporters(id, keyValue, workerConfig, errorMetricsGroup));

                        WorkerSinkTask workerSinkTask = new WorkerSinkTask(workerConfig, id,
                                (SinkTask) task, savedLoader, keyValue, keyConverter, valueConverter, consumer, workerState, connectStatsManager, connectStatsService, transformChain,
                                retryWithToleranceOperator, ReporterManagerUtil.createWorkerErrorRecordReporter(keyValue, retryWithToleranceOperator, valueConverter), statusListener, this.connectMetrics);
                        Future future = taskExecutor.submit(workerSinkTask);
                        taskToFutureMap.put(workerSinkTask, future);
                        this.pendingTasks.put(workerSinkTask, System.currentTimeMillis());
                    }
                    Plugin.compareAndSwapLoaders(savedLoader);
                } catch (Exception e) {
                    log.error("start worker task exception. config {}" + JSON.toJSONString(keyValue), e);
                    Plugin.compareAndSwapLoaders(savedLoader);
                }
            }
        }
    }

    private void redressRunningStatus(WorkerTask workerTask) {
        TaskStatus taskStatus = stateManagementService.get(workerTask.id);
        if (taskStatus != null && taskStatus.getState() != RUNNING && taskStatus.getState() != PAUSED) {
            ConnectorStatus connectorStatus = stateManagementService.get(workerTask.id.connector());
            TaskStatus newTaskStatus;
            if (null != connectorStatus && connectorStatus.getState() == PAUSED) {
                newTaskStatus = new TaskStatus(workerTask.id, PAUSED, workerConfig.getWorkerId(), System.currentTimeMillis());
            } else {
                newTaskStatus = new TaskStatus(workerTask.id, RUNNING, workerConfig.getWorkerId(), System.currentTimeMillis());
            }
            log.warn("Task {}, Old task status is {}, new task status {}", workerTask.id, taskStatus, newTaskStatus);
            stateManagementService.put(newTaskStatus);
        }
    }

    private Map<String, List<ConnectKeyValue>> newTasks(Map<String, List<ConnectKeyValue>> taskConfigs) {
        Map<String, List<ConnectKeyValue>> newTasks = new HashMap<>();
        for (String connectorName : taskConfigs.keySet()) {
            for (ConnectKeyValue keyValue : taskConfigs.get(connectorName)) {
                boolean isNewTask = !isConfigInSet(keyValue, runningTasks) && !isConfigInSet(keyValue, pendingTasks.keySet()) && !isConfigInSet(keyValue, errorTasks);
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

    private void createDirectTask(ConnectorTaskId id, ConnectKeyValue keyValue) throws Exception {
        String sourceTaskClass = keyValue.getString(ConnectorConfig.SOURCE_TASK_CLASS);
        Task sourceTask = getTask(sourceTaskClass);

        String sinkTaskClass = keyValue.getString(ConnectorConfig.SINK_TASK_CLASS);
        Task sinkTask = getTask(sinkTaskClass);

        TransformChain<ConnectRecord> transformChain = new TransformChain<>(keyValue, plugin);
        // create retry operator
        RetryWithToleranceOperator retryWithToleranceOperator = ReporterManagerUtil.createRetryWithToleranceOperator(keyValue, new ErrorMetricsGroup(id, this.connectMetrics));
        retryWithToleranceOperator.reporters(ReporterManagerUtil.sourceTaskReporters(id, keyValue, new ErrorMetricsGroup(id, this.connectMetrics)));

        WorkerDirectTask workerDirectTask = new WorkerDirectTask(
                workerConfig,
                id,
                (SourceTask) sourceTask,
                null,
                (SinkTask) sinkTask,
                keyValue,
                positionManagementService,
                workerState,
                connectStatsManager,
                connectStatsService,
                transformChain,
                retryWithToleranceOperator,
                statusListener,
                connectMetrics);

        Future future = taskExecutor.submit(workerDirectTask);

        // schedule offset committer
        sourceTaskOffsetCommitter.ifPresent(committer -> committer.schedule(id, workerDirectTask));

        taskToFutureMap.put(workerDirectTask, future);
        this.pendingTasks.put(workerDirectTask, System.currentTimeMillis());
    }

    private Task getTask(String taskClass) {
        ClassLoader savedLoader = plugin.currentThreadLoader();
        Task task = null;
        try {
            // Get plugin loader
            ClassLoader taskLoader = plugin.delegatingLoader().pluginClassLoader(taskClass);
            // Compare and set current loader
            savedLoader = Plugin.compareAndSwapLoaders(taskLoader);
            // load class
            Class taskClazz = Utils.getContextCurrentClassLoader().loadClass(taskClass).asSubclass(Task.class);
            // new task
            task = plugin.newTask(taskClazz);
        } catch (Exception ex) {
            throw new ConnectException("Create direct task failure", ex);
        } finally {
            Plugin.compareAndSwapLoaders(savedLoader);
        }
        return task;
    }

    public enum TaskType {
        SOURCE,
        SINK,
        DIRECT
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
}
