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

import com.codahale.metrics.MetricRegistry;
import io.openmessaging.connector.api.data.ConnectRecord;
import org.apache.rocketmq.connect.metrics.stats.Avg;
import org.apache.rocketmq.connect.metrics.stats.CumulativeCount;
import org.apache.rocketmq.connect.metrics.stats.Max;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.rocketmq.connect.runtime.common.ConnectKeyValue;
import org.apache.rocketmq.connect.runtime.config.WorkerConfig;
import org.apache.rocketmq.connect.runtime.connectorwrapper.status.TaskStatus;
import org.apache.rocketmq.connect.runtime.controller.isolation.Plugin;
import org.apache.rocketmq.connect.runtime.errors.RetryWithToleranceOperator;
import org.apache.rocketmq.connect.runtime.metrics.ConnectMetrics;
import org.apache.rocketmq.connect.runtime.metrics.ConnectMetricsTemplates;
import org.apache.rocketmq.connect.runtime.metrics.MetricGroup;
import org.apache.rocketmq.connect.runtime.metrics.Sensor;
import org.apache.rocketmq.connect.runtime.utils.ConnectorTaskId;
import org.apache.rocketmq.connect.runtime.utils.CurrentTaskState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Should we use callable here ?
 */
public abstract class WorkerTask implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(WorkerTask.class);
    private static final String THREAD_NAME_PREFIX = "task-thread-";

    protected final WorkerConfig workerConfig;

    protected final ConnectorTaskId id;
    protected final ClassLoader loader;
    protected final ConnectKeyValue taskConfig;
    /**
     * worker state
     */
    protected final AtomicReference<WorkerState> workerState;
    protected final RetryWithToleranceOperator retryWithToleranceOperator;
    protected final TransformChain<ConnectRecord> transformChain;
    // send status
    private final TaskStatus.Listener statusListener;
    private final CountDownLatch shutdownLatch = new CountDownLatch(1);
    private final TaskMetricsGroup taskMetricsGroup;
    /**
     * Atomic state variable
     */
    protected AtomicReference<WorkerTaskState> state;
    private volatile TargetState targetState;

    public WorkerTask(WorkerConfig workerConfig, ConnectorTaskId id, ClassLoader loader, ConnectKeyValue taskConfig, RetryWithToleranceOperator retryWithToleranceOperator, TransformChain<ConnectRecord> transformChain, AtomicReference<WorkerState> workerState, TaskStatus.Listener taskListener, ConnectMetrics connectMetrics) {
        this.workerConfig = workerConfig;
        this.id = id;
        this.loader = loader;
        this.taskConfig = taskConfig;
        this.state = new AtomicReference<>(WorkerTaskState.NEW);
        this.workerState = workerState;
        this.retryWithToleranceOperator = retryWithToleranceOperator;
        this.transformChain = transformChain;
        this.transformChain.retryWithToleranceOperator(this.retryWithToleranceOperator);
        this.targetState = TargetState.STARTED;
        this.statusListener = taskListener;
        this.taskMetricsGroup = new TaskMetricsGroup(id, connectMetrics);
    }

    public ConnectorTaskId id() {
        return id;
    }

    public ClassLoader loader() {
        return loader;
    }

    /**
     * Initialize the task for execution.
     *
     * @param taskConfig initial configuration
     */
    protected void initialize(ConnectKeyValue taskConfig) {
        // NO-op
    }

    /**
     * initinalize and start
     */
    protected abstract void initializeAndStart();

    public void doInitializeAndStart() {
        state.compareAndSet(WorkerTaskState.NEW, WorkerTaskState.PENDING);
        initializeAndStart();
        state.compareAndSet(WorkerTaskState.PENDING, WorkerTaskState.RUNNING);
    }

    /**
     * execute poll and send record
     */
    protected abstract void execute();

    private void doExecute() {
        execute();
    }

    public void removeMetrics() {
        taskMetricsGroup.close();
    }

    /**
     * get state
     *
     * @return
     */
    public WorkerTaskState getState() {
        return this.state.get();
    }

    protected boolean isRunning() {
        return WorkerState.STARTED == workerState.get() && WorkerTaskState.RUNNING == state.get();
    }

    protected boolean isStopping() {
        return WorkerTaskState.ERROR == state.get()
                || WorkerTaskState.STOPPING == state.get()
                || WorkerTaskState.STOPPED == state.get()
                || WorkerTaskState.TERMINATED == state.get();
    }

    /**
     * close resources
     */
    protected abstract void close();

    public void doClose() {
        try {
            state.compareAndSet(WorkerTaskState.RUNNING, WorkerTaskState.STOPPING);
            close();
            state.compareAndSet(WorkerTaskState.STOPPING, WorkerTaskState.STOPPED);
            // UNASSIGNED state
            statusListener.onShutdown(id);
        } catch (Throwable t) {
            log.error("{} Task threw an uncaught and unrecoverable exception during shutdown", this, t);
            throw t;
        }
    }

    /**
     * Wait for this task to finish stopping.
     *
     * @param timeoutMs time in milliseconds to await stop
     * @return true if successful, false if the timeout was reached
     */
    public boolean awaitStop(long timeoutMs) {
        try {
            return shutdownLatch.await(timeoutMs, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            return false;
        }
    }

    /**
     * clean up
     */
    public void cleanup() {
        log.info("Cleaning a task, current state {}, destination state {}", state.get().name(), WorkerTaskState.TERMINATED.name());
        if (state.compareAndSet(WorkerTaskState.STOPPED, WorkerTaskState.TERMINATED) ||
                state.compareAndSet(WorkerTaskState.ERROR, WorkerTaskState.TERMINATED)) {
            log.info("Cleaning a task success");
        } else {
            log.error("[BUG] cleaning a task but it's not in STOPPED or ERROR state");
        }
    }


    public ConnectKeyValue currentTaskConfig() {
        return taskConfig;
    }

    /**
     * current task state
     *
     * @return
     */
    public CurrentTaskState currentTaskState() {
        return new CurrentTaskState(id().connector(), taskConfig, state.get());
    }


    private void doRun() throws InterruptedException {
        try {
            synchronized (this) {
                if (isStopping()) {
                    return;
                }

                if (targetState == TargetState.PAUSED) {
                    onPause();
                    if (!awaitUnpause()) {
                        return;
                    }
                }
            }

            doInitializeAndStart();
            statusListener.onStartup(id);
            // while poll
            doExecute();
        } catch (Throwable t) {
            if (isRunning()) {
                log.error("{} Task threw an uncaught and unrecoverable exception. Task is being killed and will not recover until manually restarted", this, t);
                throw t;
            }
        } finally {
            doClose();
        }
    }

    /**
     * do execute data
     */
    @Override
    public void run() {
        ClassLoader savedLoader = Plugin.compareAndSwapLoaders(loader);
        String savedName = Thread.currentThread().getName();
        try {
            Thread.currentThread().setName(THREAD_NAME_PREFIX + id);
            doRun();
        } catch (InterruptedException e) {
            // set interrupted flag to caller
            Thread.currentThread().interrupt();
        } catch (Throwable t) {
            onFailure(t);
            throw t;
        } finally {
            Thread.currentThread().setName(savedName);
            Plugin.compareAndSwapLoaders(savedLoader);
            shutdownLatch.countDown();
        }
    }


    /**
     * record commit success
     *
     * @param duration
     */
    protected void recordCommitSuccess(long duration) {
        taskMetricsGroup.recordCommit(duration, true);
    }

    /**
     * record commit failure
     *
     * @param duration
     */
    protected void recordCommitFailure(long duration) {
        taskMetricsGroup.recordCommit(duration, false);
    }

    /**
     * batch record
     *
     * @param size
     */
    protected void recordMultiple(int size) {
        taskMetricsGroup.recordMultiple(size);
    }

    /**
     * should pause
     *
     * @return
     */
    public boolean shouldPause() {
        return this.targetState == TargetState.PAUSED;
    }

    /**
     * Await task resumption.
     *
     * @return true if the task's target state is not paused, false if the task is shutdown before resumption
     * @throws InterruptedException
     */
    protected boolean awaitUnpause() throws InterruptedException {
        synchronized (this) {
            while (targetState == TargetState.PAUSED) {
                if (isStopping()) {
                    return false;
                }
                this.wait();
            }
            return true;
        }
    }


    /**
     * change task target state
     *
     * @param state
     */
    public void transitionTo(TargetState state) {
        synchronized (this) {
            // ignore the state change if we are stopping
            if (isStopping()) {
                return;
            }
            // not equal set
            if (this.targetState != state) {
                this.targetState = state;
                // notify thread continue run
                this.notifyAll();
            }
        }
    }


    protected synchronized void onPause() {
        statusListener.onPause(id);
    }

    protected synchronized void onResume() {
        statusListener.onResume(id);
    }


    public void onFailure(Throwable t) {
        synchronized (this) {
            state.set(WorkerTaskState.ERROR);
            // on failure
            statusListener.onFailure(id, t);
        }
    }

    /**
     * timeout
     */
    public void timeout() {
        log.error("Worker task stop is timeout !!!");
        onFailure(new Throwable("Worker task stop is timeout"));
    }


    static class TaskMetricsGroup {
        private final MetricGroup metricGroup;
        private final Sensor commitTime;
        private final Sensor batchSize;

        private final Sensor taskCommitFailures;

        private final Sensor taskCommitSuccess;

        public TaskMetricsGroup(ConnectorTaskId id, ConnectMetrics connectMetrics) {
            ConnectMetricsTemplates templates = connectMetrics.templates();
            metricGroup = connectMetrics.group(
                    templates.connectorTagName(),
                    id.connector(),
                    templates.taskTagName(),
                    Integer.toString(id.task())
            );

            MetricRegistry registry = connectMetrics.registry();

            commitTime = metricGroup.sensor();
            commitTime.addStat(new Max(registry, metricGroup.name(templates.taskCommitTimeMax)));
            commitTime.addStat(new Avg(registry, metricGroup.name(templates.taskCommitTimeAvg)));

            batchSize = metricGroup.sensor();
            batchSize.addStat(new Max(registry, metricGroup.name(templates.taskBatchSizeMax)));
            batchSize.addStat(new Avg(registry, metricGroup.name(templates.taskBatchSizeAvg)));

            taskCommitFailures = metricGroup.sensor();
            taskCommitFailures.addStat(new CumulativeCount(registry, metricGroup.name(templates.taskCommitFailureCount)));

            taskCommitSuccess = metricGroup.sensor();
            taskCommitSuccess.addStat(new CumulativeCount(registry, metricGroup.name(templates.taskCommitSuccessCount)));

        }


        void close() {
            metricGroup.close();
        }

        void recordCommit(long duration, boolean success) {
            if (success) {
                commitTime.record(duration);
                taskCommitSuccess.record(1);
            } else {
                taskCommitFailures.record(1);
            }
        }

        void recordMultiple(int size) {
            batchSize.record(size);
        }
    }

}
