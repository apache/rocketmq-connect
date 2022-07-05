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

import io.openmessaging.connector.api.data.ConnectRecord;
import org.apache.rocketmq.connect.runtime.common.ConnectKeyValue;
import org.apache.rocketmq.connect.runtime.errors.RetryWithToleranceOperator;
import org.apache.rocketmq.connect.runtime.utils.ConnectorTaskId;
import org.apache.rocketmq.connect.runtime.utils.CurrentTaskState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicReference;

/**
 * Should we use callable here ?
 */
public abstract class WorkerTask implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(WorkerTask.class);
    private static final String THREAD_NAME_PREFIX = "task-thread-";

    protected final ConnectorTaskId id;
    protected final ClassLoader loader;
    protected final ConnectKeyValue taskConfig;
    /**
     * Atomic state variable
     */
    protected AtomicReference<WorkerTaskState> state;
    /**
     * worker state
     */
    protected final AtomicReference<WorkerState> workerState;
    protected final RetryWithToleranceOperator retryWithToleranceOperator;
    protected final TransformChain<ConnectRecord> transformChain;


    public WorkerTask(ConnectorTaskId id, ClassLoader loader, ConnectKeyValue taskConfig, RetryWithToleranceOperator retryWithToleranceOperator, TransformChain<ConnectRecord> transformChain, AtomicReference<WorkerState> workerState) {
        this.id = id;
        this.loader = loader;
        this.taskConfig = taskConfig;
        this.state = new AtomicReference<>(WorkerTaskState.NEW);
        this.workerState = workerState;
        this.retryWithToleranceOperator = retryWithToleranceOperator;
        this.transformChain = transformChain;
        this.transformChain.retryWithToleranceOperator(this.retryWithToleranceOperator);
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

    private void doInitializeAndStart() {
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
        return !isRunning();
    }

    /**
     * close resources
     */
    public abstract void close();

    private void doClose() {
        try {
            state.compareAndSet(WorkerTaskState.RUNNING, WorkerTaskState.STOPPING);
            close();
            state.compareAndSet(WorkerTaskState.STOPPING, WorkerTaskState.STOPPED);
        } catch (Throwable t) {
            log.error("{} Task threw an uncaught and unrecoverable exception during shutdown", this, t);
            throw t;
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
            }
            doInitializeAndStart();
            // while poll
            doExecute();
        } catch (Throwable t) {
            log.error("{} Task threw an uncaught and unrecoverable exception. Task is being killed and will not recover until manually restarted", this, t);
            throw t;
        } finally {
            doClose();
        }
    }

    /**
     * do execute data
     */
    @Override
    public void run() {
        String savedName = Thread.currentThread().getName();
        try {
            Thread.currentThread().setName(THREAD_NAME_PREFIX + id);
            doRun();
        } catch (Throwable t) {
            onFailure(t);
            throw (Error) t;
        } finally {
            Thread.currentThread().setName(savedName);
        }
    }

    public void onFailure(Throwable t) {
        synchronized (this) {
            state.set(WorkerTaskState.ERROR);
        }
    }

    public void timeout() {
        onFailure(null);
    }

}
