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
 *
 */

package org.apache.rocketmq.connect.runtime.connectorwrapper;

import com.alibaba.fastjson.JSON;
import io.openmessaging.connector.api.component.connector.Connector;
import io.openmessaging.connector.api.component.connector.ConnectorContext;
import io.openmessaging.connector.api.component.task.sink.SinkConnector;
import io.openmessaging.connector.api.component.task.source.SourceConnector;
import io.openmessaging.connector.api.errors.ConnectException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.rocketmq.connect.runtime.common.ConnectKeyValue;
import org.apache.rocketmq.connect.runtime.connectorwrapper.status.ConnectorStatus;
import org.apache.rocketmq.connect.runtime.controller.isolation.Plugin;
import org.apache.rocketmq.connect.runtime.utils.Callback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A wrapper of {@link Connector} for runtime.
 */
public class WorkerConnector implements Runnable {
    private static final Logger log = LoggerFactory.getLogger(WorkerConnector.class);
    private static final String THREAD_NAME_PREFIX = "connector-thread-";
    private final ConnectorContext context;
    private final ConnectorStatus.Listener statusListener;
    private final ClassLoader classloader;
    private final AtomicReference<TargetState> targetStateChange;
    private final AtomicReference<Callback<TargetState>> targetStateChangeCallback;
    private final CountDownLatch shutdownLatch;
    /**
     * Name of the worker connector.
     */
    private final String connectorName;
    /**
     * Instance of a connector implements.
     */
    private final Connector connector;
    /**
     * The configs for the current connector.
     */
    private ConnectKeyValue keyValue;
    // stop status
    private volatile boolean stopping;
    private State state;

    public WorkerConnector(String connectorName,
        Connector connector,
        ConnectKeyValue keyValue,
        ConnectorContext context,
        ConnectorStatus.Listener statusListener,
        ClassLoader classloader) {
        this.connectorName = connectorName;
        this.connector = connector;
        this.keyValue = keyValue;
        this.context = context;
        this.statusListener = statusListener;
        this.classloader = classloader;
        this.targetStateChange = new AtomicReference<>();
        this.targetStateChangeCallback = new AtomicReference<>();
        this.state = State.INIT;
        this.shutdownLatch = new CountDownLatch(1);
        this.stopping = false;
    }

    public ClassLoader loader() {
        return classloader;
    }

    public boolean isSinkConnector() {
        return SinkConnector.class.isAssignableFrom(connector.getClass());
    }

    public boolean isSourceConnector() {
        return SourceConnector.class.isAssignableFrom(connector.getClass());
    }

    /**
     * initialize connector
     */
    public void initialize() {
        try {
            if (!isSourceConnector() && !isSinkConnector()) {
                throw new ConnectException("Connector implementations must be a subclass of either SourceConnector or SinkConnector");
            }
            log.debug("{} Initializing connector {}", this, connector);
            connector.validate(keyValue);
            connector.init(context);
        } catch (Throwable t) {
            log.error("{} Error initializing connector", this, t);
            onFailure(t);
        }
    }

    @Override
    public void run() {
        try {
            ClassLoader savedLoader = Plugin.compareAndSwapLoaders(classloader);
            String savedName = Thread.currentThread().getName();
            try {
                Thread.currentThread().setName(THREAD_NAME_PREFIX + connectorName);
                doRun();
            } finally {
                Thread.currentThread().setName(savedName);
                Plugin.compareAndSwapLoaders(savedLoader);
            }
        } finally {
            shutdownLatch.countDown();
        }
    }

    void doRun() {
        initialize();
        while (!stopping) {
            TargetState newTargetState;
            Callback<TargetState> stateChangeCallback;
            synchronized (this) {
                newTargetState = targetStateChange.getAndSet(null);
                stateChangeCallback = this.targetStateChangeCallback.getAndSet(null);
            }
            if (newTargetState != null && !stopping) {
                doTransitionTo(newTargetState, stateChangeCallback);
            }
            synchronized (this) {
                if (targetStateChange.get() != null || stopping) {
                    continue;
                }
                try {
                    wait();
                } catch (InterruptedException e) {
                }
            }
        }
        doShutdown();
    }

    private boolean doStart() throws Throwable {
        try {
            switch (state) {
                case STARTED:
                    return false;
                case INIT:
                case STOPPED:
                    connector.start(keyValue);
                    this.state = State.STARTED;
                    return true;
                default:
                    throw new IllegalArgumentException("Cannot start connector in state " + state);
            }
        } catch (Throwable t) {
            log.error("{} Error while starting connector", this, t);
            onFailure(t);
            throw t;
        }
    }

    private void onFailure(Throwable t) {
        statusListener.onFailure(connectorName, t);
        this.state = State.FAILED;
    }

    private void resume() throws Throwable {
        if (doStart()) {
            statusListener.onResume(connectorName);
        }
    }

    private void start() throws Throwable {
        if (doStart()) {
            statusListener.onStartup(connectorName);
        }
    }

    private void pause() {
        try {
            switch (state) {
                case STOPPED:
                    return;
                case STARTED:
                    connector.stop();
                case INIT:
                    statusListener.onPause(connectorName);
                    this.state = State.STOPPED;
                    break;
                default:
                    throw new IllegalArgumentException("Cannot pause connector in state " + state);
            }
        } catch (Throwable t) {
            log.error("{} Error while shutting down connector", this, t);
            statusListener.onFailure(connectorName, t);
            this.state = State.FAILED;
        }
    }

    /**
     * Stop this connector. This method does not block, it only triggers shutdown. Use
     * #{@link #awaitShutdown} to block until completion.
     */
    public synchronized void shutdown() {
        log.info("Scheduled shutdown for {}", this);
        stopping = true;
        notify();
    }

    void doShutdown() {
        try {
            TargetState preEmptedState = targetStateChange.getAndSet(null);
            Callback<TargetState> stateChangeCallback = this.targetStateChangeCallback.getAndSet(null);
            if (stateChangeCallback != null) {
                stateChangeCallback.onCompletion(
                    new ConnectException(
                        "Could not begin changing connector state to " + preEmptedState.name()
                            + " as the connector has been scheduled for shutdown"),
                    null);
            }
            if (state == State.STARTED) {
                connector.stop();
            }
            this.state = State.STOPPED;
            if (statusListener != null) {
                statusListener.onShutdown(connectorName);
            }
            log.info("Completed shutdown for {}", this);
        } catch (Throwable t) {
            log.error("{} Error while shutting down connector", this, t);
            state = State.FAILED;
            statusListener.onFailure(connectorName, t);
        }
    }

    /**
     * Wait for this connector to finish shutting down.
     *
     * @param timeoutMs time in milliseconds to await shutdown
     * @return true if successful, false if the timeout was reached
     */
    public boolean awaitShutdown(long timeoutMs) {
        try {
            return shutdownLatch.await(timeoutMs, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            return false;
        }
    }

    public void transitionTo(TargetState targetState, Callback<TargetState> stateChangeCallback) {
        Callback<TargetState> preEmptedStateChangeCallback;
        TargetState preEmptedState;
        synchronized (this) {
            preEmptedStateChangeCallback = this.targetStateChangeCallback.getAndSet(stateChangeCallback);
            preEmptedState = targetStateChange.getAndSet(targetState);
            notify();
        }
        if (preEmptedStateChangeCallback != null) {
            preEmptedStateChangeCallback.onCompletion(
                new ConnectException(
                    "Could not begin changing connector state to " + preEmptedState.name()
                        + " before another request to change state was made;"
                        + " the new request (which is to change the state to " + targetState.name()
                        + ") has pre-empted this one"),
                null
            );
        }
    }

    void doTransitionTo(TargetState targetState, Callback<TargetState> stateChangeCallback) {
        if (state == State.FAILED) {
            stateChangeCallback.onCompletion(
                new ConnectException(this + " Cannot transition connector to " + targetState + " since it has failed"),
                null);
            return;
        }

        try {
            doTransitionTo(targetState);
            stateChangeCallback.onCompletion(null, targetState);
        } catch (Throwable t) {
            stateChangeCallback.onCompletion(
                new ConnectException(
                    "Failed to transition connector " + connectorName + " to state " + targetState,
                    t),
                null);
        }
    }

    private void doTransitionTo(TargetState targetState) throws Throwable {
        log.debug("{} Transition connector to {}", this, targetState);
        if (targetState == TargetState.PAUSED) {
            pause();
        } else if (targetState == TargetState.STARTED) {
            if (state == State.INIT) {
                start();
            } else {
                resume();
            }
        } else {
            throw new IllegalArgumentException("Unhandled target state " + targetState);
        }
    }

    /**
     * reconfigure
     *
     * @param keyValue
     */
    public void reconfigure(ConnectKeyValue keyValue) {
        try {
            this.keyValue = keyValue;
            initialize();
            connector.stop();
            connector.start(keyValue);
        } catch (Throwable throwable) {
            throw new ConnectException(throwable);
        }
    }

    /**
     * connector name
     *
     * @return
     */
    public String getConnectorName() {
        return connectorName;
    }

    /**
     * connector config
     *
     * @return
     */
    public ConnectKeyValue getKeyValue() {
        return keyValue;
    }

    /**
     * connector object
     *
     * @return
     */
    public Connector getConnector() {
        return connector;
    }

    public synchronized void cancel() {
        statusListener.onShutdown(connectorName);
    }

    @Override
    public String toString() {
        String sb = "connectorName:" + connectorName +
            "\nConfigs:" + JSON.toJSONString(keyValue);
        return sb;
    }

    private enum State {
        INIT,
        STOPPED,
        STARTED,
        FAILED,
    }
}
