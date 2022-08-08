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
import org.apache.rocketmq.connect.runtime.common.ConnectKeyValue;
import org.apache.rocketmq.connect.runtime.connectorwrapper.status.ConnectorStatus;
import org.apache.rocketmq.connect.runtime.controller.isolation.Plugin;
import org.apache.rocketmq.connect.runtime.utils.Callback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * A wrapper of {@link Connector} for runtime.
 */
public class WorkerConnector implements Runnable  {
    private static final Logger log = LoggerFactory.getLogger(WorkerConnector.class);
    private static final String THREAD_NAME_PREFIX = "connector-thread-";

    private enum State {
        INIT,    // initial state before startup
        STOPPED, // the connector has been stopped/paused.
        STARTED, // the connector has been started/resumed.
        FAILED,  // the connector has failed (no further transitions are possible after this state)
    }


    /**
     * Name of the worker connector.
     */
    private String connectorName;

    /**
     * Instance of a connector implements.
     */
    private Connector connector;

    /**
     * The configs for the current connector.
     */
    private ConnectKeyValue keyValue;

    private final ConnectorContext context;

    private final ConnectorStatus.Listener statusListener;

    private final ClassLoader classloader;

    private final AtomicReference<TargetState> pendingTargetStateChange;
    private final AtomicReference<Callback<TargetState>> pendingStateChangeCallback;

    private volatile boolean stopping;  // indicates whether the Worker has asked the connector to stop
    private State state;

    private final CountDownLatch shutdownLatch;

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
        this.pendingTargetStateChange = new AtomicReference<>();
        this.pendingStateChangeCallback = new AtomicReference<>();
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

    /**
     * When an object implementing interface <code>Runnable</code> is used
     * to create a thread, starting the thread causes the object's
     * <code>run</code> method to be called in that separately executing
     * thread.
     * <p>
     * The general contract of the method <code>run</code> is that it may
     * take any action whatsoever.
     * @see Thread#run()
     */
    @Override
    public void run() {
        try  {
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
                newTargetState = pendingTargetStateChange.getAndSet(null);
                stateChangeCallback = pendingStateChangeCallback.getAndSet(null);
            }
            if (newTargetState != null && !stopping) {
                doTransitionTo(newTargetState, stateChangeCallback);
            }
            synchronized (this) {
                if (pendingTargetStateChange.get() != null || stopping) {
                    // An update occurred before we entered the synchronized block; no big deal,
                    // just start the loop again until we've handled everything
                } else {
                    try {
                        wait();
                    } catch (InterruptedException e) {
                        // We'll pick up any potential state changes at the top of the loop
                    }
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

    public boolean isRunning() {
        return state == State.STARTED;
    }


    @SuppressWarnings("fallthrough")
    private void pause() {
        try {
            switch (state) {
                case STOPPED:
                    return;

                case STARTED:
                    connector.stop();
                    // fall through
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
            TargetState preEmptedState = pendingTargetStateChange.getAndSet(null);
            Callback<TargetState> stateChangeCallback = pendingStateChangeCallback.getAndSet(null);
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
            statusListener.onShutdown(connectorName);
            log.info("Completed shutdown for {}", this);
        } catch (Throwable t) {
            log.error("{} Error while shutting down connector", this, t);
            state = State.FAILED;
            statusListener.onFailure(connectorName, t);
        }
    }

    /**
     * Wait for this connector to finish shutting down.
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
            preEmptedStateChangeCallback = pendingStateChangeCallback.getAndSet(stateChangeCallback);
            preEmptedState = pendingTargetStateChange.getAndSet(targetState);
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
     * @return
     */
    public String getConnectorName() {
        return connectorName;
    }

    /**
     * connector config
     * @return
     */
    public ConnectKeyValue getKeyValue() {
        return keyValue;
    }

    /**
     * connector object
     * @return
     */
    public Connector getConnector() {
        return connector;
    }

    public synchronized void cancel() {
        // Proactively update the status of the connector to UNASSIGNED since this connector
        // instance is being abandoned and we won't update the status on its behalf any more
        // after this since a new instance may be started soon
        statusListener.onShutdown(connectorName);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("connectorName:" + connectorName)
            .append("\nConfigs:" + JSON.toJSONString(keyValue));
        return sb.toString();
    }
}
