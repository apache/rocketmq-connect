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

package org.apache.rocketmq.connect.runtime.connectorwrapper.status;


import org.apache.rocketmq.connect.runtime.service.StateManagementService;
import org.apache.rocketmq.connect.runtime.utils.ConnectorTaskId;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;

/**
 * Wrap all implementations of connector and task
 */
public class WrapperStatusListener implements TaskStatus.Listener, ConnectorStatus.Listener {

    private StateManagementService managementService;
    private String workerId;

    public WrapperStatusListener(StateManagementService managementService, String workerId) {
        this.managementService = managementService;
        this.workerId = workerId;
    }

    /**
     * Invoked after connector has successfully been shutdown.
     *
     * @param connector The connector name
     */
    @Override
    public void onShutdown(String connector) {
        managementService.putSafe(new ConnectorStatus(connector, ConnectorStatus.State.UNASSIGNED,
                workerId, generation()));
    }

    /**
     * error
     *
     * @param connector
     * @param cause
     */
    @Override
    public void onFailure(String connector, Throwable cause) {
        managementService.putSafe(new ConnectorStatus(connector, ConnectorStatus.State.FAILED,
                workerId, generation(), trace(cause)));
    }

    private String trace(Throwable t) {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        try {
            t.printStackTrace(new PrintStream(output, false, StandardCharsets.UTF_8.name()));
            return output.toString(StandardCharsets.UTF_8.name());
        } catch (UnsupportedEncodingException e) {
            return null;
        }
    }

    /**
     * Invoked when the connector is paused through the REST API
     *
     * @param connector The connector name
     */
    @Override
    public void onPause(String connector) {
        managementService.put(new ConnectorStatus(connector, ConnectorStatus.State.PAUSED,
                workerId, generation()));
    }

    /**
     * Invoked after the connector has been resumed.
     *
     * @param connector The connector name
     */
    @Override
    public void onResume(String connector) {
        managementService.put(new ConnectorStatus(connector, TaskStatus.State.RUNNING,
                workerId, generation()));
    }

    /**
     * Invoked after successful startup of the connector.
     *
     * @param connector The connector name
     */
    @Override
    public void onStartup(String connector) {
        managementService.put(new ConnectorStatus(connector, ConnectorStatus.State.RUNNING,
                workerId, generation()));
    }

    /**
     * Invoked when the connector is deleted through the REST API.
     *
     * @param connector The connector name
     */
    @Override
    public void onDeletion(String connector) {
        for (TaskStatus status : managementService.getAll(connector)) {
            onDeletion(status.getId());
        }
        managementService.put(new ConnectorStatus(connector, ConnectorStatus.State.DESTROYED, workerId, generation()));

    }

    /**
     * Invoked after successful startup of the task.
     *
     * @param id The id of the task
     */
    @Override
    public void onStartup(ConnectorTaskId id) {
        managementService.put(new TaskStatus(id, TaskStatus.State.RUNNING, workerId, generation()));
    }

    /**
     * Invoked after the task has been paused.
     *
     * @param id The id of the task
     */
    @Override
    public void onPause(ConnectorTaskId id) {
        managementService.put(new TaskStatus(id, TaskStatus.State.PAUSED, workerId, generation()));
    }

    /**
     * Invoked after the task has been resumed.
     *
     * @param id The id of the task
     */
    @Override
    public void onResume(ConnectorTaskId id) {
        managementService.put(new TaskStatus(id, TaskStatus.State.RUNNING, workerId, generation()));
    }

    /**
     * Invoked if the task raises an error. No shutdown event will follow.
     *
     * @param id    The id of the task
     * @param cause The error raised by the task.
     */
    @Override
    public void onFailure(ConnectorTaskId id, Throwable cause) {
        managementService.putSafe(new TaskStatus(id, TaskStatus.State.FAILED, workerId, generation(), trace(cause)));
    }

    /**
     * Invoked after successful shutdown of the task.
     *
     * @param id The id of the task
     */
    @Override
    public void onShutdown(ConnectorTaskId id) {
        managementService.putSafe(new TaskStatus(id, TaskStatus.State.UNASSIGNED, workerId, generation()));
    }


    /**
     * Invoked after the task has been deleted. Can be called if the
     * connector tasks have been reduced, or if the connector itself has
     * been deleted.
     *
     * @param id The id of the task
     */
    @Override
    public void onDeletion(ConnectorTaskId id) {
        managementService.put(new TaskStatus(id, TaskStatus.State.DESTROYED, workerId, generation()));
    }

    private Long generation() {
        return System.currentTimeMillis();
    }
}
