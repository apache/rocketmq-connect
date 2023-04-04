/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rocketmq.connect.runtime.utils;

import org.apache.rocketmq.connect.runtime.common.ConnectKeyValue;
import org.apache.rocketmq.connect.runtime.connectorwrapper.WorkerTaskState;

import java.io.Serializable;
import java.util.Map;
import java.util.Objects;


/**
 * current task state
 */
public class CurrentTaskState implements Serializable {

    private String connectorName;
    private Map<String, String> config;
    private WorkerTaskState taskState;


    public CurrentTaskState(String connector, ConnectKeyValue connectKeyValue, WorkerTaskState taskState) {
        this.connectorName = connector;
        this.config = connectKeyValue.getProperties();
        this.taskState = taskState;
    }

    public String getConnectorName() {
        return connectorName;
    }

    public void setConnectorName(String connectorName) {
        this.connectorName = connectorName;
    }

    public Map<String, String> getConfig() {
        return config;
    }

    public void setConfig(Map<String, String> config) {
        this.config = config;
    }

    public WorkerTaskState getTaskState() {
        return taskState;
    }

    public void setTaskState(WorkerTaskState taskState) {
        this.taskState = taskState;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof CurrentTaskState)) return false;
        CurrentTaskState that = (CurrentTaskState) o;
        return Objects.equals(connectorName, that.connectorName) && Objects.equals(config, that.config) && taskState == that.taskState;
    }

    @Override
    public int hashCode() {
        return Objects.hash(connectorName, config, taskState);
    }
}
