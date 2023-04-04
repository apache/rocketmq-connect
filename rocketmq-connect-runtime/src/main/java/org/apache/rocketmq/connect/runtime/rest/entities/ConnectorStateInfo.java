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
package org.apache.rocketmq.connect.runtime.rest.entities;

import java.util.List;
import java.util.Objects;

/**
 * connector state info
 */
public class ConnectorStateInfo {

    private String name;
    private ConnectorState connector;
    private List<TaskState> tasks;
    private ConnectorType type;

    public ConnectorStateInfo() {
    }

    public ConnectorStateInfo(String name,
                              ConnectorState connector,
                              List<TaskState> tasks,
                              ConnectorType type) {
        this.name = name;
        this.connector = connector;
        this.tasks = tasks;
        this.type = type;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public ConnectorState getConnector() {
        return connector;
    }

    public void setConnector(ConnectorState connector) {
        this.connector = connector;
    }

    public List<TaskState> getTasks() {
        return tasks;
    }

    public void setTasks(List<TaskState> tasks) {
        this.tasks = tasks;
    }

    public ConnectorType getType() {
        return type;
    }

    public void setType(ConnectorType type) {
        this.type = type;
    }


    public abstract static class AbstractState {
        private String state;
        private String trace;
        private String workerId;

        public AbstractState() {
        }

        public AbstractState(String state, String workerId, String trace) {
            this.state = state;
            this.workerId = workerId;
            this.trace = trace;
        }

        public String getState() {
            return state;
        }

        public void setState(String state) {
            this.state = state;
        }

        public String getTrace() {
            return trace;
        }

        public void setTrace(String trace) {
            this.trace = trace;
        }

        public String getWorkerId() {
            return workerId;
        }

        public void setWorkerId(String workerId) {
            this.workerId = workerId;
        }
    }

    public static class ConnectorState extends AbstractState {
        public ConnectorState(String state,
                              String worker,
                              String msg) {
            super(state, worker, msg);
        }
    }

    public static class TaskState extends AbstractState implements Comparable<TaskState> {
        private int id;

        public TaskState() {
        }

        public TaskState(int id, String state, String worker, String msg) {
            super(state, worker, msg);
            this.id = id;
        }

        public int getId() {
            return id;
        }

        public void setId(int id) {
            this.id = id;
        }

        @Override
        public int compareTo(TaskState that) {
            return Integer.compare(this.id, that.id);
        }

        @Override
        public boolean equals(Object o) {
            if (o == this)
                return true;
            if (!(o instanceof TaskState))
                return false;
            TaskState other = (TaskState) o;
            return compareTo(other) == 0;
        }

        @Override
        public int hashCode() {
            return Objects.hash(id);
        }
    }

}
