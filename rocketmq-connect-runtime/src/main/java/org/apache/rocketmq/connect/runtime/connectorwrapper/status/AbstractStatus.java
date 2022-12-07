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

import java.util.Objects;

/**
 * abstract state
 *
 * @param <T>
 */
public abstract class AbstractStatus<T> {
    private T id;
    private State state;
    private String workerId;
    private Long generation;
    private String trace;
    public AbstractStatus() {
    }

    public AbstractStatus(T id,
                          State state,
                          String workerId,
                          Long generation,
                          String trace
    ) {
        this.id = id;
        this.state = state;
        this.workerId = workerId;
        this.generation = generation;
        this.trace = trace;
    }

    public T getId() {
        return id;
    }

    public void setId(T id) {
        this.id = id;
    }

    public State getState() {
        return state;
    }

    public void setState(State state) {
        this.state = state;
    }

    public String getWorkerId() {
        return workerId;
    }

    public void setWorkerId(String workerId) {
        this.workerId = workerId;
    }

    public Long getGeneration() {
        return generation;
    }

    public void setGeneration(Long generation) {
        this.generation = generation;
    }

    public String getTrace() {
        return trace;
    }

    public void setTrace(String trace) {
        this.trace = trace;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof AbstractStatus)) return false;
        AbstractStatus<?> that = (AbstractStatus<?>) o;
        return Objects.equals(id, that.id) && state == that.state && Objects.equals(workerId, that.workerId) && Objects.equals(generation, that.generation) && Objects.equals(trace, that.trace);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, state, workerId, generation, trace);
    }

    @Override
    public String toString() {
        return "AbstractStatus{" +
                "id=" + id +
                ", state=" + state +
                ", workerId='" + workerId + '\'' +
                ", generation=" + generation +
                ", trace='" + trace + '\'' +
                '}';
    }

    public enum State {
        UNASSIGNED,
        RUNNING,
        PAUSED,
        FAILED,
        DESTROYED,
        RESTARTING,
    }
}
