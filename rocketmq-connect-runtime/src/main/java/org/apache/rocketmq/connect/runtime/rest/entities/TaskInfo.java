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

import org.apache.rocketmq.connect.runtime.utils.ConnectorTaskId;

import java.util.Map;
import java.util.Objects;

/**
 * task info
 */
public class TaskInfo {
    private ConnectorTaskId id;
    private Map<String, String> config;

    public TaskInfo(ConnectorTaskId id, Map<String, String> config) {
        this.id = id;
        this.config = config;
    }

    public ConnectorTaskId getId() {
        return id;
    }

    public void setId(ConnectorTaskId id) {
        this.id = id;
    }

    public Map<String, String> getConfig() {
        return config;
    }

    public void setConfig(Map<String, String> config) {
        this.config = config;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof TaskInfo)) return false;
        TaskInfo taskInfo = (TaskInfo) o;
        return Objects.equals(id, taskInfo.id) && Objects.equals(config, taskInfo.config);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, config);
    }

    @Override
    public String toString() {
        return "TaskInfo{" +
                "id=" + id +
                ", config=" + config +
                '}';
    }
}
