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

import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * connector info
 */
public class ConnectorInfo {

    private String name;
    private Map<String, String> config;
    private List<ConnectorTaskId> tasks;
    private ConnectorType type;

    public ConnectorInfo() {
    }

    public ConnectorInfo(String name, Map<String, String> config, List<ConnectorTaskId> tasks, ConnectorType type) {
        this.name = name;
        this.config = config;
        this.tasks = tasks;
        this.type = type;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Map<String, String> getConfig() {
        return config;
    }

    public void setConfig(Map<String, String> config) {
        this.config = config;
    }

    public List<ConnectorTaskId> getTasks() {
        return tasks;
    }

    public void setTasks(List<ConnectorTaskId> tasks) {
        this.tasks = tasks;
    }

    public ConnectorType getType() {
        return type;
    }

    public void setType(ConnectorType type) {
        this.type = type;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ConnectorInfo that = (ConnectorInfo) o;
        return Objects.equals(name, that.name) &&
                Objects.equals(config, that.config) &&
                Objects.equals(tasks, that.tasks) &&
                Objects.equals(type, that.type);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, config, tasks, type);
    }

}
