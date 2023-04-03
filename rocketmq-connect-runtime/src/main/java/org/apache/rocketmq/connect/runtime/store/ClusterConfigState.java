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
package org.apache.rocketmq.connect.runtime.store;

import org.apache.rocketmq.connect.runtime.connectorwrapper.TargetState;
import org.apache.rocketmq.connect.runtime.utils.ConnectorTaskId;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;


/**
 * cluster config state
 */
public class ClusterConfigState {
    public static final ClusterConfigState EMPTY = new ClusterConfigState(
            Collections.emptyMap(),
            Collections.emptyMap(),
            Collections.emptyMap(),
            Collections.emptyMap()
    );

    Map<String, Integer> connectorTaskCounts;
    Map<String, Map<String, String>> connectorConfigs;
    Map<String, TargetState> connectorTargetStates;
    Map<ConnectorTaskId, Map<String, String>> taskConfigs;

    public ClusterConfigState(Map<String, Integer> connectorTaskCounts,
                              Map<String, Map<String, String>> connectorConfigs,
                              Map<String, TargetState> connectorTargetStates,
                              Map<ConnectorTaskId, Map<String, String>> taskConfigs) {
        this.connectorTaskCounts = connectorTaskCounts;
        this.connectorConfigs = connectorConfigs;
        this.connectorTargetStates = connectorTargetStates;
        this.taskConfigs = taskConfigs;
    }

    /**
     * Check whether this snapshot contains configuration for a connector.
     *
     * @param connector name of the connector
     * @return true if this state contains configuration for the connector, false otherwise
     */
    public boolean contains(String connector) {
        return connectorConfigs.containsKey(connector);
    }

    /**
     * Get a list of the connectors in this configuration
     */
    public Set<String> connectors() {
        return connectorConfigs.keySet();
    }


    public Map<String, String> connectorConfig(String connector) {
        return connectorConfigs.get(connector);
    }

    public Map<String, String> rawConnectorConfig(String connector) {
        return connectorConfigs.get(connector);
    }

    /**
     * Get the target state of the connector
     *
     * @param connector
     * @return
     */
    public TargetState targetState(String connector) {
        return connectorTargetStates.get(connector);
    }

    /**
     * task config
     *
     * @param task
     * @return
     */
    public Map<String, String> taskConfig(ConnectorTaskId task) {
        return taskConfigs.get(task);
    }

    public Map<String, String> rawTaskConfig(ConnectorTaskId task) {
        return taskConfigs.get(task);
    }


    /**
     * get all task configs
     *
     * @param connector
     * @return
     */
    public List<Map<String, String>> allTaskConfigs(String connector) {
        Map<Integer, Map<String, String>> taskConfigs = new TreeMap<>();
        for (Map.Entry<ConnectorTaskId, Map<String, String>> taskConfigEntry : this.taskConfigs.entrySet()) {
            if (taskConfigEntry.getKey().connector().equals(connector)) {
                Map<String, String> configs = taskConfigEntry.getValue();
                taskConfigs.put(taskConfigEntry.getKey().task(), configs);
            }
        }
        return Collections.unmodifiableList(new ArrayList<>(taskConfigs.values()));
    }

    /**
     * Get the number of tasks assigned for the given connector.
     *
     * @param connectorName name of the connector to look up tasks for
     * @return the number of tasks
     */
    public int taskCount(String connectorName) {
        Integer count = connectorTaskCounts.get(connectorName);
        return count == null ? 0 : count;
    }


    /**
     * Get the current set of task IDs for the specified connector.
     *
     * @param connectorName the name of the connector to look up task configs for
     * @return the current set of connector task IDs
     */
    public List<ConnectorTaskId> tasks(String connectorName) {
        Integer numTasks = connectorTaskCounts.get(connectorName);
        if (numTasks == null) {
            return Collections.emptyList();
        }

        List<ConnectorTaskId> taskIds = new ArrayList<>(numTasks);
        for (int taskIndex = 0; taskIndex < numTasks; taskIndex++) {
            ConnectorTaskId taskId = new ConnectorTaskId(connectorName, taskIndex);
            taskIds.add(taskId);
        }
        return Collections.unmodifiableList(taskIds);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ClusterConfigState)) return false;
        ClusterConfigState that = (ClusterConfigState) o;
        return Objects.equals(connectorTaskCounts, that.connectorTaskCounts) && Objects.equals(connectorConfigs, that.connectorConfigs) && Objects.equals(connectorTargetStates, that.connectorTargetStates) && Objects.equals(taskConfigs, that.taskConfigs);
    }

    @Override
    public int hashCode() {
        return Objects.hash(connectorTaskCounts, connectorConfigs, connectorTargetStates, taskConfigs);
    }

    @Override
    public String toString() {
        return "ClusterConfigState{" +
                "connectorTaskCounts=" + connectorTaskCounts +
                ", connectorConfigs=" + connectorConfigs +
                ", connectorTargetStates=" + connectorTargetStates +
                ", taskConfigs=" + taskConfigs +
                '}';
    }
}
