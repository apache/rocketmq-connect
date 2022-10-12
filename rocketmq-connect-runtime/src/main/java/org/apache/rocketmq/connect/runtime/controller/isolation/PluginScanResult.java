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
package org.apache.rocketmq.connect.runtime.controller.isolation;


import io.openmessaging.connector.api.component.Transform;
import io.openmessaging.connector.api.component.task.sink.SinkConnector;
import io.openmessaging.connector.api.component.task.sink.SinkTask;
import io.openmessaging.connector.api.component.task.source.SourceConnector;
import io.openmessaging.connector.api.component.task.source.SourceTask;
import io.openmessaging.connector.api.data.RecordConverter;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;


/**
 * plugin scan result
 */
public class PluginScanResult {
    private final Collection<PluginWrapper<SourceConnector>> sourceConnectors;
    private final Collection<PluginWrapper<SinkConnector>> sinkConnectors;
    private final Collection<PluginWrapper<SourceTask>> sourceTasks;
    private final Collection<PluginWrapper<SinkTask>> sinkTasks;
    private final Collection<PluginWrapper<RecordConverter>> converters;
    private final Collection<PluginWrapper<Transform<?>>> transformations;

    private final List<Collection> allPlugins;

    public PluginScanResult(
            Collection<PluginWrapper<SourceConnector>> sourceConnectors,
            Collection<PluginWrapper<SinkConnector>> sinkConnectors,
            Collection<PluginWrapper<SourceTask>> sourceTasks,
            Collection<PluginWrapper<SinkTask>> sinkTasks,
            Collection<PluginWrapper<RecordConverter>> converters,
            Collection<PluginWrapper<Transform<?>>> transformations
    ) {
        this.sinkConnectors = sinkConnectors;
        this.sourceConnectors = sourceConnectors;
        this.sourceTasks = sourceTasks;
        this.sinkTasks = sinkTasks;
        this.converters = converters;
        this.transformations = transformations;
        this.allPlugins =
                Arrays.asList(sinkConnectors, sourceConnectors, sourceTasks, sinkTasks, converters, transformations);
    }

    public Collection<PluginWrapper<SinkConnector>> sinkConnectors() {
        return sinkConnectors;
    }

    public Collection<PluginWrapper<SourceConnector>> sourceConnectors() {
        return sourceConnectors;
    }

    public Collection<PluginWrapper<SourceTask>> sourceTasks() {
        return sourceTasks;
    }

    public Collection<PluginWrapper<SinkTask>> sinkTasks() {
        return sinkTasks;
    }


    public Collection<PluginWrapper<RecordConverter>> converters() {
        return converters;
    }


    public Collection<PluginWrapper<Transform<?>>> transformations() {
        return transformations;
    }

    public boolean isEmpty() {
        boolean isEmpty = true;
        for (Collection plugins : allPlugins) {
            isEmpty = isEmpty && plugins.isEmpty();
        }
        return isEmpty;
    }
}
