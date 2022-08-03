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
package org.apache.rocketmq.connect.runtime.rest;

import com.alibaba.fastjson.JSON;
import io.javalin.Context;
import io.openmessaging.connector.api.component.task.sink.SinkConnector;
import io.openmessaging.connector.api.component.task.source.SourceConnector;
import org.apache.rocketmq.connect.runtime.controller.AbstractConnectController;
import org.apache.rocketmq.connect.runtime.controller.isolation.PluginType;
import org.apache.rocketmq.connect.runtime.controller.isolation.PluginWrapper;
import org.apache.rocketmq.connect.runtime.rest.entities.ConfigKeyInfo;
import org.apache.rocketmq.connect.runtime.rest.entities.PluginInfo;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class ConnectorPluginsResource {

    private final AbstractConnectController connectController;
    private final List<PluginInfo> connectorPlugins;

    static final List<Class<? extends SinkConnector>> SINK_CONNECTOR_EXCLUDES = Arrays.asList();
    static final List<Class<? extends SourceConnector>> SOURCE_CONNECTOR_EXCLUDES = Arrays.asList();


    public ConnectorPluginsResource(AbstractConnectController connectController) {
        this.connectController = connectController;
        this.connectorPlugins = new ArrayList<>();

        // TODO: improve once plugins are allowed to be added/removed during runtime.
        addConnectorPlugins(connectController.plugin().sinkConnectors(), SINK_CONNECTOR_EXCLUDES);
        addConnectorPlugins(connectController.plugin().sourceConnectors(), SOURCE_CONNECTOR_EXCLUDES);
        addConnectorPlugins(connectController.plugin().transformations(), new ArrayList<>());
        addConnectorPlugins(connectController.plugin().converters(), Collections.emptySet());
    }

    private <T> void addConnectorPlugins(Collection<PluginWrapper<T>> plugins, Collection<Class<? extends T>> excludes) {
        plugins.stream()
                .filter(p -> !excludes.contains(p.pluginClass()))
                .map(PluginInfo::new)
                .forEach(connectorPlugins::add);
    }


    /**
     * validate plugin configs
     * @param context
     * @throws Throwable
     */
    public void validateConfigs(Context context) {
        // No-op
    }

    /**
     * list connector plugins
     * @param context
     * @return
     */
    public void listConnectorPlugins(Context context) {
       boolean connectorsOnly = context.anyFormParamNull("connectorsOnly")
               ? false : Boolean.parseBoolean(context.pathParam("connectorsOnly")) ;
        synchronized (this) {
            if (connectorsOnly) {
                List<PluginInfo> pluginInfos = Collections.unmodifiableList(connectorPlugins.stream()
                        .filter(p -> PluginType.SINK.toString().equals(p.getType()) || PluginType.SOURCE.toString().equals(p.getType()))
                        .collect(Collectors.toList()));
                context.result(JSON.toJSONString(pluginInfos));
            } else {
                context.result(JSON.toJSONString(Collections.unmodifiableList(connectorPlugins)));
            }
        }
    }

    /**
     * Get connector config def
     * @param context
     * @return
     */
    public List<ConfigKeyInfo> getConnectorConfigDef(Context context) {
        // No-op
        return Collections.emptyList();
    }

    /**
     * reload plugins
     * @param context
     */
    public void reloadPlugins(Context context) {
        connectController.getConfigManagementService().getPlugin().initLoaders();
        context.result("success");
    }
}

