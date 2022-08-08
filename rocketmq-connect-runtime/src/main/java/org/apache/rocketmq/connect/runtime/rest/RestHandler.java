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

package org.apache.rocketmq.connect.runtime.rest;

import com.alibaba.fastjson.JSON;
import io.javalin.Context;
import io.javalin.Javalin;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.rocketmq.connect.runtime.controller.AbstractConnectController;
import org.apache.rocketmq.connect.runtime.common.ConnectKeyValue;
import org.apache.rocketmq.connect.runtime.common.LoggerName;
import org.apache.rocketmq.connect.runtime.connectorwrapper.WorkerConnector;
import org.apache.rocketmq.connect.runtime.connectorwrapper.WorkerTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A rest handler to process http request.
 */
public class RestHandler {

    private static final Logger log = LoggerFactory.getLogger(LoggerName.ROCKETMQ_RUNTIME);
    private final AbstractConnectController connectController;

    public RestHandler(AbstractConnectController connectController) {
        this.connectController = connectController;
        Javalin app = Javalin.create();
        app.enableCaseSensitiveUrls();
        app = app.start(connectController.getConnectConfig().getHttpPort());
        app.get("/getClusterInfo", this::getClusterInfo);
        app.get("/listConnectors", this::listConnectors);

        app.get("/connectors/stopAll", this::handleStopAllConnector);
        app.get("/connectors/pauseAll", this::handlePauseAllConnector);
        app.get("/connectors/resumeAll", this::handleResumeAllConnector);
        app.get("/connectors/:connectorName", this::handleCreateConnector);
        app.post("/connectors/:connectorName", this::handleCreateConnector);
        app.get("/connectors/:connectorName/config", this::handleQueryConnectorConfig);
        app.get("/connectors/:connectorName/status", this::handleQueryConnectorStatus);
        app.get("/connectors/:connectorName/stop", this::handleStopConnector);
        app.get("/connectors/:connectorName/pause", this::handlePauseConnector);
        app.get("/connectors/:connectorName/resume", this::handleResumeConnector);

        app.get("/getAllocatedConnectors", this::getAllocatedConnectors);
        app.get("/getAllocatedTasks", this::getAllocatedTasks);
        app.get("/plugin/reload", this::reloadPlugins);
    }


    /**
     * get cluster info
     * @param context
     */
    private void getClusterInfo(Context context) {
        context.json(connectController.getClusterManagementService().getAllAliveWorkers());
    }

    /**
     * list all connectors
     * @param context
     */
    private void listConnectors(Context context) {
        Map<String, Map<String, Object>> out = new HashMap<>();
        for (String connector : connectController.connectors()) {
            Map<String, Object> connectorExpansions = new HashMap<>();
            connectorExpansions.put("status", connectController.connectorStatus(connector));
            connectorExpansions.put("info", connectController.connectorInfo(connector));
            out.put(connector, connectorExpansions);
        }
        context.json(out);
    }

    private void handleCreateConnector(Context context) {
        String connectorName = context.pathParam("connectorName");
        String arg;
        if (context.req.getMethod().equals("POST")) {
            arg = context.body();
        } else {
            arg = context.req.getParameter("config");
        }
        if (arg == null) {
            context.result("failed! query param 'config' is required ");
            return;
        }
        log.info("config: {}", arg);
        Map keyValue = JSON.parseObject(arg, Map.class);
        ConnectKeyValue configs = new ConnectKeyValue();
        for (Object key : keyValue.keySet()) {
            configs.put((String) key, keyValue.get(key).toString());
        }
        try {
            context.result(connectController.getConfigManagementService().putConnectorConfig(connectorName, configs));
        } catch (Exception e) {
            log.error("Handle createConnector error .", e);
            context.json(e);
        }
    }


    private void handleQueryConnectorConfig(Context context) {
        String connectorName = context.pathParam("connectorName");
        context.json(connectController.connectorInfo(connectorName));
    }

    private void handleQueryConnectorStatus(Context context) {
        String connectorName = context.pathParam("connectorName");
        context.json(connectController.connectorStatus(connectorName));
    }

    private void handleStopConnector(Context context) {
        String connectorName = context.pathParam("connectorName");
        try {
            connectController.getConfigManagementService().deleteConnectorConfig(connectorName);
            context.result("Connector deleted successfully");
        } catch (Exception e) {
            context.json(e);
        }
    }

    private void handlePauseAllConnector(Context context) {
        Collection<String> conns = connectController.connectors();
        conns.forEach(conn->{
            connectController.pauseConnector(conn);
        });
    }

    private void handleResumeAllConnector(Context context) {
        Collection<String> conns = connectController.connectors();
        conns.forEach(conn->{
            connectController.resumeConnector(conn);
        });
    }


    private void handlePauseConnector(Context context) {
        String connector = context.pathParam("connector");
        connectController.pauseConnector(connector);
        context.result("success");
    }

    private void handleResumeConnector(Context context) {
        String connector = context.pathParam("connector");
        connectController.resumeConnector(connector);
        context.result("success");
    }

    private void reloadPlugins(Context context) {
        connectController.getConfigManagementService().getPlugin().initPlugin();
        context.result("success");
    }

    private void handleStopAllConnector(Context context) {
        try {
            Map<String, ConnectKeyValue> connectorConfigs = connectController.getConfigManagementService().getConnectorConfigs();
            for (String connector : connectorConfigs.keySet()) {
                connectController.getConfigManagementService().deleteConnectorConfig(connector);
            }
            context.result("success");
        } catch (Exception e) {
            context.json(e);
        }
    }


    // old rest api

    private void getAllocatedConnectors(Context context) {
        Set<WorkerConnector> workerConnectors = connectController.getWorker().getWorkingConnectors();
        Map<String, Map<String,String>> connectors = new HashMap<>();
        for (WorkerConnector workerConnector : workerConnectors) {
            connectors.put(workerConnector.getConnectorName(), workerConnector.getKeyValue().getProperties());
        }
        context.json(JSON.toJSONString(connectors));
    }

    private void getAllocatedTasks(Context context) {
        StringBuilder sb = new StringBuilder();
        Set<Runnable> allErrorTasks = new HashSet<>();
        allErrorTasks.addAll(connectController.getWorker().getErrorTasks());
        allErrorTasks.addAll(connectController.getWorker().getCleanedErrorTasks());

        Set<Runnable> allStoppedTasks = new HashSet<>();
        allStoppedTasks.addAll(connectController.getWorker().getStoppedTasks());
        allStoppedTasks.addAll(connectController.getWorker().getCleanedStoppedTasks());

        Map<String, Object> formatter = new HashMap<>();
        formatter.put("pendingTasks", convertWorkerTaskToString(connectController.getWorker().getPendingTasks()));
        formatter.put("runningTasks",  convertWorkerTaskToString(connectController.getWorker().getWorkingTasks()));
        formatter.put("stoppingTasks",  convertWorkerTaskToString(connectController.getWorker().getStoppingTasks()));
        formatter.put("stoppedTasks",  convertWorkerTaskToString(allStoppedTasks));
        formatter.put("errorTasks",  convertWorkerTaskToString(allErrorTasks));

        context.result(JSON.toJSONString(formatter));
    }


    private Set<Object> convertWorkerTaskToString(Set<Runnable> tasks) {
        Set<Object> result = new HashSet<>();
        for (Runnable task : tasks) {
            result.add(((WorkerTask) task).currentTaskState());
        }
        return result;
    }
}
