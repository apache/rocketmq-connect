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
import com.alibaba.fastjson.JSONException;
import io.javalin.Javalin;
import io.javalin.http.Context;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.connect.runtime.common.ConnectKeyValue;
import org.apache.rocketmq.connect.runtime.common.LoggerName;
import org.apache.rocketmq.connect.runtime.connectorwrapper.WorkerConnector;
import org.apache.rocketmq.connect.runtime.connectorwrapper.WorkerTask;
import org.apache.rocketmq.connect.runtime.controller.AbstractConnectController;
import org.apache.rocketmq.connect.runtime.rest.entities.ErrorMessage;
import org.apache.rocketmq.connect.runtime.rest.entities.HttpResponse;
import org.apache.rocketmq.connect.runtime.utils.ConnectorTaskId;
import org.eclipse.jetty.http.HttpStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * A rest handler to process http request.
 */
public class RestHandler {

    private static final Logger log = LoggerFactory.getLogger(LoggerName.ROCKETMQ_RUNTIME);
    private static final String CONNECTOR_NAME = "connectorName";
    private static final String TASK_NAME = "task";
    private final AbstractConnectController connectController;

    /**
     * connector plugin resource
     */
    private ConnectorPluginsResource pluginsResource;

    public RestHandler(AbstractConnectController connectController) {
        this.connectController = connectController;
        pluginsResource = new ConnectorPluginsResource(connectController);

        Javalin app = Javalin.create();
        app = app.start(connectController.getConnectConfig().getHttpPort());

        // cluster
        app.get("/getClusterInfo", this::getClusterInfo);

        // query
        app.get("/connectors/list", this::listConnectors);
        app.get("/allocated/connectors", this::getAllocatedConnectors);
        app.get("/allocated/tasks", this::getAllocatedTasks);
        app.get("/connectors/{connectorName}/config", this::handleQueryConnectorConfig);
        app.get("/connectors/{connectorName}/status", this::handleQueryConnectorStatus);
        app.get("/connectors/{connectorName}/tasks", this::getTaskConfigs);
        app.get("/connectors/{connectorName}/tasks/{task}/status", this::getTaskStatus);

        // create
        app.get("/connectors/{connectorName}", this::handleCreateConnector);
        app.post("/connectors/{connectorName}", this::handleCreateConnector);

        // stop connector
        app.get("/connectors/{connectorName}/stop", this::handleStopConnector);
        app.get("/connectors/stop/all", this::handleStopAllConnector);

        // pause & resume
        app.get("/connectors/{connectorName}/pause", this::handlePauseConnector);
        app.get("/connectors/{connectorName}/resume", this::handleResumeConnector);
        app.get("/connectors/pause/all", this::handlePauseAllConnector);
        app.get("/connectors/resume/all", this::handleResumeAllConnector);

        // plugin
        app.get("/plugin/reload", context -> pluginsResource.reloadPlugins(context));
        app.get("/plugin/list", context -> pluginsResource.listPlugins(context));
        app.get("/plugin/list/connectors", context -> pluginsResource.listConnectorPlugins(context));
        app.get("/plugin/config", context -> pluginsResource.getConnectorConfigDef(context));
        app.get("/plugin/config/validate", context -> pluginsResource.validateConfigs(context));
    }


    /**
     * get cluster info
     *
     * @param context
     */
    private void getClusterInfo(Context context) {
        context.json(new HttpResponse<>(context.status(), connectController.aliveWorkers()));
    }

    /**
     * list all connectors
     *
     * @param context
     */
    private void listConnectors(Context context) {
        try {
            Map<String, Map<String, Object>> out = new HashMap<>();
            for (String connector : connectController.connectors()) {
                Map<String, Object> connectorExpansions = new HashMap<>();
                connectorExpansions.put("status", connectController.connectorStatus(connector));
                connectorExpansions.put("info", connectController.connectorInfo(connector));
                out.put(connector, connectorExpansions);
            }
            context.json(new HttpResponse<>(context.status(), out));
        } catch (Exception ex) {
            log.error("List all connectors failed. ", ex);
            context.json(new ErrorMessage(HttpStatus.INTERNAL_SERVER_ERROR_500, ex.getMessage()));
        }

    }

    private void handleCreateConnector(Context context) {
        String connectorName = context.pathParam(CONNECTOR_NAME);
        String arg;
        if (context.req.getMethod().equals("POST")) {
            arg = context.body();
        } else {
            arg = context.req.getParameter("config");
        }
        if (StringUtils.isBlank(arg)) {
            context.json(new ErrorMessage(HttpStatus.BAD_REQUEST_400, "Failed! query param 'config' is required "));
            return;
        }
        log.info("connect config: {}", arg);

        Map keyValue;
        try {
            keyValue = JSON.parseObject(arg, Map.class);
        } catch (JSONException e) {
            context.json(new ErrorMessage(HttpStatus.BAD_REQUEST_400, "Failed! query param 'config' is malformed"));
            return;
        }

        ConnectKeyValue configs = new ConnectKeyValue();
        for (Object key : keyValue.keySet()) {
            configs.put((String) key, keyValue.get(key).toString());
        }

        try {
            connectController.putConnectorConfig(connectorName, configs);
            context.json(new HttpResponse<>(context.status(), keyValue));
        } catch (Exception e) {
            log.error("Create connector failed .", e);
            context.json(new ErrorMessage(HttpStatus.INTERNAL_SERVER_ERROR_500, e.getMessage()));
        }
    }

    private void handleQueryConnectorConfig(Context context) {
        try {
            String connectorName = context.pathParam(CONNECTOR_NAME);
            context.json(new HttpResponse<>(context.status(), connectController.connectorInfo(connectorName)));
        } catch (Exception ex) {
            log.error("Get connector config failed .", ex);
            context.json(new ErrorMessage(HttpStatus.INTERNAL_SERVER_ERROR_500, ex.getMessage()));
        }
    }

    private void handleQueryConnectorStatus(Context context) {
        try {
            String connectorName = context.pathParam(CONNECTOR_NAME);
            context.json(new HttpResponse<>(context.status(), connectController.connectorStatus(connectorName)));
        } catch (Exception ex) {
            log.error("Get connector status failed .", ex);
            context.json(new ErrorMessage(HttpStatus.INTERNAL_SERVER_ERROR_500, ex.getMessage()));
        }
    }


    public void getTaskConfigs(Context context) {
        String connector = context.pathParam(CONNECTOR_NAME);
        try {
            context.json(new HttpResponse<>(context.status(), connectController.taskConfigs(connector)));
        } catch (Exception ex) {
            log.error("Get task configs failed .", ex);
            context.json(new ErrorMessage(HttpStatus.INTERNAL_SERVER_ERROR_500, ex.getMessage()));
        }
    }


    public void getTaskStatus(Context context) {
        try {
            String connector = context.pathParam(CONNECTOR_NAME);
            Integer task = Integer.valueOf(context.pathParam(TASK_NAME));
            context.json(new HttpResponse<>(context.status(), connectController.taskStatus(new ConnectorTaskId(connector, task))));
        } catch (Exception ex) {
            log.error("Get task status failed .", ex);
            context.json(new ErrorMessage(HttpStatus.INTERNAL_SERVER_ERROR_500, ex.getMessage()));
        }
    }


    private void handleStopConnector(Context context) {
        try {
            String connectorName = context.pathParam(CONNECTOR_NAME);
            connectController.deleteConnectorConfig(connectorName);
            context.json(new HttpResponse<>(context.status(), "Connector [" + connectorName + "] deleted successfully"));
        } catch (Exception e) {
            log.error("Stop connector failed .", e);
            context.json(new ErrorMessage(HttpStatus.INTERNAL_SERVER_ERROR_500, e.getMessage()));
        }
    }

    private void handleStopAllConnector(Context context) {
        Collection<String> connectors = connectController.connectors();
        if (connectors.isEmpty()) {
            context.json(new HttpResponse<>(context.status(), "No connector needs to be deleted, please add connector first"));
            return;
        }
        try {
            for (String connector : connectors) {
                connectController.deleteConnectorConfig(connector);
            }
            context.json(new HttpResponse<>(context.status(), connectors + " connectors are deleted"));
        } catch (Exception ex) {
            log.error("Delete all connector failed {} , {}.", connectors, ex);
            context.json(new ErrorMessage(HttpStatus.INTERNAL_SERVER_ERROR_500, ex.getMessage()));
        }
    }


    private void handlePauseConnector(Context context) {
        String connectorName = context.pathParam(CONNECTOR_NAME);
        try {
            connectController.pauseConnector(connectorName);
            context.json(new HttpResponse<>(context.status(), "Connector [" + connectorName + "] paused successfully"));
        } catch (Exception ex) {
            log.error("Pause connector failed {} , {}.", connectorName, ex);
            context.json(new ErrorMessage(HttpStatus.INTERNAL_SERVER_ERROR_500, ex.getMessage()));
        }

    }

    private void handleResumeConnector(Context context) {
        String connectorName = context.pathParam(CONNECTOR_NAME);
        try {
            connectController.resumeConnector(connectorName);
            context.json(new HttpResponse<>(context.status(), "Connector [" + connectorName + "] resumed successfully"));
        } catch (Exception ex) {
            log.error("Resume connector failed {} , {}.", connectorName, ex);
            context.json(new ErrorMessage(HttpStatus.INTERNAL_SERVER_ERROR_500, ex.getMessage()));
        }
    }


    private void handlePauseAllConnector(Context context) {
        Collection<String> conns = connectController.connectors();
        if (conns.isEmpty()) {
            context.json(new HttpResponse<>(context.status(), "No connector needs to be paused, please add connector first"));
            return;
        }
        try {
            conns.forEach(conn -> {
                connectController.pauseConnector(conn);
            });
            context.json(new HttpResponse<>(context.status(), conns.size() + " connectors are suspended"));
        } catch (Exception ex) {
            log.error("Pause all connector failed {} , {}.", conns, ex);
            context.json(new ErrorMessage(HttpStatus.INTERNAL_SERVER_ERROR_500, ex.getMessage()));
        }
    }

    private void handleResumeAllConnector(Context context) {
        Collection<String> conns = connectController.connectors();
        if (conns.isEmpty()) {
            context.json(new HttpResponse<>(context.status(), "No connector needs to be resumed, please add connector first"));
            return;
        }
        try {
            conns.forEach(conn -> {
                connectController.resumeConnector(conn);
            });
            context.json(new HttpResponse<>(context.status(), conns.size() + " connectors are resumed"));
        } catch (Exception ex) {
            log.error("Pause all connector failed {} , {}.", conns, ex);
            context.json(new ErrorMessage(HttpStatus.INTERNAL_SERVER_ERROR_500, ex.getMessage()));
        }
    }

    // old rest api

    private void getAllocatedConnectors(Context context) {
        try {
            Set<WorkerConnector> workerConnectors = connectController.getWorker().getWorkingConnectors();
            Map<String, Map<String, String>> connectors = new HashMap<>();
            for (WorkerConnector workerConnector : workerConnectors) {
                connectors.put(workerConnector.getConnectorName(), workerConnector.getKeyValue().getProperties());
            }
            context.json(new HttpResponse<>(context.status(), connectors));
        } catch (Exception ex) {
            context.json(new ErrorMessage(HttpStatus.INTERNAL_SERVER_ERROR_500, ex.getMessage()));
        }
    }

    private void getAllocatedTasks(Context context) {
        try {
            Set<Runnable> allErrorTasks = new HashSet<>();
            allErrorTasks.addAll(connectController.getWorker().getErrorTasks());
            allErrorTasks.addAll(connectController.getWorker().getCleanedErrorTasks());

            Set<Runnable> allStoppedTasks = new HashSet<>();
            allStoppedTasks.addAll(connectController.getWorker().getStoppedTasks());
            allStoppedTasks.addAll(connectController.getWorker().getCleanedStoppedTasks());

            Map<String, Object> formatter = new HashMap<>();
            formatter.put("pendingTasks", convertWorkerTaskToString(connectController.getWorker().getPendingTasks()));
            formatter.put("runningTasks", convertWorkerTaskToString(connectController.getWorker().getWorkingTasks()));
            formatter.put("stoppingTasks", convertWorkerTaskToString(connectController.getWorker().getStoppingTasks()));
            formatter.put("stoppedTasks", convertWorkerTaskToString(allStoppedTasks));
            formatter.put("errorTasks", convertWorkerTaskToString(allErrorTasks));
            context.json(new HttpResponse<>(context.status(), formatter));
        } catch (Exception ex) {
            context.json(new ErrorMessage(HttpStatus.INTERNAL_SERVER_ERROR_500, ex.getMessage()));
        }
    }


    private Set<Object> convertWorkerTaskToString(Set<Runnable> tasks) {
        Set<Object> result = new HashSet<>();
        for (Runnable task : tasks) {
            result.add(((WorkerTask) task).currentTaskState());
        }
        return result;
    }
}
