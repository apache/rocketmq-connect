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
import com.alibaba.fastjson.JSONObject;
import io.openmessaging.connector.api.component.connector.Connector;
import io.openmessaging.connector.api.component.task.source.SourceTask;
import io.openmessaging.connector.api.data.ConnectRecord;
import io.openmessaging.connector.api.data.RecordConverter;
import io.openmessaging.internal.DefaultKeyValue;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.connect.runtime.common.ConnectKeyValue;
import org.apache.rocketmq.connect.runtime.config.ConnectorConfig;
import org.apache.rocketmq.connect.runtime.config.WorkerConfig;
import org.apache.rocketmq.connect.runtime.connectorwrapper.TransformChain;
import org.apache.rocketmq.connect.runtime.connectorwrapper.Worker;
import org.apache.rocketmq.connect.runtime.connectorwrapper.WorkerConnector;
import org.apache.rocketmq.connect.runtime.connectorwrapper.WorkerSourceTask;
import org.apache.rocketmq.connect.runtime.connectorwrapper.WorkerState;
import org.apache.rocketmq.connect.runtime.controller.distributed.DistributedConnectController;
import org.apache.rocketmq.connect.runtime.controller.isolation.Plugin;
import org.apache.rocketmq.connect.runtime.controller.isolation.PluginClassLoader;
import org.apache.rocketmq.connect.runtime.errors.ErrorMetricsGroup;
import org.apache.rocketmq.connect.runtime.errors.ReporterManagerUtil;
import org.apache.rocketmq.connect.runtime.errors.RetryWithToleranceOperator;
import org.apache.rocketmq.connect.runtime.metrics.ConnectMetrics;
import org.apache.rocketmq.connect.runtime.rest.entities.PluginInfo;
import org.apache.rocketmq.connect.runtime.service.DefaultConnectorContext;
import org.apache.rocketmq.connect.runtime.service.local.LocalPositionManagementServiceImpl;
import org.apache.rocketmq.connect.runtime.stats.ConnectStatsManager;
import org.apache.rocketmq.connect.runtime.stats.ConnectStatsService;
import org.apache.rocketmq.connect.runtime.utils.ConnectorTaskId;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.net.URI;
import java.net.URL;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class RestHandlerTest {

    private static final String CREATE_CONNECTOR_URL = "http://localhost:8081/connectors/%s";
    private static final String STOP_CONNECTOR_URL = "http://localhost:8081/connectors/%s/stop";
    private static final String GET_CLUSTER_INFO_URL = "http://localhost:8081/getClusterInfo";
    private static final String GET_CONFIG_INFO_URL = "http://localhost:8081/getConfigInfo";
    private static final String GET_POSITION_INFO_URL = "http://localhost:8081/getPositionInfo";
    private static final String GET_ALLOCATED_CONNECTORS_URL = "http://localhost:8081/allocated/connectors";
    private static final String GET_ALLOCATED_TASKS_URL = "http://localhost:8081/allocated/tasks";
    private static final String QUERY_CONNECTOR_CONFIG_URL = "http://localhost:8081/connectors/testConnector/config";
    private static final String QUERY_CONNECTOR_STATUS_URL = "http://localhost:8081/connectors/testConnector/status";
    private static final String STOP_ALL_CONNECTOR_URL = "http://localhost:8081/connectors/stop/all";
    private static final String PLUGIN_LIST_URL = "http://localhost:8081/plugin/list";
    private static final String PLUGIN_RELOAD_URL = "http://localhost:8081/plugin/reload";
    @Mock
    private DistributedConnectController connectController;
    @Mock
    private Worker worker;
    @Mock
    private DefaultMQProducer producer;
    private RestHandler restHandler;
    @Mock
    private WorkerConfig connectConfig;
    @Mock
    private SourceTask sourceTask;
    @Mock
    private RecordConverter converter;
    @Mock
    private LocalPositionManagementServiceImpl positionManagementServiceImpl;
    @Mock
    private Connector connector;
    private byte[] sourcePartition;
    private byte[] sourcePosition;
    private Map<ByteBuffer, ByteBuffer> positions;
    private HttpClient httpClient;

    private List<String> aliveWorker;

    private Map<String, ConnectKeyValue> connectorConfigs;

    private Map<String, List<ConnectKeyValue>> taskConfigs;

    private Set<WorkerConnector> workerConnectors;

    private Set<Runnable> workerTasks;

    private AtomicReference<WorkerState> workerState;

    @Mock
    private ConnectStatsManager connectStatsManager;

    @Mock
    private ConnectStatsService connectStatsService;

    private PluginClassLoader pluginClassLoader;

    @Before
    public void init() throws Exception {
        workerState = new AtomicReference<>(WorkerState.STARTED);

        when(connectController.getConnectConfig()).thenReturn(connectConfig);
        when(connectConfig.getHttpPort()).thenReturn(8081);

        String connectName = "testConnector";
        ConnectKeyValue connectKeyValue = new ConnectKeyValue();
        connectKeyValue.put(ConnectorConfig.CONNECTOR_CLASS, "org.apache.rocketmq.connect.runtime.service.TestConnector");
        connectKeyValue.put(ConnectorConfig.VALUE_CONVERTER, "source-record-converter");

        ConnectKeyValue connectKeyValue1 = new ConnectKeyValue();
        connectKeyValue1.put(ConnectorConfig.CONNECTOR_CLASS, "org.apache.rocketmq.connect.runtime.service.TestConnector");
        connectKeyValue1.put(ConnectorConfig.VALUE_CONVERTER, "source-record-converter1");

        List<ConnectKeyValue> connectKeyValues = new ArrayList<ConnectKeyValue>(8) {
            {
                add(connectKeyValue);
            }
        };
        connectorConfigs = new HashMap<String, ConnectKeyValue>() {
            {
                put(connectName, connectKeyValue);
            }
        };
        taskConfigs = new HashMap<String, List<ConnectKeyValue>>() {
            {
                put(connectName, connectKeyValues);
            }
        };

        aliveWorker = new ArrayList<String>() {
            {
                add("workerId1");
                add("workerId2");
            }
        };


        sourcePartition = "127.0.0.13306".getBytes("UTF-8");
        JSONObject jsonObject = new JSONObject();
        sourcePosition = jsonObject.toJSONString().getBytes();
        positions = new HashMap<ByteBuffer, ByteBuffer>() {
            {
                put(ByteBuffer.wrap(sourcePartition), ByteBuffer.wrap(sourcePosition));
            }
        };

        WorkerConnector workerConnector1 = new WorkerConnector("testConnectorName1", connector, connectKeyValue, new DefaultConnectorContext("testConnectorName1", connectController), null, null);
        WorkerConnector workerConnector2 = new WorkerConnector("testConnectorName2", connector, connectKeyValue1, new DefaultConnectorContext("testConnectorName2", connectController), null, null);
        workerConnectors = new HashSet<WorkerConnector>() {
            {
                add(workerConnector1);
                add(workerConnector2);
            }
        };
        TransformChain<ConnectRecord> transformChain = new TransformChain<ConnectRecord>(new DefaultKeyValue(), new Plugin(new ArrayList<>()));
        // create retry operator
        RetryWithToleranceOperator retryWithToleranceOperator = ReporterManagerUtil.createRetryWithToleranceOperator(connectKeyValue, new ErrorMetricsGroup(new ConnectorTaskId("testConnectorName", 1), new ConnectMetrics(new WorkerConfig())));
        retryWithToleranceOperator.reporters(ReporterManagerUtil.sourceTaskReporters(new ConnectorTaskId("testConnectorName", 1), connectKeyValue, new ErrorMetricsGroup(new ConnectorTaskId("testConnectorName", 1), new ConnectMetrics(new WorkerConfig()))));

        WorkerSourceTask workerSourceTask1 = new WorkerSourceTask(new WorkerConfig(), new ConnectorTaskId("testConnectorName1", 1), sourceTask, null, connectKeyValue, positionManagementServiceImpl, converter, converter, producer, workerState, connectStatsManager, connectStatsService, transformChain, retryWithToleranceOperator, null, new ConnectMetrics(new WorkerConfig()));

        // create retry operator
        RetryWithToleranceOperator retryWithToleranceOperator02 = ReporterManagerUtil.createRetryWithToleranceOperator(connectKeyValue, new ErrorMetricsGroup(new ConnectorTaskId("testConnectorName", 2), new ConnectMetrics(new WorkerConfig())));
        retryWithToleranceOperator02.reporters(ReporterManagerUtil.sourceTaskReporters(new ConnectorTaskId("testConnectorName", 2), connectKeyValue, new ErrorMetricsGroup(new ConnectorTaskId("testConnectorName", 2), new ConnectMetrics(new WorkerConfig()))));

        WorkerSourceTask workerSourceTask2 = new WorkerSourceTask(new WorkerConfig(), new ConnectorTaskId("testConnectorName", 2), sourceTask, null, connectKeyValue1, positionManagementServiceImpl, converter, converter, producer, workerState, connectStatsManager, connectStatsService, transformChain, retryWithToleranceOperator02, null, new ConnectMetrics(new WorkerConfig()));
        workerTasks = new HashSet<Runnable>() {
            {
                add(workerSourceTask1);
                add(workerSourceTask2);
            }
        };
        when(connectController.getWorker()).thenReturn(worker);

        List<String> pluginPaths = new ArrayList<>();
        pluginPaths.add("src/test/java/org/apache/rocketmq/connect/runtime");
        Plugin plugin = new Plugin(pluginPaths);
        plugin.initLoaders();
        when(connectController.plugin()).thenReturn(plugin);

        URL url = new URL("file://src/test/java/org/apache/rocketmq/connect/runtime");
        URL[] urls = new URL[]{};
        pluginClassLoader = new PluginClassLoader(url, urls);
        Thread.currentThread().setContextClassLoader(pluginClassLoader);
        restHandler = new RestHandler(connectController);

        httpClient = HttpClientBuilder.create().build();
    }

    @Test
    public void testRESTful() throws Exception {
        URIBuilder uriBuilder = new URIBuilder(String.format(CREATE_CONNECTOR_URL, "testConnectorName"));
        uriBuilder.setParameter("config", "{\"connector-class\": \"org.apache.rocketmq.connect.runtime.connectorwrapper.testimpl.TestConnector\",\"mysqlAddr\": \"112.74.179.68\",\"mysqlPort\": \"3306\",\"mysqlUsername\": \"canal\",\"mysqlPassword\": \"canal\",\"source-record-converter\":\"org.apache.rocketmq.connect.runtime.converter.JsonConverter\"}");
        URI uri = uriBuilder.build();
        HttpGet httpGet = new HttpGet(uri);
        HttpResponse httpResponse = httpClient.execute(httpGet);
        assertEquals(200, httpResponse.getStatusLine().getStatusCode());
        final String result = EntityUtils.toString(httpResponse.getEntity(), "UTF-8");
        final Map map = JSON.parseObject(result, Map.class);
        final Object body = map.get("body");
        final JSONObject bodyObject = JSON.parseObject(body.toString());
        assertEquals("org.apache.rocketmq.connect.runtime.connectorwrapper.testimpl.TestConnector", bodyObject.getString("connector-class"));
        assertEquals("112.74.179.68", bodyObject.getString("mysqlAddr"));
        assertEquals("3306", bodyObject.getString("mysqlPort"));
        assertEquals("canal", bodyObject.get("mysqlUsername"));
        assertEquals("canal", bodyObject.get("mysqlPassword"));
        assertEquals("org.apache.rocketmq.connect.runtime.converter.JsonConverter", bodyObject.get("source-record-converter"));

        URIBuilder uriBuilder1 = new URIBuilder(String.format(STOP_CONNECTOR_URL, "testConnectorName"));
        URI uri1 = uriBuilder1.build();
        HttpGet httpGet1 = new HttpGet(uri1);
        HttpResponse httpResponse1 = httpClient.execute(httpGet1);
        assertEquals(200, httpResponse1.getStatusLine().getStatusCode());

        URIBuilder uriBuilder2 = new URIBuilder(GET_CLUSTER_INFO_URL);
        URI uri2 = uriBuilder2.build();
        HttpGet httpGet2 = new HttpGet(uri2);
        HttpResponse httpResponse2 = httpClient.execute(httpGet2);
        assertEquals(200, httpResponse2.getStatusLine().getStatusCode());

        httpClient = HttpClientBuilder.create().build();
        URIBuilder uriBuilder4 = new URIBuilder(GET_ALLOCATED_CONNECTORS_URL);
        URI uri4 = uriBuilder4.build();
        HttpGet httpGet4 = new HttpGet(uri4);
        HttpResponse httpResponse4 = httpClient.execute(httpGet4);
        assertEquals(200, httpResponse4.getStatusLine().getStatusCode());

        httpClient = HttpClientBuilder.create().build();
        URIBuilder uriBuilder5 = new URIBuilder(GET_ALLOCATED_TASKS_URL);
        URI uri5 = uriBuilder5.build();
        HttpGet httpGet5 = new HttpGet(uri5);
        HttpResponse httpResponse5 = httpClient.execute(httpGet5);
        assertEquals(200, httpResponse5.getStatusLine().getStatusCode());

        httpClient = HttpClientBuilder.create().build();
        URIBuilder uriBuilder6 = new URIBuilder(QUERY_CONNECTOR_CONFIG_URL);
        URI uri6 = uriBuilder6.build();
        HttpGet httpGet6 = new HttpGet(uri6);
        HttpResponse httpResponse6 = httpClient.execute(httpGet6);
        assertEquals(200, httpResponse6.getStatusLine().getStatusCode());

        httpClient = HttpClientBuilder.create().build();
        URIBuilder uriBuilder7 = new URIBuilder(QUERY_CONNECTOR_STATUS_URL);
        URI uri7 = uriBuilder7.build();
        HttpGet httpGet7 = new HttpGet(uri7);
        HttpResponse httpResponse7 = httpClient.execute(httpGet7);
        assertEquals(200, httpResponse7.getStatusLine().getStatusCode());

        httpClient = HttpClientBuilder.create().build();
        URIBuilder uriBuilder8 = new URIBuilder(STOP_ALL_CONNECTOR_URL);
        URI uri8 = uriBuilder8.build();
        HttpGet httpGet8 = new HttpGet(uri8);
        HttpResponse httpResponse8 = httpClient.execute(httpGet8);
        assertEquals(200, httpResponse8.getStatusLine().getStatusCode());

        httpClient = HttpClientBuilder.create().build();
        URIBuilder uriBuilder9 = new URIBuilder(PLUGIN_LIST_URL);
        URI uri9 = uriBuilder9.build();
        HttpGet httpGet9 = new HttpGet(uri9);
        HttpResponse httpResponse9 = httpClient.execute(httpGet9);
        assertEquals(200, httpResponse9.getStatusLine().getStatusCode());
        final String result9 = EntityUtils.toString(httpResponse9.getEntity(), "UTF-8");
        final Map bodyMap9 = JSON.parseObject(result9, Map.class);
        final Object body9 = bodyMap9.get("body");

        List<PluginInfo> connectorPlugins = JSON.parseArray(body9.toString(), PluginInfo.class);
        final Map<String, PluginInfo> connectorPluginMap = connectorPlugins.stream().collect(Collectors.toMap(PluginInfo::getClassName, item -> item, (k1, k2) -> k1));
        Assert.assertTrue(connectorPluginMap.containsKey("org.apache.rocketmq.connect.runtime.connectorwrapper.TestTransform"));
        Assert.assertTrue(connectorPluginMap.containsKey("org.apache.rocketmq.connect.runtime.connectorwrapper.testimpl.TestConverter"));

        httpClient = HttpClientBuilder.create().build();
        URIBuilder uriBuilder10 = new URIBuilder(PLUGIN_RELOAD_URL);
        URI uri10 = uriBuilder10.build();
        HttpGet httpGet10 = new HttpGet(uri10);
        HttpResponse httpResponse10 = httpClient.execute(httpGet10);
        assertEquals(200, httpResponse10.getStatusLine().getStatusCode());

    }

}
