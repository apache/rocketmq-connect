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
package org.apache.rocketmq.connect.metrics.micrometer;

import io.javalin.Javalin;
import io.javalin.core.JavalinConfig;
import io.javalin.http.Context;
import io.javalin.http.HttpCode;
import io.javalin.plugin.metrics.MicrometerPlugin;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.binder.jvm.ClassLoaderMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmGcMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmMemoryMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmThreadMetrics;
import io.micrometer.core.instrument.binder.system.DiskSpaceMetrics;
import io.micrometer.core.instrument.binder.system.ProcessorMetrics;
import io.micrometer.core.instrument.binder.system.UptimeMetrics;
import io.micrometer.prometheus.PrometheusConfig;
import io.micrometer.prometheus.PrometheusMeterRegistry;
import io.prometheus.client.exporter.common.TextFormat;
import org.apache.rocketmq.connect.metrics.ConnectMetrics;
import org.apache.rocketmq.connect.metrics.ErrorMetricsGroup;
import org.apache.rocketmq.connect.metrics.MetricsGroupTaskId;
import org.apache.rocketmq.connect.metrics.SinkTaskMetricsGroup;
import org.apache.rocketmq.connect.metrics.SourceTaskMetricsGroup;
import org.apache.rocketmq.connect.metrics.TaskMetricsGroup;
import org.apache.rocketmq.connect.metrics.WorkerMetricsGroup;

import java.io.File;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * connect metrics micrometer implementation
 */
public class ConnectMetricsMicrometerImpl extends ConnectMetrics {

    private static final String WORKER_ID_TAG_NAME = "workerId";
    private static final String METER_REGISTRY_TYPE_CONFIG_KEY = "meter.registry";

    private final MeterRegistry meterRegistry;
    private final Map<MetricsGroupTaskId, SinkTaskMetricsGroup> sinkTaskMetricsGroupMap = new ConcurrentHashMap<>();
    private final Map<MetricsGroupTaskId, SourceTaskMetricsGroup> sourceTaskMetricsGroupMap = new ConcurrentHashMap<>();
    private final Map<MetricsGroupTaskId, TaskMetricsGroup> taskMetricsGroupMap = new ConcurrentHashMap<>();
    private final Map<MetricsGroupTaskId, ErrorMetricsGroup> errorMetricsGroupMap = new ConcurrentHashMap<>();
    private final WorkerMetricsGroup workerMetricsGroup;
    private final Map<String, String> config;

    public ConnectMetricsMicrometerImpl(String workerId, Map<String, String> config) {
        super(workerId, config);

        this.config = config;
        String type = config.get(METER_REGISTRY_TYPE_CONFIG_KEY);
        if ("PrometheusMeterRegistry".equals(type)) {
            meterRegistry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);

            // common tag
            meterRegistry.config().commonTags(WORKER_ID_TAG_NAME, workerId);

            // classloader metrics
            new ClassLoaderMetrics().bindTo(meterRegistry);
            // JVM memory metrics
            new JvmMemoryMetrics().bindTo(meterRegistry);
            // JVM gc metrics
            new JvmGcMetrics().bindTo(meterRegistry);
            // JVM thread metrics
            new JvmThreadMetrics().bindTo(meterRegistry);
            // uptime metrics
            new UptimeMetrics().bindTo(meterRegistry);
            // processor metrics
            new ProcessorMetrics().bindTo(meterRegistry);
            // diskspace metrics
            new DiskSpaceMetrics(new File(System.getProperty("user.home"))).bindTo(meterRegistry);

            // worker metrics group
            workerMetricsGroup = new WorkerMetricsGroupMicrometerImpl(config, meterRegistry);
        } else {
            throw new RuntimeException("not supported meter registry type :" + type);
        }
    }

    @Override
    public SinkTaskMetricsGroup getSinkTaskMetricsGroup(MetricsGroupTaskId id) {
        return sinkTaskMetricsGroupMap.computeIfAbsent(id, key -> new SinkTaskMetricsGroupMicrometerImpl(config, key, meterRegistry));
    }

    @Override
    public SourceTaskMetricsGroup getSourceTaskMetricsGroup(MetricsGroupTaskId id) {
        return sourceTaskMetricsGroupMap.computeIfAbsent(id, key -> new SourceTaskMetricsGroupMicrometerImpl(config, key, meterRegistry));
    }

    @Override
    public TaskMetricsGroup getTaskMetricsGroup(MetricsGroupTaskId id) {
        return taskMetricsGroupMap.computeIfAbsent(id, key -> new TaskMetricsGroupMicrometerImpl(config, key, meterRegistry));
    }

    @Override
    public ErrorMetricsGroup getErrorMetricsGroup(MetricsGroupTaskId id) {
        return errorMetricsGroupMap.computeIfAbsent(id, key -> new ErrorMetricsGroupMicrometerImpl(config, key, meterRegistry));
    }

    @Override
    public WorkerMetricsGroup getWorkerMetricsGroup() {
        return workerMetricsGroup;
    }

    @Override
    public void registerJavaLinMetricsPlugin(JavalinConfig config) {
        config.registerPlugin(new MicrometerPlugin(meterRegistry));
    }

    @Override
    public void registerHttpReporter(Javalin app) {
        if (meterRegistry instanceof PrometheusMeterRegistry) {
            app.get("/metrics", this::getMetrics);
        }
    }

    @Override
    public void close() {
        meterRegistry.close();
    }

    @Override
    public void close(MetricsGroupTaskId id) {
        sinkTaskMetricsGroupMap.computeIfPresent(id, (key, value) -> {
            value.close();
            return null;
        });
        sourceTaskMetricsGroupMap.computeIfPresent(id, (key, value) -> {
            value.close();
            return null;
        });
        taskMetricsGroupMap.computeIfPresent(id, (key, value) -> {
            value.close();
            return null;
        });
        errorMetricsGroupMap.computeIfPresent(id, (key, value) -> {
            value.close();
            return null;
        });
    }

    private void getMetrics(Context context) {
        try {
            context.contentType(TextFormat.CONTENT_TYPE_004)
                    .result(((PrometheusMeterRegistry) meterRegistry).scrape());
        } catch (Exception ex) {
            context.status(HttpCode.INTERNAL_SERVER_ERROR)
                    .result(ex.getMessage());
        }
    }
}
