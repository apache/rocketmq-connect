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
package org.apache.rocketmq.connect.metrics;

import io.javalin.Javalin;
import io.javalin.core.JavalinConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Constructor;
import java.util.HashMap;
import java.util.Map;

/**
 * connect metrics
 */
public abstract class ConnectMetrics {
    private static final String ROCKETMQ_RUNTIME = "RocketMQRuntime";

    private static final Logger log = LoggerFactory.getLogger(ROCKETMQ_RUNTIME);

    private final SinkTaskMetricsGroup noopSinkTaskMetricsGroup = new SinkTaskMetricsGroup() {
        // NO-op
    };
    private final SourceTaskMetricsGroup noopSourceTaskMetricsGroup = new SourceTaskMetricsGroup() {
        // NO-op
    };
    private final TaskMetricsGroup noopTaskMetricsGroup = new TaskMetricsGroup() {
        // NO-op
    };
    private final ErrorMetricsGroup noopErrorMetricsGroup = new ErrorMetricsGroup() {
        // NO-op
    };
    private final WorkerMetricsGroup noopWorkerMetricsGroup = new WorkerMetricsGroup() {
        // NO-op
    };

    public ConnectMetrics(String workerId, Map<String, String> config) {
        // NO-op
    }

    public static ConnectMetrics newInstance(String workerId, Map<String, Map<String, String>> metricsConfig) {
        try {
            for (Map.Entry<String, Map<String, String>> entry : metricsConfig.entrySet()) {
                String clazzName = entry.getKey();
                Class<?> clazz = Class.forName(clazzName);
                if (ConnectMetrics.class.isAssignableFrom(clazz)) {
                    Constructor<?> constructor = clazz.getConstructor(String.class, Map.class);
                    return (ConnectMetrics) constructor.newInstance(workerId, entry.getValue());
                }
            }
        } catch (Exception e) {
            log.error("fail to create ConnectMetrics instance", e);
        }

        return new ConnectMetrics(workerId, new HashMap<>()) {
            // return NO-op connect metrics
        };
    }

    public SinkTaskMetricsGroup getSinkTaskMetricsGroup(MetricsGroupTaskId id) {
        return noopSinkTaskMetricsGroup;
    }

    public SourceTaskMetricsGroup getSourceTaskMetricsGroup(MetricsGroupTaskId id) {
        return noopSourceTaskMetricsGroup;
    }

    public TaskMetricsGroup getTaskMetricsGroup(MetricsGroupTaskId id) {
        return noopTaskMetricsGroup;
    }

    public ErrorMetricsGroup getErrorMetricsGroup(MetricsGroupTaskId id) {
        return noopErrorMetricsGroup;
    }

    public WorkerMetricsGroup getWorkerMetricsGroup() {
        return noopWorkerMetricsGroup;
    }

    public void registerJavaLinMetricsPlugin(JavalinConfig config) {
        // NO-op
    }

    public void registerHttpReporter(Javalin app) {
        // NO-op
    }

    public void start() {
        // NO-op
    }

    public void close() {
        // NO-op
    }

    public void close(MetricsGroupTaskId id) {
        // NO-op
    }
}
