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

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.binder.jvm.ExecutorServiceMetrics;
import org.apache.rocketmq.connect.metrics.WorkerMetricsGroup;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.function.Supplier;

/**
 * worker metrics group micrometer implementation
 */
public class WorkerMetricsGroupMicrometerImpl implements WorkerMetricsGroup {
    private final MeterRegistry meterRegistry;
    private final Map<String, Gauge> workerState = new ConcurrentHashMap<>();


    public WorkerMetricsGroupMicrometerImpl(Map<String, String> config, MeterRegistry meterRegistry) {
        this.meterRegistry = meterRegistry;
    }

    @Override
    public void registerConnectorStateMetrics(String connectorName, Supplier<Number> supplier, String... tags) {
        workerState.computeIfAbsent(connectorName, key -> Gauge.builder("connector.state", supplier)
                .tag("connector", key)
                .tags(tags)
                .register(meterRegistry));
    }

    @Override
    public void registerExecutorServiceMetrics(String executorName, ExecutorService executorService) {
        new ExecutorServiceMetrics(executorService,
                executorName,
                null)
                .bindTo(meterRegistry);
    }

    @Override
    public void close() {
    }
}
