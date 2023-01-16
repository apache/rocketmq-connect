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

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.connect.metrics.MetricsGroupTaskId;
import org.apache.rocketmq.connect.metrics.utils.DurationStyle;

import java.time.Duration;
import java.util.Arrays;
import java.util.Map;
import java.util.Objects;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * micrometer task metrics group abstract class
 */
public abstract class AbstractTaskMetricsGroup {

    private static final String CONNECTOR_TAG_NAME = "connector";
    private static final String TASK_TAG_NAME = "task";
    private static final String SLO_CONFIG_KEY = "micrometer.timer.slo";

    private static final String DEFAULT_SLO = "5ms,10ms,25ms,50ms,100ms,250ms,500ms,1000ms,2500ms,5s,10s";

    private final MeterRegistry meterRegistry;
    private final MetricsGroupTaskId metricsGroupTaskId;
    private final Duration[] slo;

    public AbstractTaskMetricsGroup(Map<String, String> config, MetricsGroupTaskId id, MeterRegistry meterRegistry) {
        this.meterRegistry = meterRegistry;
        this.metricsGroupTaskId = id;

        String strSLO = config.get(SLO_CONFIG_KEY);
        if (StringUtils.isNotBlank(strSLO)) {
            this.slo = slo(strSLO);
        } else {
            this.slo = slo(DEFAULT_SLO);
        }
    }

    protected final Counter createCounter(String metricName, String... tags) {
        return Counter.builder(metricName)
                .tag(TASK_TAG_NAME, metricsGroupTaskId.getTask())
                .tag(CONNECTOR_TAG_NAME, metricsGroupTaskId.getConnector())
                .tags(tags)
                .register(meterRegistry);
    }

    protected final DistributionSummary createDistributionSummary(String metricName, String... tags) {
        return DistributionSummary.builder(metricName)
                .tag(TASK_TAG_NAME, metricsGroupTaskId.getTask())
                .tag(CONNECTOR_TAG_NAME, metricsGroupTaskId.getConnector())
                .tags(tags)
                .register(meterRegistry);
    }

    protected final Timer createTimer(String metricName, String... tags) {
        return Timer.builder(metricName)
                .tag(TASK_TAG_NAME, metricsGroupTaskId.getTask())
                .tag(CONNECTOR_TAG_NAME, metricsGroupTaskId.getConnector())
                .tags(tags)
                .serviceLevelObjectives(slo)
                .register(meterRegistry);
    }

    protected final Gauge createGauge(String metricName, Supplier<Number> supplier, String... tags) {
        return Gauge.builder(metricName, supplier)
                .tag(TASK_TAG_NAME, metricsGroupTaskId.getTask())
                .tag(CONNECTOR_TAG_NAME, metricsGroupTaskId.getConnector())
                .tags(tags)
                .register(meterRegistry);
    }

    protected final void remove(Meter meter) {
        meterRegistry.remove(meter);
        meter.close();
    }

    private Duration[] slo(String strSLO) {
        return Arrays.stream(strSLO.split(","))
                .map(StringUtils::trimToNull)
                .filter(Objects::nonNull)
                .map(DurationStyle::detectAndParse)
                .collect(Collectors.toList())
                .toArray(new Duration[]{});
    }
}
