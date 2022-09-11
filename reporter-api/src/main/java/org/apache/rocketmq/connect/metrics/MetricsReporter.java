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

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.MetricRegistryListener;
import com.codahale.metrics.Reporter;
import com.codahale.metrics.Timer;

/**
 * rocketmq exporter
 */
public abstract class MetricsReporter implements Reporter, MetricRegistryListener, AutoConfiguration, IReporter {
    private final MetricRegistry registry;

    public MetricsReporter(MetricRegistry registry) {
        this.registry = registry;
        registry.addListener(this);
    }

    /**
     * Called when a {@link Gauge} is added to the registry.
     *
     * @param name  the gauge's name
     * @param gauge the gauge
     */
    public void onGaugeAdded(String name, Gauge<?> gauge) {
        this.onGaugeAdded(MetricUtils.stringToMetricName(name), gauge.getValue());
    }

    public abstract void onGaugeAdded(MetricName name, Object value);

    /**
     * Called when a {@link Gauge} is removed from the registry.
     *
     * @param name the gauge's name
     */
    public void onGaugeRemoved(String name) {
        this.onGaugeRemoved(MetricUtils.stringToMetricName(name));
    }

    public abstract void onGaugeRemoved(MetricName name);

    /**
     * Called when a {@link Counter} is added to the registry.
     *
     * @param name    the counter's name
     * @param counter the counter
     */
    public void onCounterAdded(String name, Counter counter) {
        this.onCounterAdded(MetricUtils.stringToMetricName(name), counter.getCount());
    }

    public abstract void onCounterAdded(MetricName name, Long value);

    /**
     * Called when a {@link Counter} is removed from the registry.
     *
     * @param name the counter's name
     */
    public void onCounterRemoved(String name) {
        this.onCounterRemoved(MetricUtils.stringToMetricName(name));
    }

    public abstract void onCounterRemoved(MetricName name);

    /**
     * Called when a {@link Histogram} is added to the registry.
     *
     * @param name      the histogram's name
     * @param histogram the histogram
     */
    public void onHistogramAdded(String name, Histogram histogram) {
        MetricName metricName = MetricUtils.stringToMetricName(name);
        this.onHistogramAdded(metricName, MetricUtils.getHistogramValue(metricName, histogram));
    }

    public abstract void onHistogramAdded(MetricName name, Double value);

    /**
     * Called when a {@link Histogram} is removed from the registry.
     *
     * @param name the histogram's name
     */
    public void onHistogramRemoved(String name) {
        this.onCounterRemoved(MetricUtils.stringToMetricName(name));
    }

    public abstract void onHistogramRemoved(MetricName name);


    /**
     * Called when a {@link Meter} is added to the registry.
     *
     * @param name  the meter's name
     * @param meter the meter
     */
    public void onMeterAdded(String name, Meter meter) {
        MetricName metricName = MetricUtils.stringToMetricName(name);
        onMeterAdded(metricName, MetricUtils.getMeterValue(metricName, meter));
    }

    public abstract void onMeterAdded(MetricName name, Double value);

    /**
     * Called when a {@link Meter} is removed from the registry.
     *
     * @param name the meter's name
     */
    public void onMeterRemoved(String name) {
        this.onMeterRemoved(MetricUtils.stringToMetricName(name));
    }

    public abstract void onMeterRemoved(MetricName name);

    /**
     * Called when a {@link Timer} is added to the registry.
     *
     * @param name  the timer's name
     * @param timer the timer
     */
    public void onTimerAdded(String name, Timer timer) {
        this.onTimerAdded(MetricUtils.stringToMetricName(name), timer);
    }

    public abstract void onTimerAdded(MetricName name, Timer timer);

    /**
     * Called when a {@link Timer} is removed from the registry.
     *
     * @param name the timer's name
     */
    public void onTimerRemoved(String name) {
        this.onCounterRemoved(MetricUtils.stringToMetricName(name));
    }

    public abstract void onTimerRemoved(MetricName name);
}
