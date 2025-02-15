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
import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Snapshot;
import com.codahale.metrics.Timer;
import io.prometheus.client.dropwizard.samplebuilder.DefaultSampleBuilder;
import io.prometheus.client.dropwizard.samplebuilder.SampleBuilder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.rocketmq.connect.metrics.stats.Stat;

/**
 * Collect Dropwizard metrics from a MetricRegistry.
 */
public class DropwizardExports extends io.prometheus.client.Collector implements io.prometheus.client.Collector.Describable {
    private static final Logger LOGGER = Logger.getLogger(DropwizardExports.class.getName());
    private MetricRegistry registry;
    private MetricFilter metricFilter;
    private SampleBuilder sampleBuilder;

    /**
     * Creates a new DropwizardExports with a {@link DefaultSampleBuilder} and {@link MetricFilter#ALL}.
     *
     * @param registry a metric registry to export in prometheus.
     */
    public DropwizardExports(MetricRegistry registry) {
        this.registry = registry;
        this.metricFilter = MetricFilter.ALL;
        this.sampleBuilder = new DefaultSampleBuilder();
    }

    /**
     * Creates a new DropwizardExports with a {@link DefaultSampleBuilder} and custom {@link MetricFilter}.
     *
     * @param registry     a metric registry to export in prometheus.
     * @param metricFilter a custom metric filter.
     */
    public DropwizardExports(MetricRegistry registry, MetricFilter metricFilter) {
        this.registry = registry;
        this.metricFilter = metricFilter;
        this.sampleBuilder = new DefaultSampleBuilder();
    }

    /**
     * @param registry      a metric registry to export in prometheus.
     * @param sampleBuilder sampleBuilder to use to create prometheus samples.
     */
    public DropwizardExports(MetricRegistry registry, SampleBuilder sampleBuilder) {
        this.registry = registry;
        this.metricFilter = MetricFilter.ALL;
        this.sampleBuilder = sampleBuilder;
    }

    /**
     * @param registry      a metric registry to export in prometheus.
     * @param metricFilter  a custom metric filter.
     * @param sampleBuilder sampleBuilder to use to create prometheus samples.
     */
    public DropwizardExports(MetricRegistry registry, MetricFilter metricFilter, SampleBuilder sampleBuilder) {
        this.registry = registry;
        this.metricFilter = metricFilter;
        this.sampleBuilder = sampleBuilder;
    }

    private static String getHelpMessage(String metricName, Metric metric) {
        return String.format("Generated from Dropwizard metric import (metric=%s, type=%s)", metricName, metric.getClass().getName());
    }

    /**
     * Export counter as Prometheus <a href="https://prometheus.io/docs/concepts/metric_types/#gauge">Gauge</a>.
     */
    MetricFamilySamples fromCounter(String dropwizardName, Counter counter) {
        MetricFamilySamples.Sample sample = sampleBuilder.createSample(dropwizardName, "", new ArrayList<String>(), new ArrayList<String>(), new Long(counter.getCount()).doubleValue());
        return new MetricFamilySamples(sample.name, Type.GAUGE, getHelpMessage(dropwizardName, counter), Arrays.asList(sample));
    }

    /**
     * Export gauge as a prometheus gauge.
     */
    MetricFamilySamples fromGauge(String dropwizardName, Gauge gauge) {
        Object obj = gauge.getValue();
        double value;
        if (obj instanceof Number) {
            value = ((Number) obj).doubleValue();
        } else if (obj instanceof Boolean) {
            value = ((Boolean) obj) ? 1 : 0;
        } else {
            LOGGER.log(Level.FINE, String.format("Invalid type for Gauge %s: %s", sanitizeMetricName(dropwizardName), obj == null ? "null" : obj.getClass().getName()));
            return null;
        }
        MetricFamilySamples.Sample sample = sampleBuilder.createSample(dropwizardName, "", new ArrayList<String>(), new ArrayList<String>(), value);
        return new MetricFamilySamples(sample.name, Type.GAUGE, getHelpMessage(dropwizardName, gauge), Arrays.asList(sample));
    }

    /**
     * Export a histogram snapshot as a prometheus SUMMARY.
     *
     * @param dropwizardName metric name.
     * @param snapshot       the histogram snapshot.
     * @param count          the total sample count for this snapshot.
     * @param factor         a factor to apply to histogram values.
     */
    MetricFamilySamples fromSnapshotAndCount(String dropwizardName, Snapshot snapshot, long count, double factor,
        String helpMessage) {
        MetricName metricName = MetricUtils.stringToMetricName(dropwizardName);
        Stat.HistogramType histogramType = Stat.HistogramType.valueOf(metricName.getType());
        List<MetricFamilySamples.Sample> samples = new ArrayList<>();
        switch (histogramType) {
            case Avg:
                samples = Arrays.asList(sampleBuilder.createSample(dropwizardName, "", new ArrayList<String>(), new ArrayList<String>(), snapshot.getMean() * factor));
                break;
            case Min:
                samples = Arrays.asList(sampleBuilder.createSample(dropwizardName, "", new ArrayList<String>(), new ArrayList<String>(), snapshot.getMin() * factor));
                break;
            case Max:
                samples = Arrays.asList(sampleBuilder.createSample(dropwizardName, "", new ArrayList<String>(), new ArrayList<String>(), snapshot.getMax() * factor));
                break;
            case Percentile_75th:
                samples = Arrays.asList(sampleBuilder.createSample(dropwizardName, "", Arrays.asList("quantile"), Arrays.asList("0.75"), snapshot.get75thPercentile() * factor));
                break;
            case Percentile_95th:
                samples = Arrays.asList(sampleBuilder.createSample(dropwizardName, "", Arrays.asList("quantile"), Arrays.asList("0.95"), snapshot.get95thPercentile() * factor));
                break;
            case Percentile_98th:
                samples = Arrays.asList(sampleBuilder.createSample(dropwizardName, "", Arrays.asList("quantile"), Arrays.asList("0.98"), snapshot.get98thPercentile() * factor));
                break;
            case Percentile_99th:
                samples = Arrays.asList(sampleBuilder.createSample(dropwizardName, "", Arrays.asList("quantile"), Arrays.asList("0.99"), snapshot.get99thPercentile() * factor));
                break;
            case Percentile_999th:
                samples = Arrays.asList(sampleBuilder.createSample(dropwizardName, "", Arrays.asList("quantile"), Arrays.asList("0.999"), snapshot.get999thPercentile() * factor));
                break;
            default:
                samples = Arrays.asList(sampleBuilder.createSample(dropwizardName, "", Arrays.asList("quantile"), Arrays.asList("0.5"), snapshot.getMedian() * factor), sampleBuilder.createSample(dropwizardName, "", Arrays.asList("quantile"), Arrays.asList("0.5"), snapshot.getMedian() * factor), sampleBuilder.createSample(dropwizardName, "", Arrays.asList("quantile"), Arrays.asList("0.75"), snapshot.get75thPercentile() * factor), sampleBuilder.createSample(dropwizardName, "", Arrays.asList("quantile"), Arrays.asList("0.95"), snapshot.get95thPercentile() * factor), sampleBuilder.createSample(dropwizardName, "", Arrays.asList("quantile"), Arrays.asList("0.98"), snapshot.get98thPercentile() * factor), sampleBuilder.createSample(dropwizardName, "", Arrays.asList("quantile"), Arrays.asList("0.99"), snapshot.get99thPercentile() * factor), sampleBuilder.createSample(dropwizardName, "", Arrays.asList("quantile"), Arrays.asList("0.999"), snapshot.get999thPercentile() * factor), sampleBuilder.createSample(dropwizardName, "_count", new ArrayList<String>(), new ArrayList<String>(), count));

        }
        return new MetricFamilySamples(samples.get(0).name, Type.SUMMARY, helpMessage, samples);
    }

    /**
     * Convert histogram snapshot.
     */
    MetricFamilySamples fromHistogram(String dropwizardName, Histogram histogram) {
        return fromSnapshotAndCount(dropwizardName, histogram.getSnapshot(), histogram.getCount(), 1.0, getHelpMessage(dropwizardName, histogram));
    }

    /**
     * Export Dropwizard Timer as a histogram. Use TIME_UNIT as time unit.
     */
    MetricFamilySamples fromTimer(String dropwizardName, Timer timer) {
        return fromSnapshotAndCount(dropwizardName, timer.getSnapshot(), timer.getCount(), 1.0D / TimeUnit.SECONDS.toNanos(1L), getHelpMessage(dropwizardName, timer));
    }

    /**
     * Export a Meter as as prometheus COUNTER.
     */
    MetricFamilySamples fromMeter(String dropwizardName, Meter meter) {
        MetricName metricName = MetricUtils.stringToMetricName(dropwizardName);
        final MetricFamilySamples.Sample sample = sampleBuilder.createSample(dropwizardName, "", new ArrayList<String>(), new ArrayList<String>(), MetricUtils.getMeterValue(metricName, meter));
        return new MetricFamilySamples(sample.name, Type.COUNTER, getHelpMessage(dropwizardName, meter), Arrays.asList(sample));
    }

    @Override
    public List<MetricFamilySamples> collect() {
        Map<String, MetricFamilySamples> mfSamplesMap = new HashMap<String, MetricFamilySamples>();

        for (SortedMap.Entry<String, Gauge> entry : registry.getGauges(metricFilter).entrySet()) {
            addToMap(mfSamplesMap, fromGauge(entry.getKey(), entry.getValue()));
        }
        for (SortedMap.Entry<String, Counter> entry : registry.getCounters(metricFilter).entrySet()) {
            addToMap(mfSamplesMap, fromCounter(entry.getKey(), entry.getValue()));
        }
        for (SortedMap.Entry<String, Histogram> entry : registry.getHistograms(metricFilter).entrySet()) {
            addToMap(mfSamplesMap, fromHistogram(entry.getKey(), entry.getValue()));
        }
        for (SortedMap.Entry<String, Timer> entry : registry.getTimers(metricFilter).entrySet()) {
            addToMap(mfSamplesMap, fromTimer(entry.getKey(), entry.getValue()));
        }
        for (SortedMap.Entry<String, Meter> entry : registry.getMeters(metricFilter).entrySet()) {
            addToMap(mfSamplesMap, fromMeter(entry.getKey(), entry.getValue()));
        }
        return new ArrayList<MetricFamilySamples>(mfSamplesMap.values());
    }

    private void addToMap(Map<String, MetricFamilySamples> mfSamplesMap, MetricFamilySamples newMfSamples) {
        if (newMfSamples != null) {
            MetricFamilySamples currentMfSamples = mfSamplesMap.get(newMfSamples.name);
            if (currentMfSamples == null) {
                mfSamplesMap.put(newMfSamples.name, newMfSamples);
            } else {
                Set<MetricFamilySamples.Sample> samples = new HashSet<MetricFamilySamples.Sample>(currentMfSamples.samples);
                samples.addAll(newMfSamples.samples);
                List<MetricFamilySamples.Sample> list = new ArrayList<>(samples);
                mfSamplesMap.put(newMfSamples.name, new MetricFamilySamples(newMfSamples.name, currentMfSamples.type, currentMfSamples.help, list));
            }
        }
    }

    @Override
    public List<MetricFamilySamples> describe() {
        return new ArrayList<MetricFamilySamples>();
    }
}