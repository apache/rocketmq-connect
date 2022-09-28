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
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.ScheduledReporter;
import com.codahale.metrics.Timer;

import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

/**
 * rocketmq exporter
 */
public abstract class ScheduledMetricsReporter extends ScheduledReporter implements AutoConfiguration, IReporter {

    public ScheduledMetricsReporter(MetricRegistry registry) {
        super(registry, "scheduled-reporter", MetricFilter.ALL, TimeUnit.SECONDS, TimeUnit.MILLISECONDS);
    }

    protected ScheduledMetricsReporter(MetricRegistry registry, String name) {
        super(registry, name, MetricFilter.ALL, TimeUnit.SECONDS, TimeUnit.MILLISECONDS);
    }

    @Override
    public void report(SortedMap<String, Gauge> gauges, SortedMap<String, Counter> counters, SortedMap<String, Histogram> histograms, SortedMap<String, Meter> meters, SortedMap<String, Timer> timers) {
        // convert gauges
        SortedMap<MetricName, Object> convertGauges = new TreeMap<>();
        for (Map.Entry<String, Gauge> gaugeEntry : gauges.entrySet()) {
            convertGauges.put(MetricUtils.stringToMetricName(gaugeEntry.getKey()), gaugeEntry.getValue().getValue());
        }
        // convert counters
        SortedMap<MetricName, Long> convertCounters = new TreeMap<>();
        for (Map.Entry<String, Counter> counterEntry : counters.entrySet()) {
            convertCounters.put(MetricUtils.stringToMetricName(counterEntry.getKey()), counterEntry.getValue().getCount());
        }

        // convert histograms
        SortedMap<MetricName, Double> convertHistograms = new TreeMap<>();
        for (Map.Entry<String, Histogram> histogramEntry : histograms.entrySet()) {
            MetricName metricName = MetricUtils.stringToMetricName(histogramEntry.getKey());
            convertHistograms.put(metricName, MetricUtils.getHistogramValue(metricName, histogramEntry.getValue()));
        }

        // convert meters
        SortedMap<MetricName, Double> convertMeters = new TreeMap<>();
        for (Map.Entry<String, Meter> meterEntry : meters.entrySet()) {
            MetricName metricName = MetricUtils.stringToMetricName(meterEntry.getKey());
            convertMeters.put(MetricUtils.stringToMetricName(meterEntry.getKey()), MetricUtils.getMeterValue(metricName, meterEntry.getValue()));
        }

        // convert timers
        SortedMap<MetricName, Timer> convertTimers = new TreeMap<>();
        for (Map.Entry<String, Timer> timerEntry : timers.entrySet()) {
            convertTimers.put(MetricUtils.stringToMetricName(timerEntry.getKey()), timerEntry.getValue());
        }
        reportMetric(convertGauges, convertCounters, convertHistograms, convertMeters, convertTimers);
    }

    public abstract void reportMetric(SortedMap<MetricName, Object> gauges, SortedMap<MetricName, Long> counters, SortedMap<MetricName, Double> histograms, SortedMap<MetricName, Double> meters, SortedMap<MetricName, Timer> timers);


    @Override
    public void close() {
        super.close();
    }
}
