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
import org.apache.rocketmq.connect.metrics.stats.Stat;

import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

/**
 * rocketmq exporter
 */
public abstract class ScheduledMetricsReporter extends ScheduledReporter implements AutoConfiguration, AstrictReporter {

    public ScheduledMetricsReporter(MetricRegistry registry) {
        super(registry, "scheduled-reporter", MetricFilter.ALL, TimeUnit.SECONDS, TimeUnit.MILLISECONDS);
    }

    protected ScheduledMetricsReporter(MetricRegistry registry, String name) {
        super(registry, name, MetricFilter.ALL, TimeUnit.SECONDS, TimeUnit.MILLISECONDS);
    }

    @Override
    public void report(SortedMap<String, Gauge> gauges, SortedMap<String, Counter> counters, SortedMap<String, Histogram> histograms, SortedMap<String, Meter> meters, SortedMap<String, Timer> timers) {
        // convert gauges
        SortedMap<MetricName, Gauge> convertGauges = new TreeMap<>();
        for (Map.Entry<String, Gauge> gaugeEntry : gauges.entrySet()) {
            convertGauges.put(MetricUtils.stringToMetricName(gaugeEntry.getKey()), gaugeEntry.getValue());
        }
        // convert counters
        SortedMap<MetricName, Counter> convertCounters = new TreeMap<>();
        for (Map.Entry<String, Counter> counterEntry : counters.entrySet()) {
            convertCounters.put(MetricUtils.stringToMetricName(counterEntry.getKey()), counterEntry.getValue());
        }

        // convert histograms
        SortedMap<MetricName, Histogram> convertHistograms = new TreeMap<>();
        for (Map.Entry<String, Histogram> histogramEntry : histograms.entrySet()) {
            convertHistograms.put(MetricUtils.stringToMetricName(histogramEntry.getKey()), histogramEntry.getValue());
        }

        // convert meters
        SortedMap<MetricName, Meter> convertMeters = new TreeMap<>();
        for (Map.Entry<String, Meter> meterEntry : meters.entrySet()) {
            convertMeters.put(MetricUtils.stringToMetricName(meterEntry.getKey()), meterEntry.getValue());
        }

        // convert timers
        SortedMap<MetricName, Timer> convertTimers = new TreeMap<>();
        for (Map.Entry<String, Timer> timerEntry : timers.entrySet()) {
            convertTimers.put(MetricUtils.stringToMetricName(timerEntry.getKey()), timerEntry.getValue());
        }
        reportMetric(convertGauges, convertCounters, convertHistograms, convertMeters, convertTimers);
    }

    public abstract void reportMetric(SortedMap<MetricName, Gauge> gauges, SortedMap<MetricName, Counter> counters, SortedMap<MetricName, Histogram> histograms, SortedMap<MetricName, Meter> meters, SortedMap<MetricName, Timer> timers);


    protected double histogramValue(MetricName name, Histogram histogram) {
        Stat.HistogramType histogramType = Stat.HistogramType.valueOf(name.getType());
        if (histogramType == null) {
            throw new IllegalArgumentException("No matching type found");
        }
        switch (histogramType) {
            case avg:
                return histogram.getSnapshot().getMean();
            case Max:
                return histogram.getSnapshot().getMax();
            case Min:
                return histogram.getSnapshot().getMin();
            default:
                throw new RuntimeException("Unsupported type " + name.getType());
        }
    }
}
