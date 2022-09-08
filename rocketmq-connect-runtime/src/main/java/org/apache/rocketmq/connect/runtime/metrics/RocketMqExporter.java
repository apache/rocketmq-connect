package org.apache.rocketmq.connect.runtime.metrics;



import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;

import java.io.IOException;

/**
 * rocketmq exporter
 */
public class RocketMqExporter extends MetricsReporter {

    public RocketMqExporter(MetricRegistry registry) {
        super(registry);
    }

    @Override
    public void onGaugeAdded(MetricName name, Gauge<?> gauge) {

    }

    @Override
    public void onGaugeRemoved(MetricName name) {

    }

    @Override
    public void onCounterAdded(MetricName name, Counter counter) {

    }

    @Override
    public void onCounterRemoved(MetricName name) {

    }

    @Override
    public void onHistogramAdded(MetricName name, Histogram histogram) {

    }

    @Override
    public void onHistogramRemoved(MetricName name) {

    }

    @Override
    public void onMeterAdded(MetricName name, Meter meter) {

    }

    @Override
    public void onMeterRemoved(MetricName name) {

    }

    @Override
    public void onTimerAdded(MetricName name, Timer timer) {

    }

    @Override
    public void onTimerRemoved(MetricName name) {

    }

    @Override
    public void close() throws IOException {

    }
}
