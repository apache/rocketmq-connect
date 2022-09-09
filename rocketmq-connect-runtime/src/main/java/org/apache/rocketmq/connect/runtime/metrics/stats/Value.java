/**
 *
 */
package org.apache.rocketmq.connect.runtime.metrics.stats;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import org.apache.rocketmq.connect.runtime.metrics.MetricName;

/**
 * gauge
 */
public class Value implements Stat, Measure {
    private final MetricRegistry registry;
    private final MetricName name;
    private long value;
    public Value(MetricRegistry registry, MetricName name){
        this.registry = registry;
        this.name = name;
        registry.register(name.toString(), new Gauge() {
            @Override
            public Object getValue() {
                return value;
            }
        });
    }

    @Override
    public void record(long value) {
        this.value = value;
    }

    @Override
    public void close() throws Exception {
        registry.remove(name.toString());
    }

    @Override
    public double value() {
        return value;
    }
}
