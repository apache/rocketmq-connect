package org.apache.rocketmq.connect.runtime.metrics;

import org.apache.rocketmq.connect.runtime.metrics.stats.Stat;

import java.util.LinkedHashSet;
import java.util.Set;

/**
 * sensor
 */
public class Sensor implements AutoCloseable {
    private final Set<Stat> stats;
    public Sensor(){
        stats = new LinkedHashSet<>();
    }

    public void addStat(Stat stat){
        stats.add(stat);
    }

    /**
     * record one
     */
    public void record() {
        recordInternal(1L);
    }

    /**
     * record value
     * @param value
     */
    public void record(long value) {
        recordInternal(value);
    }



    private synchronized void recordInternal(long value) {
        // increment all the stats
        for (Stat stat : this.stats) {
            stat.record(value);
        }
    }

    @Override
    public void close() throws Exception {
        // increment all the stats
        for (Stat stat : this.stats) {
            stat.close();
        }
    }
}
