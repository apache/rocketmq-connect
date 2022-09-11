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
package org.apache.rocketmq.connect.runtime.metrics;

import org.apache.rocketmq.connect.metrics.stats.Stat;

import java.util.LinkedHashSet;
import java.util.Set;

/**
 * sensor
 */
public class Sensor implements AutoCloseable {
    private final Set<Stat> stats;

    public Sensor() {
        stats = new LinkedHashSet<>();
    }

    public void addStat(Stat stat) {
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
     *
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
