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
package org.apache.rocketmq.connect.metrics.stats;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import org.apache.rocketmq.connect.metrics.MetricName;

/**
 * gauge
 */
public class Value implements Stat, Measure {
    private final MetricRegistry registry;
    private final MetricName name;
    private long value;

    public Value(MetricRegistry registry, MetricName name) {
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
