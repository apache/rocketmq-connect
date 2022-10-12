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

import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
import org.apache.rocketmq.connect.metrics.MetricName;

/**
 * avg
 */
public class Avg extends AbstractHistogram {

    private final Histogram histogram;
    private final MetricRegistry registry;
    private final MetricName name;

    public Avg(MetricRegistry registry, MetricName name) {
        super(name);
        this.registry = registry;
        this.name = name;
        this.histogram = registry.histogram(name.toString());
    }

    @Override
    public void record(long value) {
        histogram.update(value);
    }

    @Override
    public void close() throws Exception {
        this.registry.remove(name.toString());
    }

    @Override
    public String type() {
        return HistogramType.Avg.name();
    }
}

