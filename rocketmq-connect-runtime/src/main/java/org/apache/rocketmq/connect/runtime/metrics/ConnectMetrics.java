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

import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.MetricRegistry;
import org.apache.rocketmq.connect.runtime.config.WorkerConfig;

import java.util.concurrent.TimeUnit;


/**
 * connect metrics
 */
public class ConnectMetrics {

    private final MetricRegistry metricRegistry = new MetricRegistry();
    private final String workerId;

    private final ConnectMetricsTemplates templates = new ConnectMetricsTemplates();
    public ConnectMetrics(WorkerConfig config){
        this.workerId = config.getWorkerId();
        ConsoleReporter reporter = ConsoleReporter.forRegistry(metricRegistry).build();
        reporter.start(5, TimeUnit.SECONDS);
    }

    public String workerId(){
        return workerId;
    }

    /**
     * get connect metrics template
     * @return
     */
    public ConnectMetricsTemplates templates(){
        return templates;
    }

    /**
     * get metric registry
     * @return
     */
    public MetricRegistry registry(){
        return metricRegistry;
    }


    public MetricGroup group(String... tagKeyValues) {
        return new MetricGroup(MetricUtils.getTags(tagKeyValues));
    }

}
