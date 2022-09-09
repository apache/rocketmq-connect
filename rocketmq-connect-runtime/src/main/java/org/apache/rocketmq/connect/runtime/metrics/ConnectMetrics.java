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

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Reporter;
import com.codahale.metrics.Slf4jReporter;
import org.apache.rocketmq.connect.runtime.common.LoggerName;
import org.apache.rocketmq.connect.runtime.config.WorkerConfig;
import org.apache.rocketmq.connect.runtime.utils.Utils;
import org.slf4j.LoggerFactory;

import java.util.Map;
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
        final Slf4jReporter slf4jReporter = Slf4jReporter.forRegistry(metricRegistry)
                .outputTo(LoggerFactory.getLogger(LoggerName.ROCKETMQ_CONNECT_STATS))
                .convertRatesTo(TimeUnit.SECONDS)
                .convertDurationsTo(TimeUnit.MILLISECONDS)
                .build();
        slf4jReporter.start(10, TimeUnit.SECONDS);

        Map<String, Map<String, String>> metrics = config.getMetricsConfig();
        if (metrics != null && !metrics.isEmpty()) {
            Class[] classes = { MetricRegistry.class};
            Object[] params = { metricRegistry };
            for (Map.Entry<String, Map<String, String>> configs : metrics.entrySet()) {

                try {
                    Reporter reporter = Utils.newInstance(configs.getKey(), Reporter.class, classes, params);
                    if (reporter instanceof ScheduledMetricsReporter) {
                        ((ScheduledMetricsReporter) reporter).config(configs.getValue());
                        ((ScheduledMetricsReporter) reporter).start();
                    }
                    if (reporter instanceof MetricsReporter) {
                       ((MetricsReporter) reporter).config(configs.getValue());
                       ((MetricsReporter) reporter).start();
                    }
                } catch (ClassNotFoundException e) {
                    throw new RuntimeException(e);
                }
            }
        }
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
