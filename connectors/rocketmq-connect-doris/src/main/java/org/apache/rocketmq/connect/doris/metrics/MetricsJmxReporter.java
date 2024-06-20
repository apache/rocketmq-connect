/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.rocketmq.connect.doris.metrics;

import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.jmx.JmxReporter;
import java.util.Arrays;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.connect.doris.exception.DorisException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MetricsJmxReporter {
    static final Logger LOG = LoggerFactory.getLogger(MetricsJmxReporter.class);

    private final MetricRegistry metricRegistry;

    /**
     * Wrapper on top of listeners and metricRegistry for codehale. This will be useful to start the
     * jmx metrics when time is appropriate. (Check {@link MetricsJmxReporter#start()}
     */
    private final JmxReporter jmxReporter;

    public MetricsJmxReporter(MetricRegistry metricRegistry, final String connectorName) {
        this.metricRegistry = metricRegistry;
        this.jmxReporter = createJMXReporter(connectorName);
    }

    public MetricRegistry getMetricRegistry() {
        return metricRegistry;
    }

    /**
     * This function will internally register all metrics present inside metric registry and will
     * register mbeans to the mbeanserver
     */
    public void start() {
        jmxReporter.start();
    }

    private static ObjectName getObjectName(
        String connectorName, String jmxDomain, String metricName) {
        try {
            StringBuilder sb =
                new StringBuilder(jmxDomain)
                    .append(":connector=")
                    .append(connectorName)
                    .append(',');

            Iterator<String> tokens = Arrays.stream(StringUtils.split(metricName, "/")).iterator();
            sb.append("task=").append(tokens.next());
            sb.append(",category=").append(tokens.next());
            sb.append(",name=").append(tokens.next());

            return new ObjectName(sb.toString());
        } catch (MalformedObjectNameException e) {
            LOG.warn("Could not create Object name for MetricName:{}", metricName);
            throw new DorisException("Object Name is invalid");
        }
    }

    public void removeMetricsFromRegistry(final String prefixFilter) {
        if (!metricRegistry.getMetrics().isEmpty()) {
            LOG.debug("Unregistering all metrics:{}", prefixFilter);
            metricRegistry.removeMatching(MetricFilter.startsWith(prefixFilter));
            LOG.debug(
                "Metric registry:{}, size is:{}, names:{}",
                prefixFilter,
                metricRegistry.getMetrics().size(),
                metricRegistry.getMetrics().keySet().toString());
        }
    }

    private JmxReporter createJMXReporter(final String connectorName) {
        return JmxReporter.forRegistry(this.metricRegistry)
            .inDomain(MetricsUtil.JMX_METRIC_PREFIX)
            .convertDurationsTo(TimeUnit.SECONDS)
            .createsObjectNamesWith(
                (ignoreMeterType, jmxDomain, metricName) ->
                    getObjectName(connectorName, jmxDomain, metricName))
            .build();
    }
}
