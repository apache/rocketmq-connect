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
package org.apache.rocketmq.connect.runtime.errors;

import org.apache.rocketmq.connect.metrics.stats.CumulativeCount;
import org.apache.rocketmq.connect.runtime.metrics.ConnectMetrics;
import org.apache.rocketmq.connect.runtime.metrics.ConnectMetricsTemplates;
import org.apache.rocketmq.connect.runtime.metrics.MetricGroup;
import org.apache.rocketmq.connect.runtime.metrics.Sensor;
import org.apache.rocketmq.connect.runtime.utils.ConnectorTaskId;



/**
 * error metrics group
 * ToleranceType.ALL. It will be recorded under all, but not under fault tolerance
 */
public class ErrorMetricsGroup implements AutoCloseable {

    private final MetricGroup metricGroup;
    // metrics
    private final Sensor recordProcessingFailures;
    private final Sensor recordProcessingErrors;
    private final Sensor recordsSkipped;
    private final Sensor retries;
    private final Sensor errorsLogged;
    private final Sensor dlqProduceRequests;
    private final Sensor dlqProduceFailures;


    public ErrorMetricsGroup(ConnectorTaskId id, ConnectMetrics connectMetrics) {
        ConnectMetricsTemplates templates = connectMetrics.templates();
        metricGroup = connectMetrics.group(
                templates.connectorTagName(),
                id.connector(),
                templates.taskTagName(),
                Integer.toString(id.task())
        );

        recordProcessingFailures = metricGroup.sensor();
        recordProcessingFailures.addStat(new CumulativeCount(connectMetrics.registry(), metricGroup.name(templates.recordProcessingFailures)));

        recordProcessingErrors = metricGroup.sensor();
        recordProcessingErrors.addStat(new CumulativeCount(connectMetrics.registry(), metricGroup.name(templates.recordProcessingErrors)));

        recordsSkipped = metricGroup.sensor();
        recordsSkipped.addStat(new CumulativeCount(connectMetrics.registry(), metricGroup.name(templates.recordsSkipped)));

        retries = metricGroup.sensor();
        retries.addStat(new CumulativeCount(connectMetrics.registry(), metricGroup.name(templates.retries)));

        errorsLogged = metricGroup.sensor();
        errorsLogged.addStat(new CumulativeCount(connectMetrics.registry(), metricGroup.name(templates.errorsLogged)));

        dlqProduceRequests = metricGroup.sensor();
        dlqProduceRequests.addStat(new CumulativeCount(connectMetrics.registry(), metricGroup.name(templates.dlqProduceFailures)));

        dlqProduceFailures = metricGroup.sensor();
        dlqProduceFailures.addStat(new CumulativeCount(connectMetrics.registry(), metricGroup.name(templates.dlqProduceFailures)));

    }

    public void recordFailure() {
        recordProcessingFailures.record();
    }

    public void recordError() {
        recordProcessingErrors.record();
    }

    public void recordSkipped() {
        recordsSkipped.record();
    }

    public void recordRetry() {
        retries.record();
    }

    public void recordErrorLogged() {
        errorsLogged.record();
    }

    public void recordDeadLetterQueueProduceRequest() {
        dlqProduceRequests.record();
    }

    public void recordDeadLetterQueueProduceFailed() {
        dlqProduceFailures.record();
    }


    @Override
    public void close() throws Exception {
        metricGroup.close();
    }
}
