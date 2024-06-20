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

import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DorisConnectMonitor {

    private static final Logger LOG = LoggerFactory.getLogger(DorisConnectMonitor.class);

    // committed offset in doris
    private final AtomicLong committedOffset;

    // total record flushed in doris
    private final AtomicLong totalNumberOfRecord;
    private final AtomicLong totalSizeOfData;
    // total number of data successfully imported to doris through stream-load (or the total number
    // of data files uploaded through copy-into).
    private final AtomicLong totalLoadCount;

    // buffer metrics, updated everytime when a buffer is flushed
    private Histogram partitionBufferSizeBytesHistogram; // in Bytes
    private Histogram partitionBufferCountHistogram;
    private final AtomicLong buffMemoryUsage;
    private final int taskId;

    public DorisConnectMonitor(
        final boolean enableCustomJMXConfig,
        final Integer taskId,
        final MetricsJmxReporter metricsJmxReporter) {
        this.committedOffset = new AtomicLong(-1);

        this.totalLoadCount = new AtomicLong(0);
        this.totalNumberOfRecord = new AtomicLong(0);
        this.totalSizeOfData = new AtomicLong(0);

        this.buffMemoryUsage = new AtomicLong(0);
        this.taskId = taskId;
        if (enableCustomJMXConfig) {
            registerJMXMetrics(metricsJmxReporter);
            LOG.info("init DorisConnectMonitor, taskId={}", taskId);
        }
    }

    /**
     * Registers all the Metrics inside the metricRegistry.
     *
     * @param metricsJmxReporter wrapper class for registering all metrics related to above
     *                           connector
     */
    private void registerJMXMetrics(MetricsJmxReporter metricsJmxReporter) {
        MetricRegistry currentMetricRegistry = metricsJmxReporter.getMetricRegistry();

        // Lazily remove all registered metrics from the registry since this can be invoked during
        // partition reassignment
        LOG.debug(
            "Registering metrics existing:{}",
            metricsJmxReporter.getMetricRegistry().getMetrics().keySet().toString());
        metricsJmxReporter.removeMetricsFromRegistry(String.valueOf(taskId));

        try {
            // Offset JMX
            currentMetricRegistry.register(
                MetricsUtil.constructMetricName(
                    taskId, MetricsUtil.OFFSET_DOMAIN, MetricsUtil.COMMITTED_OFFSET),
                (Gauge<Long>) committedOffset::get);

            // Total Processed JMX
            currentMetricRegistry.register(
                MetricsUtil.constructMetricName(
                    taskId,
                    MetricsUtil.TOTAL_PROCESSED_DOMAIN,
                    MetricsUtil.TOTAL_LOAD_COUNT),
                (Gauge<Long>) totalLoadCount::get);

            currentMetricRegistry.register(
                MetricsUtil.constructMetricName(
                    taskId,
                    MetricsUtil.TOTAL_PROCESSED_DOMAIN,
                    MetricsUtil.TOTAL_RECORD_COUNT),
                (Gauge<Long>) totalNumberOfRecord::get);

            currentMetricRegistry.register(
                MetricsUtil.constructMetricName(
                    taskId,
                    MetricsUtil.TOTAL_PROCESSED_DOMAIN,
                    MetricsUtil.TOTAL_DATA_SIZE),
                (Gauge<Long>) totalSizeOfData::get);

            // Buffer histogram JMX
            partitionBufferCountHistogram =
                currentMetricRegistry.histogram(
                    MetricsUtil.constructMetricName(
                        taskId,
                        MetricsUtil.BUFFER_DOMAIN,
                        MetricsUtil.BUFFER_RECORD_COUNT));
            partitionBufferSizeBytesHistogram =
                currentMetricRegistry.histogram(
                    MetricsUtil.constructMetricName(
                        taskId,
                        MetricsUtil.BUFFER_DOMAIN,
                        MetricsUtil.BUFFER_SIZE_BYTES));
            currentMetricRegistry.register(
                MetricsUtil.constructMetricName(
                    taskId, MetricsUtil.BUFFER_DOMAIN, MetricsUtil.BUFFER_MEMORY_USAGE),
                (Gauge<Long>) buffMemoryUsage::get);
        } catch (IllegalArgumentException ex) {
            LOG.warn("Metrics already present:{}", ex.getMessage());
        }
    }

    public void setCommittedOffset(long committedOffset) {
        this.committedOffset.set(committedOffset);
    }

    public void addAndGetLoadCount() {
        this.totalLoadCount.getAndIncrement();
    }

    public void addAndGetTotalNumberOfRecord(long totalNumberOfRecord) {
        this.totalNumberOfRecord.addAndGet(totalNumberOfRecord);
    }

    public void addAndGetTotalSizeOfData(long totalSizeOfData) {
        this.totalSizeOfData.addAndGet(totalSizeOfData);
    }

    public void addAndGetBuffMemoryUsage(long memoryUsage) {
        this.buffMemoryUsage.addAndGet(memoryUsage);
    }

    public void resetMemoryUsage() {
        this.buffMemoryUsage.set(0L);
    }

    public void updateBufferMetrics(long bufferSizeBytes, int numOfRecords) {
        partitionBufferSizeBytesHistogram.update(bufferSizeBytes);
        partitionBufferCountHistogram.update(numOfRecords);
    }
}
