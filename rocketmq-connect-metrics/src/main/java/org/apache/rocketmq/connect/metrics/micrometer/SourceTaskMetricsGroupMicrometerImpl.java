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
package org.apache.rocketmq.connect.metrics.micrometer;

import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.apache.rocketmq.connect.metrics.MetricsGroupTaskId;
import org.apache.rocketmq.connect.metrics.SourceTaskMetricsGroup;

import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

/**
 * source task group micrometer implementation
 */
public class SourceTaskMetricsGroupMicrometerImpl extends AbstractTaskMetricsGroup implements SourceTaskMetricsGroup {
    private final DistributionSummary sourceRecordPoll;
    private final DistributionSummary sourceRecordWrite;
    private final DistributionSummary sourceRecordActiveCount;
    private final Timer pollTime;
    private Gauge taskState;
    private AtomicInteger activeRecordCount = new AtomicInteger(0);

    public SourceTaskMetricsGroupMicrometerImpl(Map<String, String> config, MetricsGroupTaskId id, MeterRegistry meterRegistry) {
        super(config, id, meterRegistry);

        this.sourceRecordPoll = createDistributionSummary("source.record.poll");

        this.sourceRecordWrite = createDistributionSummary("source.record.write");

        this.sourceRecordActiveCount = createDistributionSummary("source.record.active");

        this.pollTime = createTimer("source.poll");
    }

    @Override
    public void recordPoll(int batchSize, long duration) {
        this.sourceRecordPoll.record(batchSize);
        this.pollTime.record(duration, TimeUnit.MILLISECONDS);
        this.sourceRecordActiveCount.record(this.activeRecordCount.addAndGet(batchSize));
    }

    @Override
    public void recordWrite(int recordCount) {
        this.sourceRecordWrite.record(recordCount);
        this.sourceRecordActiveCount.record(this.activeRecordCount.updateAndGet(count -> count > recordCount ? count - recordCount : 0));
    }

    @Override
    public void registerTaskStateMetrics(Supplier<Number> supplier, String... tags) {
        if (taskState == null) {
            this.taskState = createGauge("source.task.state", supplier, tags);
        }
    }

    @Override
    public void close() {
        remove(this.sourceRecordPoll);
        remove(this.sourceRecordWrite);
        remove(this.sourceRecordActiveCount);
        remove(this.pollTime);
        remove(this.taskState);
    }
}