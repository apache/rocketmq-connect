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

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.apache.rocketmq.connect.metrics.MetricsGroupTaskId;
import org.apache.rocketmq.connect.metrics.SinkTaskMetricsGroup;

import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * sink task group micrometer implementation
 */
public class SinkTaskMetricsGroupMicrometerImpl extends AbstractTaskMetricsGroup implements SinkTaskMetricsGroup {
    private final DistributionSummary sinkRecordRead;
    private final DistributionSummary sinkRecordSend;
    private final Counter offsetCommitSuccess;
    private final Counter offsetCompletionSkipped;
    private final Timer putBatchTime;
    private Gauge taskState;

    public SinkTaskMetricsGroupMicrometerImpl(Map<String, String> config, MetricsGroupTaskId id, MeterRegistry meterRegistry) {
        super(config, id, meterRegistry);

        this.sinkRecordRead = createDistributionSummary("sink.record.read");

        this.sinkRecordSend = createDistributionSummary("sink.record.send");

        this.offsetCommitSuccess = createCounter("sink.offset.commit.successes");

        this.offsetCompletionSkipped = createCounter("sink.offset.commit.skipped");

        this.putBatchTime = createTimer("sink.put.batch");
    }

    @Override
    public void recordRead(int batchSize) {
        this.sinkRecordRead.record(batchSize);
    }

    @Override
    public void recordSend(int batchSize) {
        this.sinkRecordSend.record(batchSize);
    }

    @Override
    public void recordPut(long duration) {
        this.putBatchTime.record(duration, TimeUnit.MILLISECONDS);
    }

    @Override
    public void recordOffsetCommitSuccess() {
        this.offsetCommitSuccess.increment();
    }

    @Override
    public void recordOffsetCommitSkipped() {
        this.offsetCompletionSkipped.increment();
    }

    @Override
    public void registerTaskStateMetrics(Supplier<Number> supplier, String... tags) {
        if (taskState == null) {
            this.taskState = createGauge("sink.task.state", supplier, tags);
        }
    }

    @Override
    public void close() {
        remove(this.sinkRecordRead);
        remove(this.sinkRecordSend);
        remove(this.putBatchTime);
        remove(this.offsetCommitSuccess);
        remove(this.offsetCompletionSkipped);
        remove(this.taskState);
    }
}