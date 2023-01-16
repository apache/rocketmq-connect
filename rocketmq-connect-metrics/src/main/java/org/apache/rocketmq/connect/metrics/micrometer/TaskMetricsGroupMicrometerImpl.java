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
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.apache.rocketmq.connect.metrics.MetricsGroupTaskId;
import org.apache.rocketmq.connect.metrics.TaskMetricsGroup;

import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * task group micrometer implementation
 */
public class TaskMetricsGroupMicrometerImpl extends AbstractTaskMetricsGroup implements TaskMetricsGroup {
    private final Timer commitTime;
    private final DistributionSummary batchSize;
    private final Counter taskCommitFailures;
    private final Counter taskCommitSuccess;

    public TaskMetricsGroupMicrometerImpl(Map<String, String> config, MetricsGroupTaskId id, MeterRegistry meterRegistry) {
        super(config, id, meterRegistry);

        this.commitTime = createTimer("task.offset.commit");

        this.batchSize = createDistributionSummary("task.batch.size");

        this.taskCommitFailures = createCounter("task.offset.commit.failures");

        this.taskCommitSuccess = createCounter("task.offset.commit.successes");

    }

    @Override
    public void recordCommit(long duration, boolean success) {
        if (success) {
            this.commitTime.record(duration, TimeUnit.MILLISECONDS);
            this.taskCommitSuccess.increment();
        } else {
            this.taskCommitFailures.increment();
        }
    }

    @Override
    public void recordMultiple(int size) {
        this.batchSize.record(size);
    }

    @Override
    public void close() {
        remove(this.commitTime);
        remove(this.batchSize);
        remove(this.taskCommitFailures);
        remove(this.taskCommitSuccess);
        remove(this.taskCommitSuccess);
    }
}