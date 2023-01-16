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
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.rocketmq.connect.metrics.ErrorMetricsGroup;
import org.apache.rocketmq.connect.metrics.MetricsGroupTaskId;

import java.util.Map;

/**
 * error group micrometer implementation
 */
public class ErrorMetricsGroupMicrometerImpl extends AbstractTaskMetricsGroup implements ErrorMetricsGroup {
    // metrics
    private final Counter recordProcessingFailures;
    private final Counter recordProcessingErrors;
    private final Counter recordsSkipped;
    private final Counter retries;
    private final Counter errorsLogged;
    private final Counter dlqProduceRequests;
    private final Counter dlqProduceFailures;

    public ErrorMetricsGroupMicrometerImpl(Map<String, String> config, MetricsGroupTaskId id, MeterRegistry meterRegistry) {
        super(config, id, meterRegistry);

        this.recordProcessingFailures = createCounter("error.record.failures");

        this.recordProcessingErrors = createCounter("error.record.errors");

        this.recordsSkipped = createCounter("error.record.skipped");

        this.retries = createCounter("error.record.retries");

        this.errorsLogged = createCounter("error.logged");

        this.dlqProduceFailures = createCounter("error.dlq.produce.failures");

        this.dlqProduceRequests = createCounter("error.dlq.produce.requests");
    }

    @Override
    public void recordFailure() {
        this.recordProcessingFailures.increment();
    }

    @Override
    public void recordError() {
        this.recordProcessingErrors.increment();
    }

    @Override
    public void recordSkipped() {
        this.recordsSkipped.increment();
    }

    @Override
    public void recordRetry() {
        this.retries.increment();
    }

    @Override
    public void recordErrorLogged() {
        this.errorsLogged.increment();
    }

    @Override
    public void recordDeadLetterQueueProduceRequest() {
        this.dlqProduceRequests.increment();
    }

    @Override
    public void recordDeadLetterQueueProduceFailed() {
        this.dlqProduceFailures.increment();
    }

    @Override
    public void close() {
        remove(this.recordProcessingFailures);
        remove(this.recordProcessingErrors);
        remove(this.recordsSkipped);
        remove(this.retries);
        remove(this.errorsLogged);
        remove(this.dlqProduceRequests);
        remove(this.dlqProduceFailures);
    }
}
