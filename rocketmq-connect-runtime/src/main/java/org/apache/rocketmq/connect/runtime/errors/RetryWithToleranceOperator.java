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

import io.openmessaging.connector.api.data.ConnectRecord;
import io.openmessaging.connector.api.errors.ConnectException;
import io.openmessaging.connector.api.errors.RetriableException;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.connect.runtime.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

/**
 * retry operator
 */
public class RetryWithToleranceOperator implements AutoCloseable {

    public static final long RETRIES_DELAY_MIN_MS = 300;
    private static final Logger log = LoggerFactory.getLogger(RetryWithToleranceOperator.class);
    private static final Map<ErrorReporter.Stage, Class<? extends Exception>> TOLERABLE_EXCEPTIONS = new HashMap<>();

    static {
        TOLERABLE_EXCEPTIONS.put(ErrorReporter.Stage.TRANSFORMATION, Exception.class);
        TOLERABLE_EXCEPTIONS.put(ErrorReporter.Stage.CONVERTER, Exception.class);
    }

    private final long retryTimeout;
    private final long maxDelayInMillis;
    private final ToleranceType toleranceType;
    protected ProcessingContext context = new ProcessingContext();
    private long totalFailures = 0;
    private ErrorMetricsGroup errorMetricsGroup;

    public RetryWithToleranceOperator(long errorRetryTimeout,
                                      long maxDelayInMillis,
                                      ToleranceType toleranceType,
                                      ErrorMetricsGroup errorMetricsGroup) {
        this.retryTimeout = errorRetryTimeout;
        this.maxDelayInMillis = maxDelayInMillis;
        this.toleranceType = toleranceType;
        this.errorMetricsGroup = errorMetricsGroup;
    }

    public void executeFailed(ErrorReporter.Stage stage,
                              Class<?> executingClass,
                              MessageExt consumerRecord,
                              Throwable error) {

        markAsFailed();
        context.consumerRecord(consumerRecord);
        context.currentContext(stage, executingClass);
        context.error(error);
        errorMetricsGroup.recordFailure();
        context.report();
        // failure
        if (!withinToleranceLimits()) {
            errorMetricsGroup.recordError();
            throw new ConnectException("Tolerance exceeded in error handler", error);
        }
    }

    public synchronized void executeFailed(ErrorReporter.Stage stage,
                                           Class<?> executingClass,
                                           ConnectRecord sourceRecord,
                                           Throwable error) {
        markAsFailed();
        context.sourceRecord(sourceRecord);
        context.currentContext(stage, executingClass);
        context.error(error);
        errorMetricsGroup.recordFailure();
        context.report();
        if (!withinToleranceLimits()) {
            errorMetricsGroup.recordError();
            throw new ConnectException("Tolerance exceeded in Source Worker error handler", error);
        }
    }


    /**
     * Execute the recoverable operation. If the operation is already in a failed state, then simply return
     * with the existing failure.
     */
    public <V> V execute(Operation<V> operation, ErrorReporter.Stage stage, Class<?> executingClass) {
        context.currentContext(stage, executingClass);
        if (context.failed()) {
            log.debug("ProcessingContext is already in failed state. Ignoring requested operation.");
            return null;
        }

        try {
            Class<? extends Exception> ex = TOLERABLE_EXCEPTIONS.getOrDefault(context.stage(), RetriableException.class);
            return execAndHandleError(operation, ex);
        } finally {
            if (context.failed()) {
                errorMetricsGroup.recordError();
                context.report();
            }
        }
    }

    /**
     * Attempt to execute an operation.
     */
    protected <V> V execAndRetry(Operation<V> operation) throws Exception {
        int attempt = 0;
        long startTime = System.currentTimeMillis();
        long deadline = startTime + retryTimeout;
        do {
            try {
                attempt++;
                return operation.call();
            } catch (RetriableException e) {
                log.trace("Caught a retriable exception while executing {} operation with {}", context.stage(), context.executingClass());
                errorMetricsGroup.recordFailure();
                if (checkRetry(startTime)) {
                    backoff(attempt, deadline);
                    if (Thread.currentThread().isInterrupted()) {
                        log.trace("Thread was interrupted. Marking operation as failed.");
                        context.error(e);
                        return null;
                    }
                    errorMetricsGroup.recordRetry();
                } else {
                    log.trace("Can't retry. start={}, attempt={}, deadline={}", startTime, attempt, deadline);
                    context.error(e);
                    return null;
                }
            } finally {
                context.attempt(attempt);
            }
        } while (true);
    }

    /**
     * Execute a given operation multiple times (if needed), and tolerate certain exceptions.
     */
    protected <V> V execAndHandleError(Operation<V> operation, Class<? extends Exception> tolerated) {
        try {
            V result = execAndRetry(operation);
            if (context.failed()) {
                markAsFailed();
                errorMetricsGroup.recordSkipped();
            }
            return result;
        } catch (Exception e) {
            errorMetricsGroup.recordFailure();
            markAsFailed();
            context.error(e);
            if (!tolerated.isAssignableFrom(e.getClass())) {
                throw new ConnectException("Unhandled exception in error handler", e);
            }

            if (!withinToleranceLimits()) {
                throw new ConnectException("Tolerance exceeded in error handler", e);
            }
            errorMetricsGroup.recordSkipped();
            return null;
        }
    }

    private void markAsFailed() {
        totalFailures++;
    }

    public boolean withinToleranceLimits() {
        switch (toleranceType) {
            case NONE:
                if (totalFailures > 0) {
                    return false;
                }
            case ALL:
                return true;
            default:
                throw new ConnectException("Unknown tolerance type: " + toleranceType);
        }
    }

    boolean checkRetry(long startTime) {
        return (System.currentTimeMillis() - startTime) < retryTimeout;
    }

    void backoff(int attempt, long deadline) {
        int numRetry = attempt - 1;
        long delay = RETRIES_DELAY_MIN_MS << numRetry;
        if (delay > maxDelayInMillis) {
            delay = ThreadLocalRandom.current().nextLong(maxDelayInMillis);
        }
        if (delay + System.currentTimeMillis() > deadline) {
            delay = deadline - System.currentTimeMillis();
        }
        log.debug("Sleeping for {} millis", delay);
        Utils.sleep(delay);
    }

    /**
     * Set the error reporters for this connector.
     *
     * @param reporters the error reporters (should not be null).
     */
    public void reporters(List<ErrorReporter> reporters) {
        this.context.reporters(reporters);
    }

    /**
     * Set the source record being processed in the connect pipeline.
     *
     * @param preTransformRecord the source record
     */
    public void sourceRecord(ConnectRecord preTransformRecord) {
        this.context.sourceRecord(preTransformRecord);
    }


    /**
     * Set the record consumed rocketmq in a sink
     *
     * @param messageExt
     */
    public void consumerRecord(MessageExt messageExt) {
        this.context.consumerRecord(messageExt);
    }

    /**
     * failed
     *
     * @return
     */
    public boolean failed() {
        return this.context.failed();
    }

    /**
     * error
     *
     * @return
     */
    public Throwable error() {
        return this.context.error();
    }

    public ToleranceType getErrorToleranceType() {
        return toleranceType;
    }

    @Override
    public void close() {
        try {
            this.errorMetricsGroup.close();
        } catch (Exception e) {
            log.error("Error metrics group close failure", e);
        }
        this.context.close();
    }


    @Override
    public String toString() {
        return "RetryWithToleranceOperator{" +
                "retryTimeout=" + retryTimeout +
                ", errorMaxDelayInMillis=" + maxDelayInMillis +
                ", errorToleranceType=" + toleranceType +
                ", totalFailures=" + totalFailures +
                ", context=" + context +
                '}';
    }

}
