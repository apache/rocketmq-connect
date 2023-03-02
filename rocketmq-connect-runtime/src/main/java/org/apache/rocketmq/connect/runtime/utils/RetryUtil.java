/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.rocketmq.connect.runtime.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public final class RetryUtil {

    private static final Logger LOG = LoggerFactory.getLogger(RetryUtil.class);

    private static final long MAX_SLEEP_MILLISECOND = 256 * 1000;

    /**
     * execute with retry
     *
     * @param callable
     * @param retryTimes
     * @param sleepTimeInMilliSecond
     * @param exponential
     * @param <T>
     * @return
     * @throws Exception
     */
    public static <T> T executeWithRetry(Callable<T> callable,
                                         int retryTimes,
                                         long sleepTimeInMilliSecond,
                                         boolean exponential) throws Exception {
        Retry retry = new Retry();
        return retry.doRetry(callable, retryTimes, sleepTimeInMilliSecond, exponential, null);
    }


    /**
     * execute retry with exception
     *
     * @param callable
     * @param retryTimes
     * @param sleepTimeInMilliSecond
     * @param exponential
     * @param retryExceptionClasss
     * @param <T>
     * @return
     * @throws Exception
     */
    public static <T> T executeWithRetry(Callable<T> callable,
                                         int retryTimes,
                                         long sleepTimeInMilliSecond,
                                         boolean exponential,
                                         List<Class<?>> retryExceptionClasss) throws Exception {
        Retry retry = new Retry();
        return retry.doRetry(callable, retryTimes, sleepTimeInMilliSecond, exponential, retryExceptionClasss);
    }

    /**
     * Async execute with retry
     *
     * @param callable
     * @param retryTimes
     * @param sleepTimeInMilliSecond
     * @param exponential
     * @param timeoutMs
     * @param executor
     * @param <T>
     * @return
     * @throws Exception
     */
    public static <T> T asyncExecuteWithRetry(Callable<T> callable,
                                              int retryTimes,
                                              long sleepTimeInMilliSecond,
                                              boolean exponential,
                                              long timeoutMs,
                                              ThreadPoolExecutor executor) throws Exception {
        Retry retry = new AsyncRetry(timeoutMs, executor);
        return retry.doRetry(callable, retryTimes, sleepTimeInMilliSecond, exponential, null);
    }


    /**
     * Create thread pool
     *
     * @return
     */
    public static ThreadPoolExecutor createThreadPoolExecutor() {
        return new ThreadPoolExecutor(0, 5,
            60L, TimeUnit.SECONDS,
            new SynchronousQueue<Runnable>());
    }


    private static class Retry {

        public <T> T doRetry(Callable<T> callable, int retryTimes, long sleepTimeInMilliSecond, boolean exponential, List<Class<?>> retryExceptionClasss)
            throws Exception {

            if (null == callable) {
                throw new IllegalArgumentException("Callable cannot be empty ! ");
            }

            if (retryTimes < 1) {
                throw new IllegalArgumentException(String.format(
                    "Input parameter retry time [%d] cannot be less than 1 !", retryTimes));
            }

            Exception saveException = null;
            for (int i = 0; i < retryTimes; i++) {
                try {
                    return call(callable);
                } catch (Exception e) {
                    saveException = e;
                    if (null != retryExceptionClasss && !retryExceptionClasss.isEmpty()) {
                        boolean needRetry = false;
                        for (Class<?> eachExceptionClass : retryExceptionClasss) {
                            if (eachExceptionClass == e.getClass()) {
                                needRetry = true;
                                break;
                            }
                        }
                        if (!needRetry) {
                            throw saveException;
                        }
                    }

                    if (i + 1 < retryTimes && sleepTimeInMilliSecond > 0) {
                        long startTime = System.currentTimeMillis();

                        long timeToSleep;
                        if (exponential) {
                            timeToSleep = sleepTimeInMilliSecond * (long) Math.pow(2, i);
                            if (timeToSleep >= MAX_SLEEP_MILLISECOND) {
                                timeToSleep = MAX_SLEEP_MILLISECOND;
                            }
                        } else {
                            timeToSleep = sleepTimeInMilliSecond;
                            if (timeToSleep >= MAX_SLEEP_MILLISECOND) {
                                timeToSleep = MAX_SLEEP_MILLISECOND;
                            }
                        }

                        try {
                            Thread.sleep(timeToSleep);
                        } catch (InterruptedException ignored) {
                        }
                        long realTimeSleep = System.currentTimeMillis() - startTime;
                        LOG.error(String.format("Exception when calling callable, You are about to try to execute the %s retry. This retry is scheduled to wait for [%s] ms, and the actual wait is [%s] ms. Exception Msg: [%s]",
                            i + 1, timeToSleep, realTimeSleep, e.getMessage()));

                    }
                }
            }
            throw saveException;
        }

        protected <T> T call(Callable<T> callable) throws Exception {
            return callable.call();
        }
    }

    private static class AsyncRetry extends Retry {

        private final long timeoutMs;
        private final ThreadPoolExecutor executor;

        public AsyncRetry(long timeoutMs, ThreadPoolExecutor executor) {
            this.timeoutMs = timeoutMs;
            this.executor = executor;
        }

        @Override
        protected <T> T call(Callable<T> callable) throws Exception {
            Future<T> future = executor.submit(callable);
            try {
                return future.get(timeoutMs, TimeUnit.MILLISECONDS);
            } catch (Exception e) {
                LOG.warn("Try once failed", e);
                throw e;
            } finally {
                if (!future.isDone()) {
                    future.cancel(true);
                    LOG.warn("Try once task not done, cancel it, active count: " + executor.getActiveCount());
                }
            }
        }
    }

}
