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

package org.apache.rocketmq.connect.doris.utils;

import org.apache.rocketmq.connect.doris.exception.DorisException;
import org.apache.rocketmq.connect.doris.model.LoadOperation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BackoffAndRetryUtils {

    private static final Logger LOG = LoggerFactory.getLogger(BackoffAndRetryUtils.class);

    // backoff with 1, 2, 4 seconds
    private static final int[] backoffSec = {0, 1, 2, 4};

    /**
     * Interfaces to define the lambda function to be used by backoffAndRetry
     */
    public interface backoffFunction {
        Object apply() throws Exception;
    }

    /**
     * Backoff logic
     *
     * @param operation Load Operation Type which corresponds to the lambda function runnable
     * @param runnable  the lambda function itself
     * @return the object that the function returns
     * @throws Exception if the runnable function throws exception
     */
    public static Object backoffAndRetry(
        final LoadOperation operation, final backoffFunction runnable) throws Exception {
        for (final int iteration : backoffSec) {
            if (iteration != 0) {
                Thread.sleep(iteration * 1000L);
                LOG.debug("Retry Count:{} for operation:{}", iteration, operation);
            }
            try {
                return runnable.apply();
            } catch (Exception e) {
                LOG.error(
                    "Retry count:{} caught an exception for operation:{} with message:{}",
                    iteration,
                    operation,
                    e.getMessage());
            }
        }
        String errMsg = "Api retry exceeded the max retry limit, operation = " + operation;
        LOG.error(errMsg);
        throw new DorisException(errMsg);
    }
}
