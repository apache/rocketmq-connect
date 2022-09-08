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

import io.openmessaging.connector.api.data.RecordConverter;
import org.apache.rocketmq.connect.runtime.common.ConnectKeyValue;
import org.apache.rocketmq.connect.runtime.config.WorkerConfig;
import org.apache.rocketmq.connect.runtime.utils.ConnectorTaskId;

import java.util.ArrayList;
import java.util.List;

/**
 * reporter manage util
 */
public class ReporterManagerUtil {

    /**
     * create retry operator
     *
     * @param connConfig
     * @return
     */
    public static RetryWithToleranceOperator createRetryWithToleranceOperator(
            ConnectKeyValue connConfig,
            ErrorMetricsGroup errorMetricsGroup
    ) {
        DeadLetterQueueConfig deadLetterQueueConfig = new DeadLetterQueueConfig(connConfig);
        return new RetryWithToleranceOperator(
                deadLetterQueueConfig.errorRetryTimeout(),
                deadLetterQueueConfig.errorMaxDelayInMillis(),
                deadLetterQueueConfig.errorToleranceType(),
                errorMetricsGroup
        );
    }

    /**
     * create worker error record reporter
     *
     * @param connConfig
     * @param retryWithToleranceOperator
     * @param converter
     * @return
     */
    public static WorkerErrorRecordReporter createWorkerErrorRecordReporter(
            ConnectKeyValue connConfig,
            RetryWithToleranceOperator retryWithToleranceOperator,
            RecordConverter converter) {
        DeadLetterQueueConfig deadLetterQueueConfig = new DeadLetterQueueConfig(connConfig);
        if (deadLetterQueueConfig.enableErrantRecordReporter()) {
            return new WorkerErrorRecordReporter(retryWithToleranceOperator, converter);
        }
        return null;
    }

    /**
     * build sink task reporter
     *
     * @param connectorTaskId
     * @param connConfig
     * @param workerConfig
     * @return
     */
    public static List<ErrorReporter> sinkTaskReporters(ConnectorTaskId connectorTaskId,
                                                        ConnectKeyValue connConfig,
                                                        WorkerConfig workerConfig,
                                                        ErrorMetricsGroup errorMetricsGroup) {
        // ensure reporter order
        ArrayList<ErrorReporter> reporters = new ArrayList<>();
        LogReporter logReporter = new LogReporter(connectorTaskId, connConfig, errorMetricsGroup);
        reporters.add(logReporter);

        // dead letter queue reporter
        DeadLetterQueueReporter reporter = DeadLetterQueueReporter.build(connectorTaskId, connConfig, workerConfig, errorMetricsGroup);
        if (reporter != null) {
            reporters.add(reporter);
        }
        return reporters;
    }

    /**
     * build source task reporter
     *
     * @param connectorTaskId
     * @param connConfig
     * @return
     */
    public static List<ErrorReporter> sourceTaskReporters(ConnectorTaskId connectorTaskId,
                                                          ConnectKeyValue connConfig,
                                                          ErrorMetricsGroup errorMetricsGroup) {
        List<ErrorReporter> reporters = new ArrayList<>();
        LogReporter logReporter = new LogReporter(connectorTaskId, connConfig, errorMetricsGroup);
        reporters.add(logReporter);
        return reporters;
    }
}
