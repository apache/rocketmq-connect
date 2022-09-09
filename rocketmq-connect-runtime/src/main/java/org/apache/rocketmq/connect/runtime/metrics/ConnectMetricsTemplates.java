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
package org.apache.rocketmq.connect.runtime.metrics;


import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;


public class ConnectMetricsTemplates {

    public static final String CONNECTOR_TAG_NAME = "connector";
    public static final String TASK_TAG_NAME = "task";
    public static final String CONNECTOR_GROUP_NAME = "connector-metrics";
    public static final String TASK_GROUP_NAME = "connector-task-metrics";
    public static final String SOURCE_TASK_GROUP_NAME = "source-task-metrics";
    public static final String SINK_TASK_GROUP_NAME = "sink-task-metrics";
    public static final String WORKER_GROUP_NAME = "connect-worker-metrics";

    public static final String TASK_ERROR_HANDLING_GROUP_NAME = "task-error-metrics";

    private final List<MetricNameTemplate> allTemplates = new ArrayList<>();

    public final MetricNameTemplate taskCommitTimeMax;
    public final MetricNameTemplate taskCommitTimeAvg;
    public final MetricNameTemplate taskBatchSizeMax;
    public final MetricNameTemplate taskBatchSizeAvg;
    public final MetricNameTemplate taskCommitFailureCount;
    public final MetricNameTemplate taskCommitSuccessCount;
    public final MetricNameTemplate sourceRecordPollRate;
    public final MetricNameTemplate sourceRecordPollTotal;
    public final MetricNameTemplate sourceRecordWriteRate;
    public final MetricNameTemplate sourceRecordWriteTotal;
    public final MetricNameTemplate sourceRecordPollBatchTimeMax;
    public final MetricNameTemplate sourceRecordPollBatchTimeAvg;
    public final MetricNameTemplate sourceRecordActiveCount;
    public final MetricNameTemplate sourceRecordActiveCountMax;
    public final MetricNameTemplate sourceRecordActiveCountAvg;
    public final MetricNameTemplate sinkRecordReadRate;
    public final MetricNameTemplate sinkRecordReadTotal;
    public final MetricNameTemplate sinkRecordSendRate;
    public final MetricNameTemplate sinkRecordSendTotal;
    public final MetricNameTemplate sinkRecordLagMax;
    public final MetricNameTemplate sinkRecordPartitionCount;
    public final MetricNameTemplate sinkRecordOffsetCommitSeqNum;
    public final MetricNameTemplate sinkRecordOffsetCommitCompletionRate;
    public final MetricNameTemplate sinkRecordOffsetCommitCompletionTotal;
    public final MetricNameTemplate sinkRecordOffsetCommitSkipRate;
    public final MetricNameTemplate sinkRecordOffsetCommitSkipTotal;
    public final MetricNameTemplate sinkRecordPutBatchTimeMax;
    public final MetricNameTemplate sinkRecordPutBatchTimeAvg;
    public final MetricNameTemplate sinkRecordActiveCount;
    public final MetricNameTemplate sinkRecordActiveCountMax;
    public final MetricNameTemplate sinkRecordActiveCountAvg;

    public final MetricNameTemplate recordProcessingFailures;
    public final MetricNameTemplate recordProcessingErrors;
    public final MetricNameTemplate recordsSkipped;
    public final MetricNameTemplate retries;
    public final MetricNameTemplate errorsLogged;
    public final MetricNameTemplate dlqProduceRequests;
    public final MetricNameTemplate dlqProduceFailures;
    public final MetricNameTemplate lastErrorTimestamp;

    public ConnectMetricsTemplates() {
        this(new LinkedHashSet<>());
    }

    public ConnectMetricsTemplates(Set<String> tags) {

        /***** Worker task level *****/
        Set<String> workerTaskTags = new LinkedHashSet<>(tags);
        workerTaskTags.add(CONNECTOR_TAG_NAME);
        workerTaskTags.add(TASK_TAG_NAME);

        taskCommitTimeMax = createTemplate("offset-commit-max-time-ms", TASK_GROUP_NAME,
                                           "The maximum time in milliseconds taken by this task to commit offsets.", workerTaskTags);
        taskCommitTimeAvg = createTemplate("offset-commit-avg-time-ms", TASK_GROUP_NAME,
                                           "The average time in milliseconds taken by this task to commit offsets.", workerTaskTags);
        taskBatchSizeMax = createTemplate("batch-size-max", TASK_GROUP_NAME, "The maximum size of the batches processed by the connector.",
                                          workerTaskTags);
        taskBatchSizeAvg = createTemplate("batch-size-avg", TASK_GROUP_NAME, "The average size of the batches processed by the connector.",
                                          workerTaskTags);
        taskCommitFailureCount = createTemplate("offset-commit-failure-count", TASK_GROUP_NAME,
                                                     "This task's offset commit attempts that failed.",
                                                     workerTaskTags);
        taskCommitSuccessCount = createTemplate("offset-commit-success-count", TASK_GROUP_NAME,
                                                     "This task's offset commit attempts that succeeded.",
                                                     workerTaskTags);

        /***** Source worker task level *****/
        Set<String> sourceTaskTags = new LinkedHashSet<>(tags);
        sourceTaskTags.add(CONNECTOR_TAG_NAME);
        sourceTaskTags.add(TASK_TAG_NAME);

        sourceRecordPollRate = createTemplate("source-record-poll-rate", SOURCE_TASK_GROUP_NAME,
                                              "The average per-second number of records produced/polled (before transformation) by " +
                                              "this task belonging to the named source connector in this worker.",
                                              sourceTaskTags);
        sourceRecordPollTotal = createTemplate("source-record-poll-total", SOURCE_TASK_GROUP_NAME,
                                               "The total number of records produced/polled (before transformation) by this task " +
                                               "belonging to the named source connector in this worker.",
                                               sourceTaskTags);
        sourceRecordWriteRate = createTemplate("source-record-write-rate", SOURCE_TASK_GROUP_NAME,
                                               "The average per-second number of records output from the transformations and written" +
                                               " to Kafka for this task belonging to the named source connector in this worker. This" +
                                               " is after transformations are applied and excludes any records filtered out by the " +
                                               "transformations.",
                                               sourceTaskTags);
        sourceRecordWriteTotal = createTemplate("source-record-write-total", SOURCE_TASK_GROUP_NAME,
                                                "The number of records output from the transformations and written to Kafka for this" +
                                                " task belonging to the named source connector in this worker, since the task was " +
                                                "last restarted.",
                                                sourceTaskTags);
        sourceRecordPollBatchTimeMax = createTemplate("poll-batch-max-time-ms", SOURCE_TASK_GROUP_NAME,
                                                      "The maximum time in milliseconds taken by this task to poll for a batch of " +
                                                      "source records.",
                                                      sourceTaskTags);
        sourceRecordPollBatchTimeAvg = createTemplate("poll-batch-avg-time-ms", SOURCE_TASK_GROUP_NAME,
                                                      "The average time in milliseconds taken by this task to poll for a batch of " +
                                                      "source records.",
                                                      sourceTaskTags);
        sourceRecordActiveCount = createTemplate("source-record-active-count", SOURCE_TASK_GROUP_NAME,
                                                 "The number of records that have been produced by this task but not yet completely " +
                                                 "written to Kafka.",
                                                 sourceTaskTags);
        sourceRecordActiveCountMax = createTemplate("source-record-active-count-max", SOURCE_TASK_GROUP_NAME,
                                                    "The maximum number of records that have been produced by this task but not yet " +
                                                    "completely written to Kafka.",
                                                    sourceTaskTags);
        sourceRecordActiveCountAvg = createTemplate("source-record-active-count-avg", SOURCE_TASK_GROUP_NAME,
                                                    "The average number of records that have been produced by this task but not yet " +
                                                    "completely written to Kafka.",
                                                    sourceTaskTags);

        /***** Sink worker task level *****/
        Set<String> sinkTaskTags = new LinkedHashSet<>(tags);
        sinkTaskTags.add(CONNECTOR_TAG_NAME);
        sinkTaskTags.add(TASK_TAG_NAME);

        sinkRecordReadRate = createTemplate("sink-record-read-rate", SINK_TASK_GROUP_NAME,
                                            "The average per-second number of records read from Kafka for this task belonging to the" +
                                            " named sink connector in this worker. This is before transformations are applied.",
                                            sinkTaskTags);
        sinkRecordReadTotal = createTemplate("sink-record-read-total", SINK_TASK_GROUP_NAME,
                                             "The total number of records read from Kafka by this task belonging to the named sink " +
                                             "connector in this worker, since the task was last restarted.",
                                             sinkTaskTags);
        sinkRecordSendRate = createTemplate("sink-record-send-rate", SINK_TASK_GROUP_NAME,
                                            "The average per-second number of records output from the transformations and sent/put " +
                                            "to this task belonging to the named sink connector in this worker. This is after " +
                                            "transformations are applied and excludes any records filtered out by the " +
                                            "transformations.",
                                            sinkTaskTags);
        sinkRecordSendTotal = createTemplate("sink-record-send-total", SINK_TASK_GROUP_NAME,
                                             "The total number of records output from the transformations and sent/put to this task " +
                                             "belonging to the named sink connector in this worker, since the task was last " +
                                             "restarted.",
                                             sinkTaskTags);
        sinkRecordLagMax = createTemplate("sink-record-lag-max", SINK_TASK_GROUP_NAME,
                                          "The maximum lag in terms of number of records that the sink task is behind the consumer's " +
                                          "position for any topic partitions.",
                                          sinkTaskTags);
        sinkRecordPartitionCount = createTemplate("partition-count", SINK_TASK_GROUP_NAME,
                                                  "The number of topic partitions assigned to this task belonging to the named sink " +
                                                  "connector in this worker.",
                                                  sinkTaskTags);
        sinkRecordOffsetCommitSeqNum = createTemplate("offset-commit-seq-no", SINK_TASK_GROUP_NAME,
                                                      "The current sequence number for offset commits.", sinkTaskTags);
        sinkRecordOffsetCommitCompletionRate = createTemplate("offset-commit-completion-rate", SINK_TASK_GROUP_NAME,
                                                              "The average per-second number of offset commit completions that were " +
                                                              "completed successfully.",
                                                              sinkTaskTags);
        sinkRecordOffsetCommitCompletionTotal = createTemplate("offset-commit-completion-total", SINK_TASK_GROUP_NAME,
                                                               "The total number of offset commit completions that were completed " +
                                                               "successfully.",
                                                               sinkTaskTags);
        sinkRecordOffsetCommitSkipRate = createTemplate("offset-commit-skip-rate", SINK_TASK_GROUP_NAME,
                                                        "The average per-second number of offset commit completions that were " +
                                                        "received too late and skipped/ignored.",
                                                        sinkTaskTags);
        sinkRecordOffsetCommitSkipTotal = createTemplate("offset-commit-skip-total", SINK_TASK_GROUP_NAME,
                                                         "The total number of offset commit completions that were received too late " +
                                                         "and skipped/ignored.",
                                                         sinkTaskTags);
        sinkRecordPutBatchTimeMax = createTemplate("put-batch-max-time-ms", SINK_TASK_GROUP_NAME,
                                                   "The maximum time taken by this task to put a batch of sinks records.", sinkTaskTags);
        sinkRecordPutBatchTimeAvg = createTemplate("put-batch-avg-time-ms", SINK_TASK_GROUP_NAME,
                                                   "The average time taken by this task to put a batch of sinks records.", sinkTaskTags);
        sinkRecordActiveCount = createTemplate("sink-record-active-count", SINK_TASK_GROUP_NAME,
                                               "The number of records that have been read from Kafka but not yet completely " +
                                               "committed/flushed/acknowledged by the sink task.",
                                               sinkTaskTags);
        sinkRecordActiveCountMax = createTemplate("sink-record-active-count-max", SINK_TASK_GROUP_NAME,
                                                  "The maximum number of records that have been read from Kafka but not yet completely "
                                                  + "committed/flushed/acknowledged by the sink task.",
                                                  sinkTaskTags);
        sinkRecordActiveCountAvg = createTemplate("sink-record-active-count-avg", SINK_TASK_GROUP_NAME,
                                                  "The average number of records that have been read from Kafka but not yet completely "
                                                  + "committed/flushed/acknowledged by the sink task.",
                                                  sinkTaskTags);

        /***** task error metrics *****/
        Set<String> taskErrorHandlingTags = new LinkedHashSet<>(tags);
        taskErrorHandlingTags.add(CONNECTOR_TAG_NAME);
        taskErrorHandlingTags.add(TASK_TAG_NAME);

        recordProcessingFailures = createTemplate("total-record-failures", TASK_ERROR_HANDLING_GROUP_NAME,
                "The number of record processing failures in this task.", taskErrorHandlingTags);
        recordProcessingErrors = createTemplate("total-record-errors", TASK_ERROR_HANDLING_GROUP_NAME,
                "The number of record processing errors in this task. ", taskErrorHandlingTags);
        recordsSkipped = createTemplate("total-records-skipped", TASK_ERROR_HANDLING_GROUP_NAME,
                "The number of records skipped due to errors.", taskErrorHandlingTags);
        retries = createTemplate("total-retries", TASK_ERROR_HANDLING_GROUP_NAME,
                "The number of operations retried.", taskErrorHandlingTags);
        errorsLogged = createTemplate("total-errors-logged", TASK_ERROR_HANDLING_GROUP_NAME,
                "The number of errors that were logged.", taskErrorHandlingTags);
        dlqProduceRequests = createTemplate("deadletterqueue-produce-requests", TASK_ERROR_HANDLING_GROUP_NAME,
                "The number of attempted writes to the dead letter queue.", taskErrorHandlingTags);
        dlqProduceFailures = createTemplate("deadletterqueue-produce-failures", TASK_ERROR_HANDLING_GROUP_NAME,
                "The number of failed writes to the dead letter queue.", taskErrorHandlingTags);
        lastErrorTimestamp = createTemplate("last-error-timestamp", TASK_ERROR_HANDLING_GROUP_NAME,
                "The epoch timestamp when this task last encountered an error.", taskErrorHandlingTags);
    }

    private MetricNameTemplate createTemplate(String name, String group, String doc, Set<String> tags) {
        MetricNameTemplate template = new MetricNameTemplate(name, group, doc, tags);
        allTemplates.add(template);
        return template;
    }

    public List<MetricNameTemplate> getAllTemplates() {
        return Collections.unmodifiableList(allTemplates);
    }

    public String connectorTagName() {
        return CONNECTOR_TAG_NAME;
    }

    public String taskTagName() {
        return TASK_TAG_NAME;
    }

    public String connectorGroupName() {
        return CONNECTOR_GROUP_NAME;
    }

    public String taskGroupName() {
        return TASK_GROUP_NAME;
    }

    public String sinkTaskGroupName() {
        return SINK_TASK_GROUP_NAME;
    }

    public String sourceTaskGroupName() {
        return SOURCE_TASK_GROUP_NAME;
    }

    public String workerGroupName() {
        return WORKER_GROUP_NAME;
    }

    public String taskErrorHandlingGroupName() {
        return TASK_ERROR_HANDLING_GROUP_NAME;
    }
}
