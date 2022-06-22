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

import io.openmessaging.connector.api.component.task.sink.ErrorRecordReporter;
import io.openmessaging.connector.api.component.task.sink.SinkTask;
import io.openmessaging.connector.api.data.ConnectRecord;
import io.openmessaging.connector.api.data.Converter;
import io.openmessaging.connector.api.data.RecordConverter;
import io.openmessaging.connector.api.data.RecordPartition;
import org.apache.rocketmq.common.message.MessageExt;

import java.util.HashMap;
import java.util.Map;

/**
 * worker error record reporter
 */
public class WorkerErrorRecordReporter implements ErrorRecordReporter {

    private RetryWithToleranceOperator retryWithToleranceOperator;
    private RecordConverter converter;

    public WorkerErrorRecordReporter(RetryWithToleranceOperator retryWithToleranceOperator,
                                     RecordConverter converter) {
        this.retryWithToleranceOperator = retryWithToleranceOperator;
        this.converter = converter;
    }

    /**
     * report record
     *
     * @param record
     * @param error
     * @return
     */
    @Override
    public void report(ConnectRecord record, Throwable error) {

        RecordPartition partition = record.getPosition().getPartition();
        String topic = partition.getPartition().containsKey("topic") ? String.valueOf(partition.getPartition().get("topic")) : null;
        byte[] value = converter.fromConnectData(topic , record.getSchema(), record.getData());
        MessageExt consumerRecord = new MessageExt();
        consumerRecord.setBody(value);
        retryWithToleranceOperator.executeFailed(ErrorReporter.Stage.TASK_PUT, SinkTask.class, consumerRecord, error);
    }
}
