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

import com.alibaba.fastjson.JSON;
import io.openmessaging.connector.api.component.task.sink.ErrorRecordReporter;
import io.openmessaging.connector.api.component.task.sink.SinkTask;
import io.openmessaging.connector.api.data.ConnectRecord;
import io.openmessaging.connector.api.data.RecordConverter;
import io.openmessaging.connector.api.data.RecordPartition;
import org.apache.rocketmq.common.message.MessageExt;

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
        Integer queueId = partition.getPartition().containsKey("queueId") ? (Integer) partition.getPartition().get("queueId") : null;
        Long queueOffset = partition.getPartition().containsKey("queueOffset") ? (Long) partition.getPartition().get("queueOffset") : null;
        String brokerName = partition.getPartition().containsKey("brokerName") ? String.valueOf(partition.getPartition().get("topic")) : null;

        MessageExt consumerRecord = new MessageExt();
        if (converter != null && converter instanceof RecordConverter) {
            byte[] value = converter.fromConnectData(topic, record.getSchema(), record.getData());
            consumerRecord.setBody(value);
            consumerRecord.setBrokerName(brokerName);
            consumerRecord.setQueueId(queueId);
            consumerRecord.setQueueOffset(queueOffset);
        } else {
            byte[] messageBody = JSON.toJSONString(record).getBytes();
            consumerRecord.setBody(messageBody);
        }
        // add extensions
        record.getExtensions().keySet().forEach(key -> {
            consumerRecord.putUserProperty(key, record.getExtensions().getString(key));
        });
        retryWithToleranceOperator.executeFailed(ErrorReporter.Stage.TASK_PUT, SinkTask.class, consumerRecord, error);
    }
}
