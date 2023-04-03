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

package org.apache.rocketmq.connect.runtime.errors;

import com.alibaba.fastjson.JSON;
import io.openmessaging.connector.api.errors.ConnectException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.connect.runtime.common.ConnectKeyValue;
import org.apache.rocketmq.connect.runtime.config.WorkerConfig;
import org.apache.rocketmq.connect.runtime.utils.ConnectUtil;
import org.apache.rocketmq.connect.runtime.utils.ConnectorTaskId;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.Map;
import java.util.Objects;


/**
 * Write the original consumed record into a dead letter queue
 */
public class DeadLetterQueueReporter implements ErrorReporter {

    public static final String HEADER_PREFIX = "__connect.errors.";
    public static final String ERROR_HEADER_ORIG_TOPIC = HEADER_PREFIX + "topic";
    public static final String ERROR_HEADER_ORIG_PARTITION = HEADER_PREFIX + "partition";
    public static final String ERROR_HEADER_ORIG_OFFSET = HEADER_PREFIX + "offset";
    public static final String ERROR_HEADER_CONNECTOR_NAME = HEADER_PREFIX + "connector.name";
    public static final String ERROR_HEADER_TASK_ID = HEADER_PREFIX + "task.id";
    public static final String ERROR_HEADER_CLUSTER_ID = HEADER_PREFIX + "cluster.id";
    public static final String ERROR_HEADER_STAGE = HEADER_PREFIX + "stage";
    public static final String ERROR_HEADER_EXECUTING_CLASS = HEADER_PREFIX + "class.name";
    public static final String ERROR_HEADER_EXCEPTION = HEADER_PREFIX + "exception.class.name";
    public static final String ERROR_HEADER_EXCEPTION_MESSAGE = HEADER_PREFIX + "exception.message";
    public static final String ERROR_HEADER_EXCEPTION_STACK_TRACE = HEADER_PREFIX + "exception.stacktrace";
    private static final Logger log = LoggerFactory.getLogger(DeadLetterQueueReporter.class);
    /**
     * config
     */
    private final DeadLetterQueueConfig deadLetterQueueConfig;
    private final ErrorMetricsGroup errorMetricsGroup;
    /**
     * The configs of current source task.
     */
    private ConnectKeyValue config;
    /**
     * A RocketMQ producer to send message to dest MQ.
     */
    private DefaultMQProducer producer;
    /**
     * worker id
     */
    private String workerId;
    private ConnectorTaskId connectorTaskId;

    /**
     * Initialize the dead letter queue reporter with a producer
     *
     * @param producer
     * @param connConfig
     * @param connectorTaskId
     */
    DeadLetterQueueReporter(DefaultMQProducer producer,
                            ConnectKeyValue connConfig,
                            ConnectorTaskId connectorTaskId,
                            ErrorMetricsGroup errorMetricsGroup) {
        Objects.requireNonNull(producer);
        Objects.requireNonNull(connConfig);
        Objects.requireNonNull(connectorTaskId);
        this.producer = producer;
        this.config = connConfig;
        this.connectorTaskId = connectorTaskId;
        this.deadLetterQueueConfig = new DeadLetterQueueConfig(connConfig);
        this.errorMetricsGroup = errorMetricsGroup;
    }

    /**
     * build reporter
     *
     * @param connectorTaskId
     * @param sinkConfig
     * @param workerConfig
     * @return
     */
    public static DeadLetterQueueReporter build(ConnectorTaskId connectorTaskId,
                                                ConnectKeyValue sinkConfig,
                                                WorkerConfig workerConfig,
                                                ErrorMetricsGroup errorMetricsGroup) {

        DeadLetterQueueConfig deadLetterQueueConfig = new DeadLetterQueueConfig(sinkConfig);
        String dlqTopic = deadLetterQueueConfig.dlqTopicName();
        if (dlqTopic.isEmpty()) {
            return null;
        }
        if (!ConnectUtil.isTopicExist(workerConfig, dlqTopic)) {
            TopicConfig topicConfig = new TopicConfig(dlqTopic);
            topicConfig.setReadQueueNums(deadLetterQueueConfig.dlqTopicReadQueueNums());
            topicConfig.setWriteQueueNums(deadLetterQueueConfig.dlqTopicWriteQueueNums());
            ConnectUtil.createTopic(workerConfig, topicConfig);
        }
        DefaultMQProducer dlqProducer = ConnectUtil.initDefaultMQProducer(workerConfig);
        return new DeadLetterQueueReporter(dlqProducer, sinkConfig, connectorTaskId, errorMetricsGroup);
    }

    /**
     * Write the raw records into a topic
     */
    @Override
    public void report(ProcessingContext context) {
        if (this.deadLetterQueueConfig.dlqTopicName().trim().isEmpty()) {
            return;
        }

        errorMetricsGroup.recordDeadLetterQueueProduceRequest();
        MessageExt originalMessage = context.consumerRecord();
        if (originalMessage == null) {
            errorMetricsGroup.recordDeadLetterQueueProduceFailed();
            return;
        }

        Message producerRecord = new Message();
        producerRecord.setTopic(deadLetterQueueConfig.dlqTopicName());
        producerRecord.setBody(originalMessage.getBody());

        if (deadLetterQueueConfig.isDlqContextHeadersEnabled()) {
            populateContextHeaders(originalMessage, context);
        }

        try {
            producer.send(originalMessage, new SendCallback() {
                @Override
                public void onSuccess(SendResult result) {
                    log.info("Successful send error message to RocketMQ:{}, Topic {}", result.getMsgId(), result.getMessageQueue().getTopic());
                }

                @Override
                public void onException(Throwable throwable) {
                    errorMetricsGroup.recordDeadLetterQueueProduceFailed();
                    log.error("Source task send record failed ,error msg {}. message {}", throwable.getMessage(), JSON.toJSONString(originalMessage), throwable);
                }
            });
        } catch (MQClientException e) {
            log.error("Send message MQClientException. message: {}, error info: {}.", producerRecord, e);
        } catch (RemotingException e) {
            log.error("Send message RemotingException. message: {}, error info: {}.", producerRecord, e);
        } catch (InterruptedException e) {
            log.error("Send message InterruptedException. message: {}, error info: {}.", producerRecord, e);
            throw new ConnectException(e);
        }
    }


    /**
     * pop context property
     *
     * @param producerRecord
     * @param context
     */
    void populateContextHeaders(Message producerRecord, ProcessingContext context) {
        Map<String, String> headers = producerRecord.getProperties();
        if (context.consumerRecord() != null) {
            producerRecord.putUserProperty(ERROR_HEADER_ORIG_TOPIC, context.consumerRecord().getTopic());
            producerRecord.putUserProperty(ERROR_HEADER_ORIG_PARTITION, String.valueOf(context.consumerRecord().getQueueId()));
            producerRecord.putUserProperty(ERROR_HEADER_ORIG_OFFSET, String.valueOf(context.consumerRecord().getQueueOffset()));
        }
        if (workerId != null) {
            producerRecord.putUserProperty(ERROR_HEADER_CLUSTER_ID, workerId);
        }
        producerRecord.putUserProperty(ERROR_HEADER_STAGE, context.stage().name());
        producerRecord.putUserProperty(ERROR_HEADER_EXECUTING_CLASS, context.executingClass().getName());
        producerRecord.putUserProperty(ERROR_HEADER_CONNECTOR_NAME, connectorTaskId.connector());
        producerRecord.putUserProperty(ERROR_HEADER_TASK_ID, connectorTaskId.task() + "");
        if (context.error() != null) {
            Throwable error = context.error();
            headers.put(ERROR_HEADER_EXCEPTION, error.getClass().getName());
            headers.put(ERROR_HEADER_EXCEPTION_MESSAGE, error.getMessage());
            byte[] trace;
            if ((trace = stacktrace(context.error())) != null) {
                headers.put(ERROR_HEADER_EXCEPTION_STACK_TRACE, new String(trace));
            }
        }
    }

    private byte[] stacktrace(Throwable error) {
        if (error == null) {
            return null;
        }
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        try {
            PrintStream stream = new PrintStream(bos, true, "UTF-8");
            error.printStackTrace(stream);
            bos.close();
            return bos.toByteArray();
        } catch (IOException e) {
            log.error("Could not serialize stacktrace.", e);
        }
        return null;
    }

    @Override
    public void close() {
        if (producer != null) {
            producer.shutdown();
        }
        try {
            this.errorMetricsGroup.close();
        } catch (Exception e) {
            log.error("Error metrics group close failure", e);
        }
    }
}
