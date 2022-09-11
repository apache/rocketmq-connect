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
import org.apache.rocketmq.common.message.MessageExt;

import java.util.Collection;
import java.util.Collections;
import java.util.Objects;

/**
 * contains all info current time
 */
class ProcessingContext implements AutoCloseable {

    /**
     * reporters
     */
    private Collection<ErrorReporter> reporters = Collections.emptyList();

    /**
     * send message
     */
    private MessageExt consumedMessage;

    /**
     * original source record
     */
    private ConnectRecord sourceRecord;

    /**
     * stage
     */
    private ErrorReporter.Stage stage;
    private Class<?> klass;

    /**
     * attempt
     */
    private int attempt;
    /**
     * error message
     */
    private Throwable error;

    /**
     * reset info
     */
    private void reset() {
        attempt = 0;
        stage = null;
        klass = null;
        error = null;
    }

    /**
     * @param consumedMessage the record
     */
    public void consumerRecord(MessageExt consumedMessage) {
        this.consumedMessage = consumedMessage;
        reset();
    }


    public MessageExt consumerRecord() {
        return consumedMessage;
    }

    /**
     * @return the source record being processed.
     */
    public ConnectRecord sourceRecord() {
        return sourceRecord;
    }

    /**
     * Set the source record being processed in the connect pipeline.
     *
     * @param record the source record
     */
    public void sourceRecord(ConnectRecord record) {
        this.sourceRecord = record;
        reset();
    }

    /**
     * Set the stage in the connector pipeline which is currently executing.
     *
     * @param stage the stage
     */
    public void stage(ErrorReporter.Stage stage) {
        this.stage = stage;
    }

    /**
     * @return the stage in the connector pipeline which is currently executing.
     */
    public ErrorReporter.Stage stage() {
        return stage;
    }

    /**
     * @return the class which is going to execute the current operation.
     */
    public Class<?> executingClass() {
        return klass;
    }

    /**
     * @param klass set the class which is currently executing.
     */
    public void executingClass(Class<?> klass) {
        this.klass = klass;
    }

    /**
     * A helper method to set both the stage and the class.
     *
     * @param stage the stage
     * @param klass the class which will execute the operation in this stage.
     */
    public void currentContext(ErrorReporter.Stage stage, Class<?> klass) {
        stage(stage);
        executingClass(klass);
    }


    /**
     * report errors
     */
    public void report() {
        if (reporters.size() == 1) {
            reporters.iterator().next().report(this);
        }
        reporters.stream().forEach(r -> r.report(this));
    }


    /**
     * @param attempt the number of attempts made to execute the current operation.
     */
    public void attempt(int attempt) {
        this.attempt = attempt;
    }

    public int attempt() {
        return attempt;
    }


    public Throwable error() {
        return error;
    }

    /**
     * set error
     *
     * @param error
     */
    public void error(Throwable error) {
        this.error = error;
    }


    /**
     * @return
     */
    public boolean failed() {
        return error() != null;
    }


    /**
     * set reporters
     *
     * @param reporters
     */
    public void reporters(Collection<ErrorReporter> reporters) {
        Objects.requireNonNull(reporters);
        this.reporters = reporters;
    }

    @Override
    public void close() {
        ConnectException e = null;
        for (ErrorReporter reporter : reporters) {
            try {
                reporter.close();
            } catch (Throwable t) {
                e = e != null ? e : new ConnectException("Failed to close all reporters");
                e.addSuppressed(t);
            }
        }
        if (e != null) {
            throw e;
        }
    }

    public String toString(boolean includeMessage) {
        StringBuilder builder = new StringBuilder();
        builder.append("Executing stage '");
        builder.append(stage().name());
        builder.append("' with class '");
        builder.append(executingClass() == null ? "null" : executingClass().getName());
        builder.append('\'');
        if (includeMessage && sourceRecord() != null) {
            builder.append(", where source record is = ");
            builder.append(sourceRecord());
        } else if (includeMessage && consumerRecord() != null) {
            MessageExt msg = consumerRecord();
            builder.append(", where consumed record is ");
            builder.append("{topic='").append(consumedMessage.getTopic()).append('\'');
            builder.append(", partition=").append(msg.getQueueId());
            builder.append(", offset=").append(msg.getQueueOffset());
            builder.append(", bornTimestamp=").append(msg.getBornTimestamp());
            builder.append(", storeTimestamp=").append(msg.getStoreTimestamp());
            builder.append("}");
        }
        builder.append('.');
        return builder.toString();
    }
}
