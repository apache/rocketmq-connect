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
package org.apache.rocketmq.connect.kafka.connect.adaptor.task;

import io.openmessaging.KeyValue;
import io.openmessaging.connector.api.component.task.sink.SinkTask;
import io.openmessaging.connector.api.data.ConnectRecord;
import io.openmessaging.connector.api.errors.ConnectException;
import org.apache.kafka.connect.runtime.TaskConfig;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.apache.rocketmq.connect.kafka.connect.adaptor.config.ConnectKeyValue;
import org.apache.rocketmq.connect.kafka.connect.adaptor.context.RocketMQKafkaSinkTaskContext;
import org.apache.rocketmq.connect.kafka.connect.adaptor.transforms.TransformationWrapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * abstract kafka connect sink
 */
public abstract class AbstractKafkaConnectSink extends SinkTask implements TaskClassSetter {

    private static final Logger log = LoggerFactory.getLogger(AbstractKafkaConnectSink.class);
    protected TransformationWrapper transformationWrapper;
    /**
     * kafka connect init
     */
    protected ConnectKeyValue configValue;
    private SinkTaskContext kafkaSinkTaskContext;
    private org.apache.kafka.connect.sink.SinkTask sinkTask;

    /**
     * convert by kafka sink transform
     *
     * @param record
     */
    protected abstract SinkRecord transforms(SinkRecord record);

    /**
     * convert ConnectRecord to SinkRecord
     *
     * @param record
     * @return
     */
    public abstract SinkRecord processSinkRecord(ConnectRecord record);


    /**
     * Put the records to the sink
     *
     * @param records sink records
     */
    @Override
    public void put(List<ConnectRecord> records) {
        // convert sink data
        List<SinkRecord> sinkRecords = new ArrayList<>();
        records.forEach(connectRecord -> {
            SinkRecord record = this.processSinkRecord(connectRecord);
            sinkRecords.add(this.transforms(record));
        });
        sinkTask.put(sinkRecords);
    }


    @Override
    public void validate(KeyValue keyValue) {
    }

    @Override
    public void start(KeyValue keyValue) {
        this.configValue = new ConnectKeyValue();
        keyValue.keySet().forEach(key -> {
            this.configValue.put(key, keyValue.getString(key));
        });

        setTaskClass(configValue);

        Map<String, String> taskConfig = new HashMap<>(configValue.config());
        // get the source class name from config and create source task from reflection
        try {
            sinkTask = Class.forName(taskConfig.get(TaskConfig.TASK_CLASS_CONFIG))
                    .asSubclass(org.apache.kafka.connect.sink.SinkTask.class)
                    .getDeclaredConstructor()
                    .newInstance();
        } catch (Exception e) {
            throw new ConnectException("Load task class failed, " + taskConfig.get(TaskConfig.TASK_CLASS_CONFIG));
        }

        kafkaSinkTaskContext = new RocketMQKafkaSinkTaskContext(sinkTaskContext, taskConfig);
        sinkTask.initialize(kafkaSinkTaskContext);
        sinkTask.start(taskConfig);
        transformationWrapper = new TransformationWrapper(taskConfig);
    }


    @Override
    public void stop() {
        if (sinkTask != null) {
            sinkTask.stop();
            sinkTask = null;
        }
    }
}
