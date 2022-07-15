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
import io.openmessaging.connector.api.component.task.source.SourceTask;
import io.openmessaging.connector.api.data.ConnectRecord;
import io.openmessaging.connector.api.errors.ConnectException;
import org.apache.kafka.connect.runtime.TaskConfig;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTaskContext;
import org.apache.kafka.connect.storage.OffsetStorageReader;
import org.apache.rocketmq.connect.kafka.connect.adaptor.config.ConnectKeyValue;
import org.apache.rocketmq.connect.kafka.connect.adaptor.context.KafkaOffsetStorageReader;
import org.apache.rocketmq.connect.kafka.connect.adaptor.context.RocketMQKafkaSourceTaskContext;
import org.apache.rocketmq.connect.kafka.connect.adaptor.schema.Converters;
import org.apache.rocketmq.connect.kafka.connect.adaptor.transforms.TransformationWrapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * abstract kafka connect
 */
public abstract class AbstractKafkaConnectSource extends SourceTask implements TaskClassSetter {

    private static final Logger log = LoggerFactory.getLogger(AbstractKafkaConnectSource.class);

    protected TransformationWrapper transformationWrapper;
    /**
     * kafka connect init
     */
    protected ConnectKeyValue configValue;
    private SourceTaskContext kafkaSourceTaskContext;
    private org.apache.kafka.connect.source.SourceTask sourceTask;
    private OffsetStorageReader offsetReader;

    @Override
    public List<ConnectRecord> poll() throws InterruptedException {
        List<SourceRecord> recordList = sourceTask.poll();
        if (recordList == null || recordList.isEmpty()) {
            Thread.sleep(1000);
        }
        List<ConnectRecord> records = new ArrayList<>();
        for (SourceRecord sourceRecord : recordList) {
            // transforms
            SourceRecord transformRecord = transforms(sourceRecord);
            if (transformRecord == null){
                log.debug("SourceRecord has been filtered out , record {}", sourceRecord.toString());
                continue;
            }
            ConnectRecord processRecord = Converters.fromSourceRecord(transformRecord);
            if (processRecord != null) {
                records.add(processRecord);
            }
        }
        return records;
    }

    /**
     * convert transform
     *
     * @param sourceRecord
     */
    protected abstract SourceRecord transforms(SourceRecord sourceRecord);

    /**
     * process source record
     *
     * @param next
     * @return
     */
    public abstract ConnectRecord processSourceRecord(SourceRecord next);


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
            sourceTask = Class.forName(taskConfig.get(TaskConfig.TASK_CLASS_CONFIG), true, AbstractKafkaConnectSource.class.getClassLoader())
                    .asSubclass(org.apache.kafka.connect.source.SourceTask.class)
                    .getDeclaredConstructor()
                    .newInstance();
        } catch (Exception e) {
            throw new ConnectException("Load task class failed, " + taskConfig.get(TaskConfig.TASK_CLASS_CONFIG));
        }

        offsetReader = new KafkaOffsetStorageReader(
                sourceTaskContext
        );

        kafkaSourceTaskContext = new RocketMQKafkaSourceTaskContext(offsetReader, taskConfig);
        sourceTask.initialize(kafkaSourceTaskContext);
        sourceTask.start(taskConfig);
        transformationWrapper = new TransformationWrapper(taskConfig);
    }


    @Override
    public void stop() {
        if (sourceTask != null) {
            sourceTask.stop();
            sourceTask = null;
        }
    }
}
