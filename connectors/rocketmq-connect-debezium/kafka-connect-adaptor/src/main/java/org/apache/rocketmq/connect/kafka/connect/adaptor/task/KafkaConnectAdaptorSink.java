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

import io.openmessaging.connector.api.data.ConnectRecord;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.rocketmq.connect.kafka.connect.adaptor.schema.Converters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.List;

/**
 * kafka connect adaptor sink
 */
public abstract class KafkaConnectAdaptorSink extends AbstractKafkaConnectSink {
    private static final Logger log = LoggerFactory.getLogger(KafkaConnectAdaptorSink.class);

    /**
     * convert by kafka sink transform
     *
     * @param record
     */
    @Override
    protected SinkRecord transforms(SinkRecord record) {
        List<Transformation> transformations = transformationWrapper.transformations();
        Iterator transformationIterator = transformations.iterator();
        while (transformationIterator.hasNext()) {
            Transformation<SinkRecord> transformation = (Transformation) transformationIterator.next();
            log.trace("applying transformation {} to {}", transformation.getClass().getName(), record);
            record = transformation.apply(record);
            if (record == null) {
                break;
            }
        }
        return record;
    }

    /**
     * convert ConnectRecord to SinkRecord
     *
     * @param record
     * @return
     */
    @Override
    public SinkRecord processSinkRecord(ConnectRecord record) {
        SinkRecord sinkRecord = Converters.fromConnectRecord(record);
        return transforms(sinkRecord);
    }

}
