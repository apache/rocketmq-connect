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

package org.apache.rocketmq.connect.debezium;

import io.debezium.relational.HistorizedRelationalDatabaseConnectorConfig;
import io.openmessaging.KeyValue;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.rocketmq.connect.kafka.connect.adaptor.KafkaConnectAdaptorSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.List;

/**
 * debezium source
 */
public abstract class DebeziumSource extends KafkaConnectAdaptorSource {
    private static final Logger log = LoggerFactory.getLogger(DebeziumSource.class);

    private static final String DEFAULT_HISTORY = "org.apache.rocketmq.connect.debezium.RocketMqDatabaseHistory";
    private TransformationWrapper transformationWrapper;

    /**
     * set source task class
     *
     * @param config
     * @throws Exception
     */
    public abstract void setSourceTask(KeyValue config);

    /**
     * convert by transform
     *
     * @param record
     */
    @Override
    protected SourceRecord transforms(SourceRecord record) {
        List<Transformation<SourceRecord>> transformations = transformationWrapper.transformations();
        Iterator transformationIterator = transformations.iterator();
        while (transformationIterator.hasNext()) {
            Transformation<SourceRecord> transformation = (Transformation) transformationIterator.next();
            log.trace("Applying transformation {} to {}", transformation.getClass().getName(), record);
            record = transformation.apply(record);
            if (record == null) {
                break;
            }
        }
        return record;
    }

    @Override
    public void init(KeyValue config) {
        setSourceTask(config);
        // database.history : implementation class for database history.
        config.put(HistorizedRelationalDatabaseConnectorConfig.DATABASE_HISTORY.name(), DEFAULT_HISTORY);
        // history config detail
        super.init(config);

        transformationWrapper = new TransformationWrapper(super.configValue.config());
    }
}
