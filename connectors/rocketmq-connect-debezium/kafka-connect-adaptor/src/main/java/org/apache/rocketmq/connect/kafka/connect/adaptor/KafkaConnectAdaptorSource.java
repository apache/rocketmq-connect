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

package org.apache.rocketmq.connect.kafka.connect.adaptor;

import io.openmessaging.connector.api.data.ConnectRecord;
import io.openmessaging.connector.api.data.RecordOffset;
import io.openmessaging.connector.api.data.RecordPartition;
import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.rocketmq.connect.kafka.connect.adaptor.schema.SchemaConverter;
import org.apache.rocketmq.connect.kafka.connect.adaptor.schema.ValueConverter;

import java.util.Iterator;


/**
 * kafka connect adaptor source
 */
public class KafkaConnectAdaptorSource extends AbstractKafkaConnectSource {


    /**
     * convert transform
     *
     * @param sourceRecord
     */
    @Override
    protected SourceRecord transforms(SourceRecord sourceRecord) {
        return sourceRecord;
    }

    /**
     * process source record
     *
     * @param record
     * @return
     */
    @Override
    public ConnectRecord processSourceRecord(SourceRecord record) {
        // sourceRecord convert connect Record
        SchemaConverter schemaConverter = new SchemaConverter(record);
        ValueConverter valueConverter = new ValueConverter(record);
        ConnectRecord connectRecord = new ConnectRecord(
                new RecordPartition(record.sourcePartition()),
                new RecordOffset(record.sourceOffset()),
                record.timestamp(),
                schemaConverter.schemaBuilder().build(),
                valueConverter.value());
        Iterator<Header> headers = record.headers().iterator();
        while (headers.hasNext()) {
            Header header = headers.next();
            connectRecord.addExtension(header.key(), String.valueOf(header.value()));
        }
        return connectRecord;
    }
}
