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
package org.apache.rocketmq.connect.kafka.connect.adaptor.schema;

import io.openmessaging.connector.api.data.ConnectRecord;
import io.openmessaging.connector.api.data.RecordOffset;
import io.openmessaging.connector.api.data.RecordPartition;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.header.Headers;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.rocketmq.connect.kafka.connect.adaptor.context.RocketMQKafkaSinkTaskContext;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * converter transforms record
 */
public class Converters {

    public static final String TOPIC = "topic";

    public static ConnectRecord fromSourceRecord(SourceRecord record) {
        // sourceRecord convert connect Record
        RocketMQSourceSchemaConverter valueSchemaConverter = new RocketMQSourceSchemaConverter(record.valueSchema());
        io.openmessaging.connector.api.data.Schema valueSchema = valueSchemaConverter.schema();

        io.openmessaging.connector.api.data.Schema keySchema = null;
        if (record.keySchema() != null) {
            RocketMQSourceSchemaConverter keySchemaConverter = new RocketMQSourceSchemaConverter(record.keySchema());
            keySchema = keySchemaConverter.schema();
        }


        RocketMQSourceValueConverter rocketMQSourceValueConverter = new RocketMQSourceValueConverter();

        ConnectRecord connectRecord = new ConnectRecord(
                new RecordPartition(record.sourcePartition()),
                new RecordOffset(record.sourceOffset()),
                record.timestamp(),
                keySchema,
                record.key() == null ? null : rocketMQSourceValueConverter.value(keySchema, record.key()),
                valueSchema,
                record.value() == null ? null : rocketMQSourceValueConverter.value(valueSchema, record.value()));
        String sourceTopic = record.topic();
        if (StringUtils.isNotBlank(sourceTopic) ) {
            connectRecord.addExtension(TOPIC, sourceTopic);
        }
        Iterator<Header> headers = record.headers().iterator();
        while (headers.hasNext()) {
            Header header = headers.next();
            connectRecord.addExtension(header.key(), String.valueOf(header.value()));
        }
        return connectRecord;
    }


    public static ConnectRecord fromSinkRecord(SinkRecord record) {
        // sourceRecord convert connect Record
        RocketMQSourceSchemaConverter valueSchemaConverter = new RocketMQSourceSchemaConverter(record.valueSchema());
        io.openmessaging.connector.api.data.Schema valueSchema = valueSchemaConverter.schema();

        io.openmessaging.connector.api.data.Schema keySchema = null;
        if (record.keySchema() != null) {
            RocketMQSourceSchemaConverter keySchemaConverter = new RocketMQSourceSchemaConverter(record.keySchema());
            keySchema = keySchemaConverter.schema();
        }

        RocketMQSourceValueConverter rocketMQSourceValueConverter = new RocketMQSourceValueConverter();
        ConnectRecord connectRecord = new ConnectRecord(
                toRecordPartition(record),
                toRecordOffset(record),
                record.timestamp(),
                keySchema,
                record.key() == null ? null : rocketMQSourceValueConverter.value(keySchema, record.key()),
                valueSchema,
                record.value() == null ? null :rocketMQSourceValueConverter.value(valueSchema, record.value()));
        Iterator<Header> headers = record.headers().iterator();
        while (headers.hasNext()) {
            Header header = headers.next();
            connectRecord.addExtension(header.key(), String.valueOf(header.value()));
        }
        return connectRecord;
    }


    /**
     * convert rocketmq connect record to sink record
     *
     * @param record
     * @return
     */
    public static SinkRecord fromConnectRecord(ConnectRecord record) {
        // connect record  convert kafka  sink record
        KafkaSinkSchemaConverter valueSchemaConverter = new KafkaSinkSchemaConverter(record.getSchema());
        Schema schema = valueSchemaConverter.schema();
        // key converter
        Schema keySchema = null;
        if (record.getKeySchema() != null){
            KafkaSinkSchemaConverter keySchemaConverter = new KafkaSinkSchemaConverter(record.getKeySchema());
            keySchema = keySchemaConverter.schema();
        }

        KafkaSinkValueConverter sinkValueConverter = new KafkaSinkValueConverter();
        // add headers
        Headers headers = new ConnectHeaders();
        Iterator extensions = record.getExtensions().keySet().iterator();
        while (extensions.hasNext()) {
            String key = String.valueOf(extensions.next());
            headers.add(key, record.getExtensions().getString(key), null);
        }

        SinkRecord sinkRecord = new SinkRecord(
                topic(record.getPosition().getPartition()),
                partition(record.getPosition().getPartition()),
                keySchema,
                record.getKey() == null ? null : sinkValueConverter.value(keySchema, record.getKey()),
                schema,
                record.getData() == null ? null : sinkValueConverter.value(schema, record.getData()),
                offset(record.getPosition().getOffset()),
                record.getTimestamp(),
                TimestampType.NO_TIMESTAMP_TYPE,
                headers
        );
        return sinkRecord;
    }

    public static RecordPartition toRecordPartition(SinkRecord record) {

        Map<String, String> recordPartitionMap = new HashMap<>();
        recordPartitionMap.put(RocketMQKafkaSinkTaskContext.TOPIC, record.topic());
        recordPartitionMap.put(RocketMQKafkaSinkTaskContext.QUEUE_ID, record.kafkaPartition() + "");
        return new RecordPartition(recordPartitionMap);
    }

    public static RecordOffset toRecordOffset(SinkRecord record) {
        Map<String, String> recordOffsetMap = new HashMap<>();
        recordOffsetMap.put(RocketMQKafkaSinkTaskContext.QUEUE_OFFSET, record.kafkaOffset() + "");
        return new RecordOffset(recordOffsetMap);
    }


    /**
     * get topic
     *
     * @param partition
     * @return
     */
    public static String topic(RecordPartition partition) {
        return partition.getPartition().get(RocketMQKafkaSinkTaskContext.TOPIC).toString();
    }

    /**
     * get partition
     *
     * @param partition
     * @return
     */
    public static int partition(RecordPartition partition) {
        if (partition.getPartition().containsKey(RocketMQKafkaSinkTaskContext.QUEUE_ID)) {
            return Integer.valueOf(partition.getPartition().get(RocketMQKafkaSinkTaskContext.QUEUE_ID).toString());
        }
        return -1;
    }

    /**
     * get offset
     *
     * @param offset
     * @return
     */
    public static int offset(RecordOffset offset) {
        if (offset.getOffset().containsKey(RocketMQKafkaSinkTaskContext.QUEUE_OFFSET)) {
            return Integer.valueOf(offset.getOffset().get(RocketMQKafkaSinkTaskContext.QUEUE_OFFSET).toString());
        }
        return -1;
    }
}
