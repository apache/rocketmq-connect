#set( $symbol_pound = '#' )
#set( $symbol_dollar = '$' )
#set( $symbol_escape = '\' )
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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package ${package}.source;

import io.openmessaging.KeyValue;
import io.openmessaging.connector.api.component.task.source.SourceTask;
import io.openmessaging.connector.api.data.ConnectRecord;
import io.openmessaging.connector.api.data.Field;
import io.openmessaging.connector.api.data.RecordOffset;
import io.openmessaging.connector.api.data.RecordPartition;
import io.openmessaging.connector.api.data.Schema;
import io.openmessaging.connector.api.data.SchemaBuilder;
import io.openmessaging.connector.api.data.Struct;
import io.openmessaging.internal.DefaultKeyValue;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import ${package}.helper.${dbNameToCamel}HelperClient;
import ${package}.helper.${dbNameToCamel}Record;
import ${package}.config.${dbNameToCamel}Constants;
import ${package}.config.${dbNameToCamel}SourceConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ${dbNameToCamel}SourceTask extends SourceTask {

    private static final Logger log = LoggerFactory.getLogger(${dbNameToCamel}SourceTask.class);

    private ${dbNameToCamel}SourceConfig config;

    private ${dbNameToCamel}HelperClient helperClient;

    @Override public List<ConnectRecord> poll() {
        List<ConnectRecord> res = new ArrayList<>();
        long offset = readRecordOffset();
        List<${dbNameToCamel}Record> recordList = helperClient.query(offset, ${dbNameToCamel}Constants.BATCH_SIZE);
        res = recordList.stream().map(record -> ${dbNameToLowerCase}Record2ConnectRecord(record, offset)).collect(Collectors.toList());
        // FIXME: Write your code here
        throw new RuntimeException("Method not implemented");

        return res;
    }

    private long readRecordOffset() {
        final RecordOffset positionInfo = this.sourceTaskContext.offsetStorageReader().readOffset(buildRecordPartition(config.getTable()));
        if (positionInfo == null) {
            return 0;
        }
        Object offset = positionInfo.getOffset().get(config.getTable() + "_" + ${dbNameToCamel}Constants.${dbNameToUpperCase}_OFFSET);
        return offset == null ? 0 : Long.parseLong(offset.toString());
    }

    private ConnectRecord ${dbNameToLowerCase}Record2ConnectRecord(${dbNameToCamel}Record ${dbNameToLowerCase}Record, long offset)
        throws NoSuchFieldException, IllegalAccessException {
        Schema schema = SchemaBuilder.struct().name(config.getTable()).build();
        final List<Field> fields = buildFields(${dbNameToLowerCase}Record);
        schema.setFields(fields);
        final ConnectRecord connectRecord = new ConnectRecord(buildRecordPartition(config.getTable()),
            buildRecordOffset(offset),
            System.currentTimeMillis(),
            schema,
            this.buildPayLoad(fields, schema, ${dbNameToLowerCase}Record));
        connectRecord.setExtensions(this.buildExtensions(${dbNameToLowerCase}Record));
        return connectRecord;
    }

    private List<Field> buildFields(
        ${dbNameToCamel}Record ${dbNameToLowerCase}Record) throws NoSuchFieldException, IllegalAccessException {
        List<Field> fields = new ArrayList<>();

        // FIXME: Write your code here

        return fields;
    }

    private RecordPartition buildRecordPartition(String partitionValue) {
        Map<String, String> partitionMap = new HashMap<>();
        partitionMap.put(${dbNameToCamel}Constants.${dbNameToUpperCase}_PARTITION, partitionValue);
        return new RecordPartition(partitionMap);
    }

    private Struct buildPayLoad(List<Field> fields, Schema schema, ${dbNameToCamel}Record ${dbNameToLowerCase}Record) {
        Struct payLoad = new Struct(schema);
        for (int i = 0; i < fields.size(); i++) {
            // FIXME: Write your code here
        }
        return payLoad;
    }

    private KeyValue buildExtensions(${dbNameToUpperCase}Record ${dbNameToLowerCase}Record) {
        KeyValue keyValue = new DefaultKeyValue();
        String topicName = config.getTopic();
        if (topicName == null || topicName.equals("")) {
            String connectorName = this.sourceTaskContext.getConnectorName();
            topicName = config.getTable() + "_" + connectorName;
        }
        keyValue.put(${dbNameToUpperCase}Constants.TOPIC, topicName);
        return keyValue;
    }

    private RecordOffset buildRecordOffset(long offset) {
        Map<String, Long> offsetMap = new HashMap<>();
        offsetMap.put(config.getTable() + "_" + ${dbNameToCamel}Constants.${dbNameToUpperCase}_OFFSET, offset);
        return new RecordOffset(offsetMap);
    }

    private static Schema getSchema(Class clazz) {
        if (clazz.equals(Byte.class)) {
            return SchemaBuilder.int8().build();
        } else if (clazz.equals(Short.class) || clazz.equals(UnsignedByte.class)) {
            return SchemaBuilder.int16().build();
        } else if (clazz.equals(Integer.class) || clazz.equals(UnsignedShort.class)) {
            return SchemaBuilder.int32().build();
        } else if (clazz.equals(Long.class) || clazz.equals(UnsignedInteger.class)) {
            return SchemaBuilder.int64().build();
        } else if (clazz.equals(Float.class)) {
            return SchemaBuilder.float32().build();
        } else if (clazz.equals(Double.class)) {
            return SchemaBuilder.float64().build();
        } else if (clazz.equals(String.class)) {
            return SchemaBuilder.string().build();
        } else if (clazz.equals(Date.class) || clazz.equals(LocalDateTime.class) || clazz.equals(LocalDate.class)) {
            return SchemaBuilder.time().build();
        } else if (clazz.equals(Timestamp.class)) {
            return SchemaBuilder.timestamp().build();
        } else if (clazz.equals(Boolean.class)) {
            return SchemaBuilder.bool().build();
        }
        return SchemaBuilder.string().build();
    }

    @Override public void start(KeyValue keyValue) {
        this.config = new ${dbNameToCamel}SourceConfig();
        this.config.load(keyValue);
        this.helperClient = new ${dbNameToCamel}HelperClient(this.config);
        if (!helperClient.ping()) {
            throw new RuntimeException("Cannot connect to ${dbNameToLowerCase} server!");
        }
    }

    @Override public void stop() {
        this.helperClient = null;
    }
}
