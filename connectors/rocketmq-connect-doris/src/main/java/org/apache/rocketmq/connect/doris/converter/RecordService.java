/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.rocketmq.connect.doris.converter;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.openmessaging.connector.api.data.ConnectRecord;
import io.openmessaging.connector.api.data.Struct;
import io.openmessaging.connector.api.errors.ConnectException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.StringJoiner;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.connect.doris.cfg.DorisOptions;
import org.apache.rocketmq.connect.doris.converter.schema.SchemaChangeManager;
import org.apache.rocketmq.connect.doris.converter.schema.SchemaEvolutionMode;
import org.apache.rocketmq.connect.doris.converter.type.Type;
import org.apache.rocketmq.connect.doris.exception.DataFormatException;
import org.apache.rocketmq.connect.doris.exception.DorisException;
import org.apache.rocketmq.connect.doris.exception.SchemaChangeException;
import org.apache.rocketmq.connect.doris.model.ColumnDescriptor;
import org.apache.rocketmq.connect.doris.model.TableDescriptor;
import org.apache.rocketmq.connect.doris.model.doris.Schema;
import org.apache.rocketmq.connect.doris.service.DorisSystemService;
import org.apache.rocketmq.connect.doris.service.RestService;
import org.apache.rocketmq.connect.doris.utils.ConnectRecordUtil;
import org.apache.rocketmq.connect.doris.writer.LoadConstants;
import org.apache.rocketmq.connect.doris.writer.RecordBuffer;
import org.apache.rocketmq.connect.runtime.converter.record.json.JsonConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RecordService {
    private static final Logger LOG = LoggerFactory.getLogger(RecordService.class);
    public static final String SCHEMA_CHANGE_VALUE = "SchemaChangeValue";
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private final JsonConverter converter;
    private DorisSystemService dorisSystemService;
    private SchemaChangeManager schemaChangeManager;
    private DorisOptions dorisOptions;
    private RecordTypeRegister recordTypeRegister;
    private Map<String, TableDescriptor> dorisTableDescriptorCache;

    public RecordService() {
        this.converter = new JsonConverter();
        Map<String, Object> config = new HashMap<>();
        config.put("converterConfig", "false");
        this.converter.configure(config);
    }

    public RecordService(DorisOptions dorisOptions) {
        this();
        this.dorisOptions = dorisOptions;
        this.recordTypeRegister = new RecordTypeRegister(dorisOptions);
        this.dorisSystemService = new DorisSystemService(dorisOptions);
        this.schemaChangeManager = new SchemaChangeManager(dorisOptions);
        this.dorisTableDescriptorCache = new HashMap<>();
    }

    /**
     * process struct record from debezium: { "schema": { "type": "struct", "fields": [ ...... ],
     * "optional": false, "name": "" }, "payload": { "name": "doris", "__deleted": "true" } }
     */
    public String processStructRecord(ConnectRecord record) {
        String processedRecord;
        String topicName = ConnectRecordUtil.getTopicName(record.getPosition().getPartition());
        if (ConverterMode.DEBEZIUM_INGESTION == dorisOptions.getConverterMode()) {
            validate(record);
            RecordDescriptor recordDescriptor = buildRecordDescriptor(record);
            if (recordDescriptor.isTombstone()) {
                return null;
            }
            String tableName = dorisOptions.getTopicMapTable(recordDescriptor.getTopicName());
            checkAndApplyTableChangesIfNeeded(tableName, recordDescriptor);

            List<String> nonKeyFieldNames = recordDescriptor.getNonKeyFieldNames();
            if (recordDescriptor.isDelete()) {
                processedRecord =
                    parseFieldValues(
                        recordDescriptor,
                        recordDescriptor.getBeforeStruct(),
                        nonKeyFieldNames,
                        true);
            } else {
                processedRecord =
                    parseFieldValues(
                        recordDescriptor,
                        recordDescriptor.getAfterStruct(),
                        nonKeyFieldNames,
                        false);
            }
        } else {
            byte[] bytes =
                converter.fromConnectData(topicName, record.getSchema().getValueSchema(), record.getData());
            processedRecord = new String(bytes, StandardCharsets.UTF_8);
        }
        return processedRecord;
    }

    private void validate(ConnectRecord record) {
        if (isSchemaChange(record)) {
            LOG.warn(
                "Schema change records are not supported by doris-kafka-connector. Adjust `topics` or `topics.regex` to exclude schema change topic.");
            throw new DorisException(
                "Schema change records are not supported by doris-kafka-connector. Adjust `topics` or `topics.regex` to exclude schema change topic.");
        }
    }

    private static boolean isSchemaChange(final ConnectRecord record) {
        return record.getSchema().getValueSchema() != null
            && StringUtils.isNotEmpty(record.getSchema().getValueSchema().getName())
            && record.getSchema().getValueSchema().getName().contains(SCHEMA_CHANGE_VALUE);
    }

    private void checkAndApplyTableChangesIfNeeded(
        String tableName, RecordDescriptor recordDescriptor) {
        if (!hasTable(tableName)) {
            // TODO Table does not exist, lets attempt to create it.
            LOG.warn("The {} table does not exist, please create it manually.", tableName);
            throw new DorisException(
                "The " + tableName + " table does not exist, please create it manually.");
        } else {
            // Table exists, lets attempt to alter it if necessary.
            alterTableIfNeeded(tableName, recordDescriptor);
        }
    }

    private boolean hasTable(String tableName) {
        if (!dorisTableDescriptorCache.containsKey(tableName)) {
            boolean exist = dorisSystemService.tableExists(dorisOptions.getDatabase(), tableName);
            if (exist) {
                dorisTableDescriptorCache.put(tableName, null);
            }
            return exist;
        }
        return true;
    }

    private void alterTableIfNeeded(String tableName, RecordDescriptor record) {
        // Resolve table metadata from the database
        final TableDescriptor table = fetchDorisTableDescriptor(tableName);

        Set<RecordDescriptor.FieldDescriptor> missingFields = resolveMissingFields(record, table);
        if (missingFields.isEmpty()) {
            // There are no missing fields, simply return
            // TODO should we check column type changes or default value changes?
            return;
        }

        LOG.info(
            "Find some miss columns in {} table, try to alter add this columns={}.",
            tableName,
            missingFields.stream()
                .map(RecordDescriptor.FieldDescriptor::getName)
                .collect(Collectors.toList()));
        if (SchemaEvolutionMode.NONE.equals(dorisOptions.getSchemaEvolutionMode())) {
            LOG.warn(
                "Table '{}' cannot be altered because schema evolution is disabled.",
                tableName);
            throw new SchemaChangeException(
                "Cannot alter table " + table + " because schema evolution is disabled");
        }
        for (RecordDescriptor.FieldDescriptor missingField : missingFields) {
            schemaChangeManager.addColumnDDL(tableName, missingField);
        }
        TableDescriptor newTableDescriptor = obtainTableSchema(tableName);
        dorisTableDescriptorCache.put(tableName, newTableDescriptor);
    }

    private Set<RecordDescriptor.FieldDescriptor> resolveMissingFields(
        RecordDescriptor record, TableDescriptor table) {
        Set<RecordDescriptor.FieldDescriptor> missingFields = new HashSet<>();
        for (Map.Entry<String, RecordDescriptor.FieldDescriptor> entry :
            record.getFields().entrySet()) {
            String filedName = entry.getKey();
            if (!table.hasColumn(filedName)) {
                missingFields.add(entry.getValue());
            }
        }
        return missingFields;
    }

    private TableDescriptor fetchDorisTableDescriptor(String tableName) {
        if (!dorisTableDescriptorCache.containsKey(tableName)
            || Objects.isNull(dorisTableDescriptorCache.get(tableName))) {
            TableDescriptor tableDescriptor = obtainTableSchema(tableName);
            dorisTableDescriptorCache.put(tableName, tableDescriptor);
        }
        return dorisTableDescriptorCache.get(tableName);
    }

    private TableDescriptor obtainTableSchema(String tableName) {
        Schema schema =
            RestService.getSchema(dorisOptions, dorisOptions.getDatabase(), tableName, LOG);
        List<ColumnDescriptor> columnDescriptors = new ArrayList<>();
        schema.getProperties()
            .forEach(
                column -> {
                    ColumnDescriptor columnDescriptor =
                        ColumnDescriptor.builder()
                            .columnName(column.getName())
                            .typeName(column.getType())
                            .comment(column.getComment())
                            .build();
                    columnDescriptors.add(columnDescriptor);
                });
        return TableDescriptor.builder()
            .tableName(tableName)
            .type(schema.getKeysType())
            .columns(columnDescriptors)
            .build();
    }

    /**
     * process list record from kafka [{"name":"doris1"},{"name":"doris2"}]
     */
    public String processListRecord(ConnectRecord record) {
        try {
            StringJoiner sj = new StringJoiner(RecordBuffer.LINE_SEPARATOR);
            List recordList = (List) record.getData();
            for (Object item : recordList) {
                sj.add(MAPPER.writeValueAsString(item));
            }
            return sj.toString();
        } catch (IOException e) {
            LOG.error("process list record failed: {}", record.getData());
            throw new DataFormatException("process list record failed");
        }
    }

    /**
     * process map record from kafka {"name":"doris"}
     */
    public String processMapRecord(ConnectRecord record) {
        try {
            return MAPPER.writeValueAsString(record.getData());
        } catch (IOException e) {
            LOG.error("process map record failed: {}", record.getData());
            throw new DataFormatException("process map record failed");
        }
    }

    private String parseFieldValues(
        RecordDescriptor record, Struct source, List<String> fields, boolean isDelete) {
        Map<String, Object> filedMapping = new LinkedHashMap<>();
        String filedResult = null;
        for (String fieldName : fields) {
            final RecordDescriptor.FieldDescriptor field = record.getFields().get(fieldName);
            Type type = field.getType();
            Object value =
                field.getSchema().isOptional()
                    ? source.getWithoutDefault(fieldName)
                    : source.get(fieldName);
            Object convertValue = type.getValue(value, field.getSchema());
            if (Objects.nonNull(convertValue) && !type.isNumber()) {
                filedMapping.put(fieldName, convertValue.toString());
            } else {
                filedMapping.put(fieldName, convertValue);
            }
        }
        try {
            if (isDelete) {
                filedMapping.put(LoadConstants.DORIS_DELETE_SIGN, LoadConstants.DORIS_DEL_TRUE);
            } else {
                filedMapping.put(LoadConstants.DORIS_DELETE_SIGN, LoadConstants.DORIS_DEL_FALSE);
            }
            filedResult = MAPPER.writeValueAsString(filedMapping);
        } catch (JsonProcessingException e) {
            LOG.error("parse record failed, cause by parse json error: {}", filedMapping);
        }
        return filedResult;
    }

    /**
     * Given a single Record from put API, process it and convert it into a Json String.
     *
     * @param record record from Kafka
     * @return Json String
     */
    public String getProcessedRecord(ConnectRecord record) {
        String processedRecord;
        if (record.getData() instanceof Struct) {
            processedRecord = processStructRecord(record);
        } else if (record.getData() instanceof List) {
            processedRecord = processListRecord(record);
        } else if (record.getData() instanceof Map) {
            processedRecord = processMapRecord(record);
        } else {
            processedRecord = record.getData().toString();
        }
        return processedRecord;
    }

    private RecordDescriptor buildRecordDescriptor(ConnectRecord record) {
        RecordDescriptor recordDescriptor;
        try {
            recordDescriptor =
                RecordDescriptor.builder()
                    .withSinkRecord(record)
                    .withTypeRegistry(recordTypeRegister.getTypeRegistry())
                    .build();
        } catch (Exception e) {
            throw new ConnectException("Failed to process a sink record", e);
        }
        return recordDescriptor;
    }

}
