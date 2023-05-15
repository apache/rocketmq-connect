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
package org.apache.rocketmq.connect.jdbc.sink;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.rocketmq.connect.jdbc.dialect.DatabaseDialect;
import org.apache.rocketmq.connect.jdbc.exception.TableAlterOrCreateException;
import org.apache.rocketmq.connect.jdbc.schema.column.ColumnDefinition;
import org.apache.rocketmq.connect.jdbc.schema.table.TableDefinition;
import org.apache.rocketmq.connect.jdbc.schema.table.TableDefinitions;
import org.apache.rocketmq.connect.jdbc.schema.table.TableId;
import org.apache.rocketmq.connect.jdbc.sink.metadata.FieldsMetadata;
import org.apache.rocketmq.connect.jdbc.sink.metadata.SinkRecordField;
import org.apache.rocketmq.connect.jdbc.util.TableType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class DbStructure {
    private static final Logger log = LoggerFactory.getLogger(DbStructure.class);
    private final DatabaseDialect dbDialect;
    private final TableDefinitions tableDefinitions;

    public DbStructure(DatabaseDialect dbDialect) {
        this.dbDialect = dbDialect;
        this.tableDefinitions = new TableDefinitions(dbDialect);
    }


    public boolean createOrAmendIfNecessary(
            final JdbcSinkConfig config,
            final Connection connection,
            final TableId tableId,
            final FieldsMetadata fieldsMetadata
    ) throws SQLException {
        if (tableDefinitions.get(connection, tableId) == null) {
            try {
                create(config, connection, tableId, fieldsMetadata);
            } catch (SQLException sqle) {
                log.warn("Create failed, will attempt amend if table already exists", sqle);
                try {
                    TableDefinition newDefn = tableDefinitions.refresh(connection, tableId);
                    if (newDefn == null) {
                        throw sqle;
                    }
                } catch (SQLException e) {
                    throw sqle;
                }
            }
        }
        return amendIfNecessary(config, connection, tableId, fieldsMetadata, config.getMaxRetries());
    }


    public TableDefinition tableDefinition(
            Connection connection,
            TableId tableId
    ) throws SQLException {
        TableDefinition tableDefinition = tableDefinitions.get(connection, tableId);
        if (tableDefinition != null) {
            return tableDefinition;
        }
        return tableDefinitions.refresh(connection, tableId);
    }

    void create(
            final JdbcSinkConfig config,
            final Connection connection,
            final TableId tableId,
            final FieldsMetadata fieldsMetadata
    ) throws SQLException {
        if (!config.isAutoCreate()) {
            throw new TableAlterOrCreateException(
                    String.format("Table %s is missing and auto-creation is disabled", tableId)
            );
        }
        String sql = dbDialect.buildCreateTableStatement(tableId, fieldsMetadata.allFields.values());
        log.info("Creating table with sql: {}", sql);
        dbDialect.executeSchemaChangeStatements(connection, Collections.singletonList(sql));
    }

    boolean amendIfNecessary(
            final JdbcSinkConfig config,
            final Connection connection,
            final TableId tableId,
            final FieldsMetadata fieldsMetadata,
            final int maxRetries
    ) throws SQLException, TableAlterOrCreateException {
        final TableDefinition tableDefn = tableDefinitions.get(connection, tableId);
        final Set<SinkRecordField> missingFields = missingFields(
            fieldsMetadata.allFields.values(),
            tableDefn.columnNames()
        );

        if (missingFields.isEmpty()) {
            return false;
        }
        TableType type = tableDefn.type();
        switch (type) {
            case TABLE:
                // Rather than embed the logic and change lots of lines, just break out
                break;
            case VIEW:
            default:
                throw new TableAlterOrCreateException(
                        String.format(
                                "%s %s is missing fields (%s) and ALTER %s is unsupported",
                                type.capitalized(),
                                tableId,
                                missingFields,
                                type.jdbcName()
                        )
                );
        }

        final Set<SinkRecordField> replacedMissingFields = new HashSet<>();
        for (SinkRecordField missingField : missingFields) {
            if (!missingField.isOptional() && missingField.defaultValue() == null) {
                throw new TableAlterOrCreateException(String.format(
                        "Cannot ALTER %s %s to add missing field %s, as the field is not optional and does "
                                + "not have a default value",
                        type.jdbcName(),
                        tableId,
                        missingField
                ));
            }
        }

        if (!config.isAutoCreate()) {
            throw new TableAlterOrCreateException(String.format(
                    "%s %s is missing fields (%s) and auto-evolution is disabled",
                    type.capitalized(),
                    tableId,
                    replacedMissingFields
            ));
        }

        final List<String> amendTableQueries = dbDialect.buildAlterTable(tableId, replacedMissingFields);
        log.info(
                "Amending {} to add missing fields:{} maxRetries:{} with SQL: {}",
                type,
                replacedMissingFields,
                maxRetries,
                amendTableQueries
        );
        try {
            dbDialect.executeSchemaChangeStatements(connection, amendTableQueries);
        } catch (SQLException sqle) {
            if (maxRetries <= 0) {
                throw new TableAlterOrCreateException(
                        String.format(
                                "Failed to amend %s '%s' to add missing fields: %s",
                                type,
                                tableId,
                                replacedMissingFields
                        ),
                        sqle
                );
            }
            log.warn("Amend failed, re-attempting", sqle);
            tableDefinitions.refresh(connection, tableId);
            // Perhaps there was a race with other tasks to add the columns
            return amendIfNecessary(
                    config,
                    connection,
                    tableId,
                    fieldsMetadata,
                    maxRetries - 1
            );
        }

        tableDefinitions.refresh(connection, tableId);
        return true;
    }

    Set<SinkRecordField> missingFields(
            Collection<SinkRecordField> fields,
            Set<String> dbColumnNames
    ) {
        final Set<SinkRecordField> missingFields = new HashSet<>();
        for (SinkRecordField field : fields) {
            if (!dbColumnNames.contains(field.name())) {
                log.debug("Found missing field: {}", field);
                missingFields.add(field);
            }
        }

        if (missingFields.isEmpty()) {
            return missingFields;
        }

        // check if the missing fields can be located by ignoring case
        Set<String> columnNamesLowerCase = new HashSet<>();
        for (String columnName : dbColumnNames) {
            columnNamesLowerCase.add(columnName.toLowerCase());
        }

        if (columnNamesLowerCase.size() != dbColumnNames.size()) {
            log.warn(
                    "Table has column names that differ only by case. Original columns={}",
                    dbColumnNames
            );
        }

        final Set<SinkRecordField> missingFieldsIgnoreCase = new HashSet<>();
        for (SinkRecordField missing : missingFields) {
            if (!columnNamesLowerCase.contains(missing.name().toLowerCase())) {
                missingFieldsIgnoreCase.add(missing);
            }
        }

        if (missingFieldsIgnoreCase.size() > 0) {
            log.info(
                    "Unable to find fields {} among column names {}",
                    missingFieldsIgnoreCase,
                    dbColumnNames
            );
        }

        return missingFieldsIgnoreCase;
    }


    /**
     * delete field
     *
     * @param fields
     * @param dbColumnNames
     * @return
     */
    Set<ColumnDefinition> alterDeleteFields(
            Collection<SinkRecordField> fields,
            Map<String, ColumnDefinition> dbColumnNames
    ) {

        final Map<String, SinkRecordField> recordFields = new ConcurrentHashMap<>();
        for (SinkRecordField field : fields) {
            recordFields.put(field.name(), field);
        }

        Set<ColumnDefinition> deleteColumns = new HashSet<>();
        // filter delete column name
        for (String columnName : dbColumnNames.keySet()) {
            if (!recordFields.containsKey(columnName)) {
                log.debug("Found delete field: {}", columnName);
                ColumnDefinition columnDefinition = dbColumnNames.get(columnName);
                deleteColumns.add(dbColumnNames.get(columnName));
            }
        }
        return deleteColumns;
    }
}
