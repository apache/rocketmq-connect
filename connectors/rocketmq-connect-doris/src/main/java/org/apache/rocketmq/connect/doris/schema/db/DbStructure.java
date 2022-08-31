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
package org.apache.rocketmq.connect.doris.schema.db;

import org.apache.rocketmq.connect.doris.connector.DorisSinkConfig;
import org.apache.rocketmq.connect.doris.exception.TableAlterOrCreateException;
import org.apache.rocketmq.connect.doris.schema.column.ColumnDefinition;
//import org.apache.rocketmq.connect.doris.schema.table.TableDefinition;
import org.apache.rocketmq.connect.doris.schema.table.TableId;
import org.apache.rocketmq.connect.doris.sink.metadata.FieldsMetadata;
import org.apache.rocketmq.connect.doris.sink.metadata.SinkRecordField;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;

import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 *
 */
public class DbStructure {
    private static final Logger log = LoggerFactory.getLogger(DbStructure.class);

//    private final DatabaseDialect dbDialect;
//    private final TableDefinitions tableDefns;
//
//    public DbStructure(DatabaseDialect dbDialect) {
////        this.dbDialect = dbDialect;
//        this.tableDefns = new TableDefinitions();
//    }

    /**
     * Create or amend table.
     *
     * @param config         the connector configuration
//     * @param connection     the database connection handle
     * @param tableId        the table ID
     * @param fieldsMetadata the fields metadata
     * @return whether a DDL operation was performed
     * @throws SQLException if a DDL operation was deemed necessary but failed
     */
    public boolean createOrAmendIfNecessary(
            final DorisSinkConfig config,
            final TableId tableId,
            final FieldsMetadata fieldsMetadata
    ) throws SQLException {
        return false;
//        if (!tableDefns.contains(tableId)) {
//            // Table does not yet exist, so attempt to create it ...
//            try {
//                create(config, tableId, fieldsMetadata);
//            } catch (TableAlterOrCreateException sqle) {
//                log.warn("Create failed, will attempt amend if table already exists", sqle);
//                try {
//                    tableDefns.refresh(tableId);
////                    TableDefinition newDefn = tableDefns.refresh(tableId);
////                    if (newDefn == null) {
////                        throw sqle;
////                    }
//                } catch (SQLException e) {
//                    throw sqle;
//                }
//            }
//        }
//        return amendIfNecessary(config, tableId, fieldsMetadata, config.getMaxRetries());
    }

//    public void applyDdl(String ddl) throws SQLException {
//        Connection connection = dbDialect.getConnection();
//        dbDialect.applyDdlStatements(connection, Collections.singletonList(ddl));
//    }

    /**
     * Get the definition for the table with the given ID. This returns a cached definition if
     * there is one; otherwise, it reads the definition from the database
     *
     * @param connection the connection that may be used to fetch the table definition if not
     *                   already known; may not be null
     * @param tableId    the ID of the table; may not be null
     * @return the table definition; or null if the table does not exist
     * @throws SQLException if there is an error getting the definition from the database
     */
//    public TableDefinition tableDefinition(
//            Connection connection,
//            TableId tableId
//    ) throws SQLException {
//        TableDefinition defn = tableDefns.get(connection, tableId);
//        if (defn != null) {
//            return defn;
//        }
//        return tableDefns.refresh(connection, tableId);
//    }

    /**
     * @throws SQLException if CREATE failed
     */
    void create(
            final DorisSinkConfig config,
            final TableId tableId,
            final FieldsMetadata fieldsMetadata
    ) throws TableAlterOrCreateException {
        log.info("I don't want to say Doris doc is a shit, but I didn't find stream load API to create a table. Please do it yourself :(");
        throw new TableAlterOrCreateException("Create table on doris first");
    }

    /**
     * @return whether an ALTER was successfully performed
     * @throws SQLException if ALTER was deemed necessary but failed
     */
    boolean amendIfNecessary(
            final DorisSinkConfig config,
            final TableId tableId,
            final FieldsMetadata fieldsMetadata,
            final int maxRetries
    ) throws TableAlterOrCreateException {
        throw new TableAlterOrCreateException("not support");
//        final TableDefinition tableDefn = tableDefns.get(connection, tableId);
//        final Set<SinkRecordField> missingFields = missingFields(
//                fieldsMetadata.allFields.values(),
//                tableDefn.columnNames()
//        );
//
//        if (missingFields.isEmpty()) {
//            return false;
//        }
//        // At this point there are missing fields
//        TableType type = tableDefn.type();
//        switch (type) {
//            case TABLE:
//                // Rather than embed the logic and change lots of lines, just break out
//                break;
//            case VIEW:
//            default:
//                throw new TableAlterOrCreateException(
//                        String.format(
//                                "%s %s is missing fields (%s) and ALTER %s is unsupported",
//                                type.capitalized(),
//                                tableId,
//                                missingFields,
//                                type.jdbcName()
//                        )
//                );
//        }
//
//        final Set<SinkRecordField> replacedMissingFields = new HashSet<>();
//        for (SinkRecordField missingField : missingFields) {
//            if (!missingField.isOptional() && missingField.defaultValue() == null) {
//                throw new TableAlterOrCreateException(String.format(
//                        "Cannot ALTER %s %s to add missing field %s, as the field is not optional and does "
//                                + "not have a default value",
//                        type.jdbcName(),
//                        tableId,
//                        missingField
//                ));
//            }
//        }
//
//        if (!config.isAutoCreate()) {
//            throw new TableAlterOrCreateException(String.format(
//                    "%s %s is missing fields (%s) and auto-evolution is disabled",
//                    type.capitalized(),
//                    tableId,
//                    replacedMissingFields
//            ));
//        }
//
//        final List<String> amendTableQueries = dbDialect.buildAlterTable(tableId, replacedMissingFields);
//        log.info(
//                "Amending {} to add missing fields:{} maxRetries:{} with SQL: {}",
//                type,
//                replacedMissingFields,
//                maxRetries,
//                amendTableQueries
//        );
//        try {
//            dbDialect.applyDdlStatements(connection, amendTableQueries);
//        } catch (SQLException sqle) {
//            if (maxRetries <= 0) {
//                throw new TableAlterOrCreateException(
//                        String.format(
//                                "Failed to amend %s '%s' to add missing fields: %s",
//                                type,
//                                tableId,
//                                replacedMissingFields
//                        ),
//                        sqle
//                );
//            }
//            log.warn("Amend failed, re-attempting", sqle);
//            tableDefns.refresh(connection, tableId);
//            // Perhaps there was a race with other tasks to add the columns
//            return amendIfNecessary(
//                    config,
//                    connection,
//                    tableId,
//                    fieldsMetadata,
//                    maxRetries - 1
//            );
//        }
//
//        tableDefns.refresh(connection, tableId);
//        return true;
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
