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
package org.apache.rocketmq.connect.jdbc.connector;

import io.openmessaging.KeyValue;
import org.apache.rocketmq.connect.jdbc.config.AbstractConfig;
import org.apache.rocketmq.connect.jdbc.dialect.DatabaseDialect;
import org.apache.rocketmq.connect.jdbc.exception.ConfigException;
import org.apache.rocketmq.connect.jdbc.schema.table.TableId;
import org.apache.rocketmq.connect.jdbc.util.TableType;

import java.time.ZoneId;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.TimeZone;
import java.util.stream.Collectors;

/**
 * jdbc sink config
 */
public class JdbcSinkConfig extends AbstractConfig {

    public enum InsertMode {
        INSERT,
        UPSERT,
        UPDATE;

    }

    public enum PrimaryKeyMode {
        NONE,
        RECORD_KEY,
        RECORD_VALUE;
    }

    public static final String TABLE_NAME_FORMAT = "table.name.format";
    public static final String TABLE_NAME_FORMAT_DEFAULT = "${topic}";
    private static final String TABLE_NAME_FORMAT_DISPLAY = "Table Name Format";

    /**
     * table name from header
     */
    public static final String TABLE_NAME_FROM_HEADER = "table.name.from.header";
    private static final boolean TABLE_NAME_FROM_HEADER_DEFAULT = Boolean.FALSE;
    private static final String TABLE_NAME_FROM_HEADER_DISPLAY = "Table from header";

    /**
     * max retries
     */
    public static final String MAX_RETRIES = "max.retries";
    private static final int MAX_RETRIES_DEFAULT = 10;
    private static final String MAX_RETRIES_DOC =
            "The maximum number of times to retry on errors before failing the task.";

    public static final String RETRY_BACKOFF_MS = "retry.backoff.ms";
    private static final int RETRY_BACKOFF_MS_DEFAULT = 3000;

    public static final String BATCH_SIZE = "batch.size";
    private static final int BATCH_SIZE_DEFAULT = 100;


    public static final String DELETE_ENABLED = "delete.enabled";
    private static final boolean DELETE_ENABLED_DEFAULT = false;


    public static final String AUTO_CREATE = "auto.create";
    private static final boolean AUTO_CREATE_DEFAULT = false;

    public static final String AUTO_EVOLVE = "auto.evolve";
    private static final boolean AUTO_EVOLVE_DEFAULT = false;
    private static final String AUTO_EVOLVE_DOC =
            "Whether to automatically add columns in the table schema when found to be missing relative "
                    + "to the record schema by issuing ``ALTER``.";

    public static final String INSERT_MODE = "insert.mode";
    private static final String INSERT_MODE_DEFAULT = "insert";
    private static final String INSERT_MODE_DOC =
            "The insertion mode to use. Supported modes are:\n"
                    + "``insert``\n"
                    + "    Use standard SQL ``INSERT`` statements.\n"
                    + "``upsert``\n"
                    + "    Use the appropriate upsert semantics for the target database if it is supported by "
                    + "the connector, e.g. ``INSERT OR IGNORE``.\n"
                    + "``update``\n"
                    + "    Use the appropriate update semantics for the target database if it is supported by "
                    + "the connector, e.g. ``UPDATE``.";
    private static final String INSERT_MODE_DISPLAY = "Insert Mode";


    public static final String PK_FIELDS = "pk.fields";
    private static final String PK_FIELDS_DEFAULT = "";
    private static final String PK_FIELDS_DOC =
            "List of comma-separated primary key field names. The runtime interpretation of this config"
                    + " depends on the ``pk.mode``:\n"
                    + "``none``\n"
                    + "    Ignored as no fields are used as primary key in this mode.\n"
                    + "``record_key``\n"
                    + "    If empty, all fields from the key struct will be used, otherwise used to extract the"
                    + " desired fields - for primitive key only a single field name must be configured.\n"
                    + "``record_value``\n"
                    + "    If empty, all fields from the value struct will be used, otherwise used to extract "
                    + "the desired fields.";
    private static final String PK_FIELDS_DISPLAY = "Primary Key Fields";

    public static final String PK_MODE = "pk.mode";
    private static final String PK_MODE_DEFAULT = "none";
    private static final String PK_MODE_DOC =
            "The primary key mode, also refer to ``" + PK_FIELDS + "`` documentation for interplay. "
                    + "Supported modes are:\n"
                    + "``none``\n"
                    + "    No keys utilized.\n"
                    + "``record_value``\n"
                    + "    Field(s) from the record value are used, which must be a struct.";
    private static final String PK_MODE_DISPLAY = "Primary Key Mode";

    public static final String FIELDS_WHITELIST = "fields.whitelist";
    private static final String FIELDS_WHITELIST_DEFAULT = "";
    private static final String FIELDS_WHITELIST_DOC =
            "List of comma-separated record value field names. If empty, all fields from the record "
                    + "value are utilized, otherwise used to filter to the desired fields.\n"
                    + "Note that ``" + PK_FIELDS + "`` is applied independently in the context of which field"
                    + "(s) form the primary key columns in the destination database,"
                    + " while this configuration is applicable for the other columns.";
    private static final String FIELDS_WHITELIST_DISPLAY = "Fields Whitelist";


    public static final String DIALECT_NAME_CONFIG = "dialect.name";
    public static final String DIALECT_NAME_DEFAULT = "";


    public static final String DB_TIMEZONE_CONFIG = "db.timezone";
    public static final String DB_TIMEZONE_DEFAULT = "UTC";

    // table types
    public static final String TABLE_TYPES_CONFIG = "table.types";
    public static final String TABLE_TYPES_DEFAULT = TableType.TABLE.toString();
    private static final String TABLE_TYPES_DOC =
            "The comma-separated types of database tables to which the sink connector can write. "
                    + "By default this is ``" + TableType.TABLE + "``, but any combination of ``"
                    + TableType.TABLE + "`` and ``" + TableType.VIEW + "`` is allowed. Not all databases "
                    + "support writing to views, and when they do the the sink connector will fail if the "
                    + "view definition does not match the records' schemas (regardless of ``"
                    + AUTO_EVOLVE + "``).";

    // white list tables
    public static final String TABLE_WHITE_LIST_CONFIG = "tables.whitelist";
    public static final String TABLE_WHITE_LIST_DEFAULT = "";
    private static final String TABLE_WHITE_LIST_DOC =
            "Table white list.<br>db1.table01,db1.table02</br>";


    private String tableNameFormat;
    private boolean tableFromHeader;
    private int maxRetries;
    private int retryBackoffMs;
    private int batchSize;
    private boolean deleteEnabled;
    private boolean autoCreate;
    private boolean autoEvolve;
    private InsertMode insertMode;
    public final PrimaryKeyMode pkMode;
    private List<String> pkFields;
    private Set<String> fieldsWhitelist;
    private Set<String> tableWhitelist;
    private String dialectName;
    private TimeZone timeZone;
    private EnumSet<TableType> tableTypes;

    public JdbcSinkConfig(KeyValue config) {
        super(config);
        tableNameFormat = config.getString(TABLE_NAME_FORMAT, TABLE_NAME_FORMAT_DEFAULT).trim();
        tableFromHeader = getBoolean(config, TABLE_NAME_FROM_HEADER, false);
        batchSize = config.getInt(BATCH_SIZE, BATCH_SIZE_DEFAULT);

        maxRetries = config.getInt(MAX_RETRIES, MAX_RETRIES_DEFAULT);
        retryBackoffMs = config.getInt(RETRY_BACKOFF_MS, RETRY_BACKOFF_MS_DEFAULT);
        autoCreate = getBoolean(config, AUTO_CREATE, AUTO_CREATE_DEFAULT);
        autoEvolve = getBoolean(config, AUTO_EVOLVE, AUTO_EVOLVE_DEFAULT);
        if (Objects.nonNull(config.getString(INSERT_MODE))) {
            insertMode = InsertMode.valueOf(config.getString(INSERT_MODE, INSERT_MODE_DEFAULT).toUpperCase());
        }
        deleteEnabled = getBoolean(config, DELETE_ENABLED, DELETE_ENABLED_DEFAULT);
        pkMode = PrimaryKeyMode.valueOf(config.getString(PK_MODE, PK_MODE_DEFAULT).toUpperCase());
        pkFields = getList(config, PK_FIELDS);
        if (deleteEnabled && pkMode != PrimaryKeyMode.RECORD_KEY) {
            throw new ConfigException(
                    "Primary key mode must be 'record_key' when delete support is enabled");
        }
        dialectName = config.getString(DIALECT_NAME_CONFIG);
        fieldsWhitelist = new HashSet<>(getList(config, FIELDS_WHITELIST));
        // table white list
        tableWhitelist = new HashSet<>(getList(config, TABLE_WHITE_LIST_CONFIG));
        String dbTimeZone = config.getString(DB_TIMEZONE_CONFIG, DB_TIMEZONE_DEFAULT);
        timeZone = TimeZone.getTimeZone(ZoneId.of(dbTimeZone));
        tableTypes = TableType.parse(getList(config, TABLE_TYPES_CONFIG, TABLE_TYPES_DEFAULT));


    }

    public String getTableNameFormat() {
        return tableNameFormat;
    }

    public boolean isTableFromHeader() {
        return tableFromHeader;
    }

    public int getMaxRetries() {
        return maxRetries;
    }

    public int getRetryBackoffMs() {
        return retryBackoffMs;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public boolean isDeleteEnabled() {
        return deleteEnabled;
    }

    public boolean isAutoCreate() {
        return autoCreate;
    }

    public boolean isAutoEvolve() {
        return autoEvolve;
    }

    public InsertMode getInsertMode() {
        return insertMode;
    }

    public PrimaryKeyMode getPkMode() {
        return pkMode;
    }

    public List<String> getPkFields() {
        return pkFields;
    }

    public Set<String> getFieldsWhitelist() {
        return fieldsWhitelist;
    }

    public Set<String> getTableWhitelist() {
        return tableWhitelist;
    }

    public String getDialectName() {
        return dialectName;
    }

    public TimeZone getTimeZone() {
        return timeZone;
    }

    public EnumSet<TableType> getTableTypes() {
        return tableTypes;
    }

    /**
     * filter white table
     *
     * @param dbDialect
     * @param tableId
     * @return
     */
    public boolean filterWhiteTable(DatabaseDialect dbDialect, TableId tableId) {
        // not filter table
        if (tableWhitelist.isEmpty()) {
            return true;
        }
        for (String tableName : tableWhitelist) {
            TableId table = dbDialect.parseToTableId(tableName);
            if (table.catalogName() != null && table.catalogName().equals(tableId.catalogName())) {
                return true;
            }
            if (table.tableName().equals(tableId.tableName())) {
                return true;
            }
        }
        return false;
    }

    public Set<String> tableTypeNames() {
        return tableTypes.stream().map(TableType::toString).collect(Collectors.toSet());
    }

}
