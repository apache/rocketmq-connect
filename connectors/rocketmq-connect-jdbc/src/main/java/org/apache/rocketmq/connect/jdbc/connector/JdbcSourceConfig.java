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
import org.apache.rocketmq.connect.jdbc.util.NumericMapping;
import org.apache.rocketmq.connect.jdbc.util.TableType;

import java.time.ZoneId;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TimeZone;

/**
 * jdbc source config
 */
public class JdbcSourceConfig extends AbstractConfig {
    /**
     * table load mode
     */
    public enum TableLoadMode {
        MODE_BULK("bulk"),
        MODE_TIMESTAMP("timestamp"),
        MODE_INCREMENTING("incrementing"),
        MODE_TIMESTAMP_INCREMENTING("timestamp+incrementing");
        private String name;

        TableLoadMode(String name) {
            this.name = name;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public static TableLoadMode findTableLoadModeByName(String name) {
            for (TableLoadMode mode : TableLoadMode.values()) {
                if (mode.getName().equals(name)) {
                    return mode;
                }
            }
            throw new IllegalArgumentException("Unsupports mode " + name);
        }
    }

    //source poll interval ms
    public static final String POLL_INTERVAL_MS_CONFIG = "poll.interval.ms";
    private static final String POLL_INTERVAL_MS_DOC = "Frequency in ms to poll for new data in each table.";
    public static final int POLL_INTERVAL_MS_DEFAULT = 5000;
    // batch max rows

    public static final String BATCH_MAX_ROWS_CONFIG = "batch.max.rows";
    public static final int BATCH_MAX_ROWS_DEFAULT = 100;

    // numeric precision mapping
    public static final String NUMERIC_PRECISION_MAPPING_CONFIG = "numeric.precision.mapping";
    public static final boolean NUMERIC_PRECISION_MAPPING_DEFAULT = false;

    // numeric mapping
    public static final String NUMERIC_MAPPING_CONFIG = "numeric.mapping";
    public static final String NUMERIC_MAPPING_DEFAULT = null;

    // dialect name

    public static final String DIALECT_NAME_CONFIG = "dialect.name";
    public static final String DIALECT_NAME_DEFAULT = "";

    // table load mode
    public static final String MODE_CONFIG = "mode";

    // incrementing column name
    public static final String INCREMENTING_COLUMN_NAME_CONFIG = "incrementing.column.name";
    public static final String INCREMENTING_COLUMN_NAME_DEFAULT = "";

    // timestamp column name
    public static final String TIMESTAMP_COLUMN_NAME_CONFIG = "timestamp.column.name";
    public static final String TIMESTAMP_COLUMN_NAME_DEFAULT = "";


    // timestamp initial
    public static final String TIMESTAMP_INITIAL_CONFIG = "timestamp.initial";
    public static final Long TIMESTAMP_INITIAL_DEFAULT = null;
    public static final long TIMESTAMP_INITIAL_CURRENT = Long.valueOf(-1);

    // Metadata Change Monitoring Interval (ms)
    public static final String TABLE_POLL_INTERVAL_MS_CONFIG = "table.poll.interval.ms";
    public static final long TABLE_POLL_INTERVAL_MS_DEFAULT = 60 * 1000;

    // table white list
    public static final String TABLE_WHITELIST_CONFIG = "table.whitelist";
    public static final String TABLE_WHITELIST_DEFAULT = "";

    // table black list
    public static final String TABLE_BLACKLIST_CONFIG = "table.blacklist";
    public static final String TABLE_BLACKLIST_DEFAULT = "";

    public static final String SCHEMA_PATTERN_CONFIG = "schema.pattern";
    private static final String SCHEMA_PATTERN_DOC =
            "Schema pattern to fetch table metadata from the database.\n"
                    + "  * ``\"\"`` retrieves those without a schema.\n"
                    + "  * null (default) indicates that the schema name is not used to narrow the search and "
                    + "that all table metadata is fetched, regardless of the schema.";
    private static final String SCHEMA_PATTERN_DISPLAY = "Schema pattern";
    public static final String SCHEMA_PATTERN_DEFAULT = null;

    public static final String CATALOG_PATTERN_CONFIG = "catalog.pattern";
    private static final String CATALOG_PATTERN_DOC =
            "Catalog pattern to fetch table metadata from the database.\n"
                    + "  * ``\"\"`` retrieves those without a catalog \n"
                    + "  * null (default) indicates that the schema name is not used to narrow the search and "
                    + "that all table metadata is fetched, regardless of the catalog.";
    private static final String CATALOG_PATTERN_DISPLAY = "Schema pattern";
    public static final String CATALOG_PATTERN_DEFAULT = null;

    public static final String QUERY_CONFIG = "query";
    public static final String QUERY_DEFAULT = "";

    public static final String TOPIC_PREFIX_CONFIG = "topic.prefix";
    private static final String TOPIC_PREFIX_DOC =
            "Prefix to prepend to table names to generate the name of the Kafka topic to publish data "
                    + "to, or in the case of a custom query, the full name of the topic to publish to.";
    private static final String TOPIC_PREFIX_DISPLAY = "Topic Prefix";

    /**
     * validate non null
     */
    public static final String VALIDATE_NON_NULL_CONFIG = "validate.non.null";
    private static final String VALIDATE_NON_NULL_DOC =
            "By default, the JDBC connector will validate that all incrementing and timestamp tables "
                    + "have NOT NULL set for the columns being used as their ID/timestamp. If the tables don't,"
                    + " JDBC connector will fail to start. Setting this to false will disable these checks.";
    public static final boolean VALIDATE_NON_NULL_DEFAULT = true;
    private static final String VALIDATE_NON_NULL_DISPLAY = "Validate Non Null";

    public static final String TIMESTAMP_DELAY_INTERVAL_MS_CONFIG = "timestamp.delay.interval.ms";
    private static final String TIMESTAMP_DELAY_INTERVAL_MS_DOC =
            "How long to wait after a row with certain timestamp appears before we include it in the "
                    + "result. You may choose to add some delay to allow transactions with earlier timestamp to"
                    + " complete. The first execution will fetch all available records (i.e. starting at "
                    + "timestamp 0) until current time minus the delay. Every following execution will get data"
                    + " from the last time we fetched until current time minus the delay.";
    public static final long TIMESTAMP_DELAY_INTERVAL_MS_DEFAULT = 0;
    private static final String TIMESTAMP_DELAY_INTERVAL_MS_DISPLAY = "Delay Interval (ms)";


    public static final String DB_TIMEZONE_CONFIG = "db.timezone";
    public static final String DB_TIMEZONE_DEFAULT = "UTC";
    private static final String DB_TIMEZONE_CONFIG_DOC =
            "Name of the JDBC timezone used in the connector when "
                    + "querying with time-based criteria. Defaults to UTC.";
    private static final String DB_TIMEZONE_CONFIG_DISPLAY = "DB time zone";

    public static final String TABLE_TYPE_DEFAULT = "TABLE";
    public static final String TABLE_TYPE_CONFIG = "table.types";
    private static final String TABLE_TYPE_DOC =
            "By default, the JDBC connector will only detect tables with type TABLE from the source "
                    + "Database. This config allows a command separated list of table types to extract. Options"
                    + " include:\n"
                    + "  * TABLE\n"
                    + "  * VIEW\n"
                    + "  * SYSTEM TABLE\n"
                    + "  * GLOBAL TEMPORARY\n"
                    + "  * LOCAL TEMPORARY\n"
                    + "  * ALIAS\n"
                    + "  * SYNONYM\n"
                    + "  In most cases it only makes sense to have either TABLE or VIEW.";


    // The suffix to add at offset partition's key
    public static final String OFFSET_SUFFIX_CONFIG = "offset.suffix";
    public static final String OFFSET_SUFFIX_DEFAULT = "";
    public static final String OFFSET_SUFFIX_DOC = "Add this suffix to offset partition's key. " +
            "So every time when create connector can use new offset";

    // query suffix
    public static final String QUERY_SUFFIX_CONFIG = "query.suffix";
    public static final String QUERY_SUFFIX_DEFAULT = "";
    public static final String QUERY_SUFFIX_DOC = "Suffix to append at the end of the generated query.";

    private int pollIntervalMs;
    private int batchMaxRows;
    private Boolean numericPrecisionMapping;
    private String numericMapping;
    private String dialectName;
    private String mode;
    private String incrementingColumnName;
    private List<String> timestampColumnNames;
    private long timestampDelayIntervalMs;
    private Long timestampInitial = TIMESTAMP_INITIAL_DEFAULT;
    private Set<String> tableWhitelist;
    private Set<String> tableBlacklist;
    private String schemaPattern;
    private String catalogPattern;
    private String query;
    private String topicPrefix;
    private boolean validateNonNull;
    private EnumSet<TableType> tableTypes;
    private TimeZone timeZone;
    private String offsetSuffix;
    private String querySuffix;

    public JdbcSourceConfig(KeyValue config) {
        super(config);
        this.pollIntervalMs = config.getInt(POLL_INTERVAL_MS_CONFIG, POLL_INTERVAL_MS_DEFAULT);
        this.batchMaxRows = config.getInt(BATCH_MAX_ROWS_CONFIG, BATCH_MAX_ROWS_DEFAULT);
        this.numericPrecisionMapping = getBoolean(config, NUMERIC_PRECISION_MAPPING_CONFIG, NUMERIC_PRECISION_MAPPING_DEFAULT);
        this.numericMapping = config.getString(NUMERIC_MAPPING_CONFIG, NUMERIC_MAPPING_DEFAULT);
        this.dialectName = config.getString(DIALECT_NAME_CONFIG, DIALECT_NAME_DEFAULT);
        this.mode = config.getString(MODE_CONFIG);
        this.incrementingColumnName = config.getString(INCREMENTING_COLUMN_NAME_CONFIG);
        this.timestampColumnNames = getList(config, TIMESTAMP_COLUMN_NAME_CONFIG);
        timestampDelayIntervalMs = config.getLong(TIMESTAMP_DELAY_INTERVAL_MS_CONFIG);
//        this.timestampInitial=config.getLong(TIMESTAMP_INITIAL_CONFIG,TIMESTAMP_INITIAL_DEFAULT);
        if (config.containsKey(TIMESTAMP_INITIAL_CONFIG)) {
            this.timestampInitial = config.getLong(TIMESTAMP_INITIAL_CONFIG);
        }
        this.tableWhitelist = new HashSet<>(getList(config, TABLE_WHITELIST_CONFIG));
        this.tableBlacklist = new HashSet<>(getList(config, TABLE_BLACKLIST_CONFIG));
        this.schemaPattern = config.getString(SCHEMA_PATTERN_CONFIG);
        this.catalogPattern = config.getString(CATALOG_PATTERN_CONFIG);
        this.query = config.getString(QUERY_CONFIG);
        this.topicPrefix = config.getString(TOPIC_PREFIX_CONFIG);
        this.validateNonNull = getBoolean(config, VALIDATE_NON_NULL_CONFIG, VALIDATE_NON_NULL_DEFAULT);
        tableTypes = TableType.parse(getList(config, TABLE_TYPE_CONFIG, TABLE_TYPE_DEFAULT));
        String dbTimeZone = config.getString(DB_TIMEZONE_CONFIG, DB_TIMEZONE_DEFAULT);
        this.timeZone = TimeZone.getTimeZone(ZoneId.of(dbTimeZone));
        this.querySuffix = config.getString(QUERY_SUFFIX_CONFIG, QUERY_SUFFIX_DEFAULT);
        this.offsetSuffix = config.getString(OFFSET_SUFFIX_CONFIG, OFFSET_SUFFIX_DEFAULT);
    }


    public NumericMapping numericMapping() {
        return NumericMapping.get(this);
    }

    public int getPollIntervalMs() {
        return pollIntervalMs;
    }

    public int getBatchMaxRows() {
        return batchMaxRows;
    }

    public Boolean getNumericPrecisionMapping() {
        return numericPrecisionMapping;
    }

    public String getNumericMapping() {
        return numericMapping;
    }

    public String getDialectName() {
        return dialectName;
    }

    public String getMode() {
        return mode;
    }

    public String getIncrementingColumnName() {
        return incrementingColumnName;
    }

    public List<String> getTimestampColumnNames() {
        return timestampColumnNames;
    }

    public Long getTimestampInitial() {
        return timestampInitial;
    }

    public Set<String> getTableWhitelist() {
        return tableWhitelist;
    }

    public Set<String> getTableBlacklist() {
        return tableBlacklist;
    }

    public String getSchemaPattern() {
        return schemaPattern;
    }

    public String getCatalogPattern() {
        return catalogPattern;
    }

    public String getQuery() {
        return query;
    }

    public String getTopicPrefix() {
        return topicPrefix;
    }

    public boolean isValidateNonNull() {
        return validateNonNull;
    }

    public EnumSet<TableType> getTableTypes() {
        return tableTypes;
    }

    public TimeZone getTimeZone() {
        return timeZone;
    }

    public long getTimestampDelayIntervalMs() {
        return timestampDelayIntervalMs;
    }


    public String getOffsetSuffix() {
        return offsetSuffix;
    }

    public String getQuerySuffix() {
        return querySuffix;
    }
}
