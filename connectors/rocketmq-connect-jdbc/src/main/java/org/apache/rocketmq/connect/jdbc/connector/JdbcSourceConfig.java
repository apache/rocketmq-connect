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
import java.util.*;

/**
 * jdbc source config
 * @author xiaoyi
 */
public class JdbcSourceConfig extends AbstractConfig {
    /**
     * table load mode
     */
    public enum TableLoadMode {
        MODE_BULK("bulk"),
        MODE_TIMESTAMP("timestamp"),
        MODE_INCREMENTING( "incrementing"),
        MODE_TIMESTAMP_INCREMENTING("timestamp+incrementing");
        private String name;
        TableLoadMode(String name) {
            this.name=name;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }
        public static TableLoadMode findTableLoadModeByName(String name){
            for (TableLoadMode mode : TableLoadMode.values()){
                if (mode.getName().equals(name)){
                    return mode;
                }
            }
            throw new IllegalArgumentException("Unsupports mode "+ name);
        }
    }

    //source poll interval ms
    public static final String POLL_INTERVAL_MS_CONFIG = "poll.interval.ms";
    private static final String POLL_INTERVAL_MS_DOC = "Frequency in ms to poll for new data in each table.";
    public static final int POLL_INTERVAL_MS_DEFAULT = 5000;
    // batch max rows

    public static final String BATCH_MAX_ROWS_CONFIG = "batch.max.rows";
    private static final String BATCH_MAX_ROWS_DOC = "Maximum number of rows to include in a single batch when polling for new data. This setting can be used to limit the amount of data buffered internally in the connector.";
    public static final int BATCH_MAX_ROWS_DEFAULT = 100;

    // numeric precision mapping
    public static final String NUMERIC_PRECISION_MAPPING_CONFIG = "numeric.precision.mapping";
    private static final String NUMERIC_PRECISION_MAPPING_DOC =
            "Whether or not to attempt mapping NUMERIC values by precision to integral types. This "
                    + "option is now deprecated. A future version may remove it completely. Please use "
                    + "``numeric.mapping`` instead.";
    public static final boolean NUMERIC_PRECISION_MAPPING_DEFAULT = false;

    // numeric mapping
    public static final String NUMERIC_MAPPING_CONFIG = "numeric.mapping";
    private static final String NUMERIC_MAPPING_DOC =
            "Map NUMERIC values by precision and optionally scale to integral or decimal types.\n"
                    + "  * Use ``none`` if all NUMERIC columns are to be represented by Connect's DECIMAL "
                    + "logical type.\n"
                    + "  * Use ``best_fit`` if NUMERIC columns should be cast to Connect's INT8, INT16, "
                    + "INT32, INT64, or FLOAT64 based upon the column's precision and scale. This option may "
                    + "still represent the NUMERIC value as Connect DECIMAL if it cannot be cast to a native "
                    + "type without losing precision. For example, a NUMERIC(20) type with precision 20 would "
                    + "not be able to fit in a native INT64 without overflowing and thus would be retained as "
                    + "DECIMAL.\n"
                    + "  * Use ``best_fit_eager_double`` if in addition to the properties of ``best_fit`` "
                    + "described above, it is desirable to always cast NUMERIC columns with scale to Connect "
                    + "FLOAT64 type, despite potential of loss in accuracy.\n"
                    + "  * Use ``precision_only`` to map NUMERIC columns based only on the column's precision "
                    + "assuming that column's scale is 0.\n"
                    + "  * The ``none`` option is the default, but may lead to serialization issues with Avro "
                    + "since Connect's DECIMAL type is mapped to its binary representation, and ``best_fit`` "
                    + "will often be preferred since it maps to the most appropriate primitive type.";

    public static final String NUMERIC_MAPPING_DEFAULT = null;
    private static final String NUMERIC_MAPPING_DISPLAY = "Map Numeric Values, Integral "
            + "or Decimal, By Precision and Scale";

    // dialect name

    public static final String DIALECT_NAME_CONFIG = "dialect.name";
    public static final String DIALECT_NAME_DEFAULT = "";
    private static final String DIALECT_NAME_DOC =
            "The name of the database dialect that should be used for this connector. By default this "
                    + "is empty, and the connector automatically determines the dialect based upon the "
                    + "JDBC connection URL. Use this if you want to override that behavior and use a "
                    + "specific dialect. All properly-packaged dialects in the JDBC connector plugin "
                    + "can be used.";

    // table load mode
    public static final String MODE_CONFIG = "mode";
    private static final String MODE_DOC =
            "The mode for updating a table each time it is polled. Options include:\n"
                    + "  * bulk: perform a bulk load of the entire table each time it is polled\n"
                    + "  * incrementing: use a strictly incrementing column on each table to "
                    + "detect only new rows. Note that this will not detect modifications or "
                    + "deletions of existing rows.\n"
                    + "  * timestamp: use a timestamp (or timestamp-like) column to detect new and modified "
                    + "rows. This assumes the column is updated with each write, and that values are "
                    + "monotonically incrementing, but not necessarily unique.\n"
                    + "  * timestamp+incrementing: use two columns, a timestamp column that detects new and "
                    + "modified rows and a strictly incrementing column which provides a globally unique ID for "
                    + "updates so each row can be assigned a unique stream offset.";

    // incrementing column name
    public static final String INCREMENTING_COLUMN_NAME_CONFIG = "incrementing.column.name";
    private static final String INCREMENTING_COLUMN_NAME_DOC =
            "The name of the strictly incrementing column to use to detect new rows. Any empty value "
                    + "indicates the column should be autodetected by looking for an auto-incrementing column. "
                    + "This column may not be nullable.";
    public static final String INCREMENTING_COLUMN_NAME_DEFAULT = "";

    // timestamp column name
    public static final String TIMESTAMP_COLUMN_NAME_CONFIG = "timestamp.column.name";
    private static final String TIMESTAMP_COLUMN_NAME_DOC =
            "Comma separated list of one or more timestamp columns to detect new or modified rows using "
                    + "the COALESCE SQL function. Rows whose first non-null timestamp value is greater than the "
                    + "largest previous timestamp value seen will be discovered with each poll. At least one "
                    + "column should not be nullable.";
    public static final String TIMESTAMP_COLUMN_NAME_DEFAULT = "";


    // timestamp initial
    public static final String TIMESTAMP_INITIAL_CONFIG = "timestamp.initial";
    public static final Long TIMESTAMP_INITIAL_DEFAULT = null;
    public static final long TIMESTAMP_INITIAL_CURRENT = Long.valueOf(-1);
    public static final String TIMESTAMP_INITIAL_DOC =
            "The epoch timestamp used for initial queries that use timestamp criteria. "
                    + "Use -1 to use the current time. If not specified, all data will be retrieved.";
    public static final String TIMESTAMP_INITIAL_DISPLAY = "Unix time value of initial timestamp";

    // Metadata Change Monitoring Interval (ms)
    public static final String TABLE_POLL_INTERVAL_MS_CONFIG = "table.poll.interval.ms";
    private static final String TABLE_POLL_INTERVAL_MS_DOC =
            "Frequency in ms to poll for new or removed tables, which may result in updated task "
                    + "configurations to start polling for data in added tables or stop polling for data in "
                    + "removed tables.";
    public static final long TABLE_POLL_INTERVAL_MS_DEFAULT = 60 * 1000;

    // table white list
    public static final String TABLE_WHITELIST_CONFIG = "table.whitelist";
    private static final String TABLE_WHITELIST_DOC =
            "List of tables to include in copying. If specified, ``table.blacklist`` may not be set. "
                    + "Use a comma-separated list to specify multiple tables "
                    + "(for example, ``table.whitelist: \"User, Address, Email\"``).";
    public static final String TABLE_WHITELIST_DEFAULT = "";

    // table black list
    public static final String TABLE_BLACKLIST_CONFIG = "table.blacklist";
    private static final String TABLE_BLACKLIST_DOC =
            "List of tables to exclude from copying. If specified, ``table.whitelist`` may not be set. "
                    + "Use a comma-separated list to specify multiple tables "
                    + "(for example, ``table.blacklist: \"User, Address, Email\"``).";
    public static final String TABLE_BLACKLIST_DEFAULT = "";
    private static final String TABLE_BLACKLIST_DISPLAY = "Table Blacklist";

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
    private static final String QUERY_DOC =
            "If specified, the query to perform to select new or updated rows. Use this setting if you "
                    + "want to join tables, select subsets of columns in a table, or filter data. If used, this"
                    + " connector will only copy data using this query -- whole-table copying will be disabled."
                    + " Different query modes may still be used for incremental updates, but in order to "
                    + "properly construct the incremental query, it must be possible to append a WHERE clause "
                    + "to this query (i.e. no WHERE clauses may be used). If you use a WHERE clause, it must "
                    + "handle incremental queries itself.";
    public static final String QUERY_DEFAULT = "";
    private static final String QUERY_DISPLAY = "Query";

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
    private Long timestampInitial;
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
        this.pollIntervalMs = config.getInt(POLL_INTERVAL_MS_CONFIG,POLL_INTERVAL_MS_DEFAULT);
        this.batchMaxRows = config.getInt(BATCH_MAX_ROWS_CONFIG,BATCH_MAX_ROWS_DEFAULT);
        this.numericPrecisionMapping = getBoolean(config,NUMERIC_PRECISION_MAPPING_CONFIG,NUMERIC_PRECISION_MAPPING_DEFAULT);
        this.numericMapping = config.getString(NUMERIC_MAPPING_CONFIG,NUMERIC_MAPPING_DEFAULT);
        this.dialectName = config.getString(DIALECT_NAME_CONFIG, DIALECT_NAME_DEFAULT );
        this.mode = config.getString(MODE_CONFIG);
        this.incrementingColumnName = config.getString(INCREMENTING_COLUMN_NAME_CONFIG);
        this.timestampColumnNames = getList(config,TIMESTAMP_COLUMN_NAME_CONFIG);
        timestampDelayIntervalMs=config.getLong(TIMESTAMP_DELAY_INTERVAL_MS_CONFIG);
        this.timestampInitial = config.containsKey(TIMESTAMP_INITIAL_CONFIG) ? config.getLong(TIMESTAMP_INITIAL_CONFIG) : TIMESTAMP_INITIAL_DEFAULT;
        this.tableWhitelist = new HashSet<>(getList(config,TABLE_WHITELIST_CONFIG));
        this.tableBlacklist = new HashSet<>(getList(config,TABLE_BLACKLIST_CONFIG));
        this.schemaPattern = config.getString(SCHEMA_PATTERN_CONFIG);
        this.catalogPattern = config.getString(CATALOG_PATTERN_CONFIG);
        this.query = config.getString(QUERY_CONFIG);
        this.topicPrefix = config.getString(TOPIC_PREFIX_CONFIG);
        this.validateNonNull = getBoolean(config,VALIDATE_NON_NULL_CONFIG,VALIDATE_NON_NULL_DEFAULT);
        tableTypes = TableType.parse(getList(config,TABLE_TYPE_CONFIG,TABLE_TYPE_DEFAULT)) ;
        String dbTimeZone =config.getString(DB_TIMEZONE_CONFIG,DB_TIMEZONE_DEFAULT);
        this.timeZone=TimeZone.getTimeZone(ZoneId.of(dbTimeZone));
        this.querySuffix=config.getString(QUERY_SUFFIX_CONFIG,QUERY_SUFFIX_DEFAULT);
        this.offsetSuffix=config.getString(OFFSET_SUFFIX_CONFIG,OFFSET_SUFFIX_DEFAULT);
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
