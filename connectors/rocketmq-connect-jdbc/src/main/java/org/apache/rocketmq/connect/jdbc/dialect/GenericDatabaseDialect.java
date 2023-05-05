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
package org.apache.rocketmq.connect.jdbc.dialect;

import io.openmessaging.connector.api.data.FieldType;
import io.openmessaging.connector.api.errors.ConnectException;
import java.nio.ByteBuffer;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Queue;
import java.util.Set;
import java.util.TimeZone;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import org.apache.rocketmq.connect.jdbc.binder.DefaultJdbcRecordBinder;
import org.apache.rocketmq.connect.jdbc.binder.JdbcRecordBinder;
import org.apache.rocketmq.connect.jdbc.config.AbstractConfig;
import org.apache.rocketmq.connect.jdbc.converter.DefaultColumnConverter;
import org.apache.rocketmq.connect.jdbc.converter.JdbcColumnConverter;
import org.apache.rocketmq.connect.jdbc.util.ColumnDefAdjuster;
import org.apache.rocketmq.connect.jdbc.schema.column.ColumnDefinition;
import org.apache.rocketmq.connect.jdbc.schema.column.ColumnId;
import org.apache.rocketmq.connect.jdbc.schema.table.TableDefinition;
import org.apache.rocketmq.connect.jdbc.schema.table.TableId;
import org.apache.rocketmq.connect.jdbc.sink.JdbcSinkConfig;
import org.apache.rocketmq.connect.jdbc.sink.metadata.FieldsMetadata;
import org.apache.rocketmq.connect.jdbc.sink.metadata.SchemaPair;
import org.apache.rocketmq.connect.jdbc.sink.metadata.SinkRecordField;
import org.apache.rocketmq.connect.jdbc.source.JdbcSourceConfig;
import org.apache.rocketmq.connect.jdbc.source.TimestampIncrementingCriteria;
import org.apache.rocketmq.connect.jdbc.util.ExpressionBuilder;
import org.apache.rocketmq.connect.jdbc.util.IdentifierRules;
import org.apache.rocketmq.connect.jdbc.util.JdbcDriverInfo;
import org.apache.rocketmq.connect.jdbc.util.NumericMapping;
import org.apache.rocketmq.connect.jdbc.util.QuoteMethod;
import org.apache.rocketmq.connect.jdbc.util.TableType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * generic database dialect
 */
public abstract class GenericDatabaseDialect implements DatabaseDialect {
    private static final Logger log = LoggerFactory.getLogger(GenericDatabaseDialect.class);

    protected AbstractConfig config;
    /**
     * Whether to map {@code NUMERIC} JDBC types by precision.
     */
    protected NumericMapping mapNumerics = NumericMapping.NONE;
    protected String catalogPattern;
    protected String schemaPattern;
    protected Set<String> tableTypes;
    protected String jdbcUrl;
    private final QuoteMethod quoteSqlIdentifiers;
    private IdentifierRules defaultIdentifierRules;
    private AtomicReference<IdentifierRules> identifierRules = new AtomicReference<>();
    private Queue<Connection> connections = new ConcurrentLinkedQueue<>();
    private volatile JdbcDriverInfo jdbcDriverInfo;

    private TimeZone timeZone;

    public GenericDatabaseDialect(AbstractConfig config) {
        this(config, IdentifierRules.DEFAULT);
    }

    protected GenericDatabaseDialect(AbstractConfig config, IdentifierRules defaultIdentifierRules) {
        this.config = config;
        this.defaultIdentifierRules = defaultIdentifierRules;
        this.jdbcUrl = config.getConnectionDbUrl();
        if (config instanceof JdbcSinkConfig) {
            JdbcSinkConfig sinkConfig = (JdbcSinkConfig) config;
            catalogPattern = JdbcSourceConfig.CATALOG_PATTERN_DEFAULT;
            schemaPattern = JdbcSourceConfig.SCHEMA_PATTERN_DEFAULT;
            tableTypes = sinkConfig.tableTypeNames();
            quoteSqlIdentifiers = QuoteMethod.get(config.getQuoteSqlIdentifiers());
            mapNumerics = NumericMapping.NONE;
            timeZone = sinkConfig.getTimeZone();
        } else {
            JdbcSourceConfig sourceConfig = (JdbcSourceConfig) config;
            catalogPattern = sourceConfig.getCatalogPattern();
            schemaPattern = sourceConfig.getSchemaPattern();
            tableTypes = sourceConfig.getTableTypes().stream().map(TableType::toString).collect(Collectors.toSet());
            quoteSqlIdentifiers = QuoteMethod.get(config.getQuoteSqlIdentifiers());
            mapNumerics = sourceConfig.numericMapping();
            timeZone = sourceConfig.getTimeZone();
        }
    }

    /**
     * init jdbc connection
     *
     * @return
     * @throws SQLException
     */
    @Override
    public Connection getConnection() throws SQLException {
        String username = config.getConnectionDbUser();
        String dbPassword = config.getConnectionDbPassword();
        Properties properties = new Properties();
        if (username != null) {
            properties.setProperty("user", username);
        }
        if (dbPassword != null) {
            properties.setProperty("password", dbPassword);
        }
        properties = addConnectionProperties(properties);
        DriverManager.setLoginTimeout(40);
        Connection connection = DriverManager.getConnection(jdbcUrl, properties);
        // init jdbc driver info
        if (Objects.isNull(jdbcDriverInfo)){
            jdbcDriverInfo = createdJdbcDriverInfo(connection);
        }
        connections.add(connection);
        return connection;
    }

    /**
     * add connect properties
     *
     * @param properties
     * @return
     */
    protected Properties addConnectionProperties(Properties properties) {
        return properties;
    }

    @Override
    public void close() {
        Connection conn;
        while ((conn = connections.poll()) != null) {
            try {
                conn.close();
            } catch (Throwable e) {
                log.warn("Error while closing connection to {}", jdbcDriverInfo, e);
            }
        }
    }

    @Override
    public boolean isConnectionValid(Connection connection, int timeout) throws SQLException {
        if (jdbcDriverInfo.jdbcMajorVersion() >= 4) {
            return connection.isValid(timeout);
        }
        String query = checkConnectionQuery();
        if (query != null) {
            try (Statement statement = connection.createStatement()) {
                if (statement.execute(query)) {
                    ResultSet rs = null;
                    try {
                        rs = statement.getResultSet();
                    } finally {
                        if (rs != null) {
                            rs.close();
                        }
                    }
                }
            }
        }
        return true;
    }

    /**
     * Check connection query
     *
     * @return
     */
    protected String checkConnectionQuery() {
        return "SELECT 1";
    }

    /**
     * Get jdbc driver info
     * @return
     */
    protected JdbcDriverInfo createdJdbcDriverInfo(Connection connection) throws SQLException {
        DatabaseMetaData metadata = connection.getMetaData();
        this.jdbcDriverInfo = new JdbcDriverInfo(
            metadata.getJDBCMajorVersion(),
            metadata.getJDBCMinorVersion(),
            metadata.getDriverName(),
            metadata.getDatabaseProductName(),
            metadata.getDatabaseProductVersion()
        );
        return jdbcDriverInfo;
    }

    /**
     * catalog config
     * @return
     */
    protected String catalogPattern() {
        return catalogPattern;
    }
    /**
     * schema config
     * @return
     */
    protected String schemaPattern() {
        return schemaPattern;
    }

    @Override
    public List<TableId> listTableIds(Connection conn) throws SQLException {
        DatabaseMetaData metadata = conn.getMetaData();
        String[] tableTypes = tableTypes(metadata, this.tableTypes);
        String tableTypeDisplay = displayableTableTypes(tableTypes, ", ");
        log.debug("Using {} dialect to get {}", this, tableTypeDisplay);
        try (ResultSet rs = metadata.getTables(catalogPattern(), schemaPattern(), "%", tableTypes)) {
            List<TableId> tableIds = new ArrayList<>();
            while (rs.next()) {
                String catalogName = rs.getString(1);
                String schemaName = rs.getString(2);
                String tableName = rs.getString(3);
                TableId tableId = new TableId(catalogName, schemaName, tableName);
                if (includeTable(tableId)) {
                    tableIds.add(tableId);
                }
            }
            log.debug("Used {} dialect to find {} {}", this, tableIds.size(), tableTypeDisplay);
            return tableIds;
        }
    }


    /**
     * Check include table
     * @param table
     * @return
     */
    protected boolean includeTable(TableId table) {
        return true;
    }


    /**
     * Find the available table types that are returned by the JDBC driver that case insensitively
     * match the specified types.
     *
     * @param metadata
     * @param types
     * @return
     * @throws SQLException
     */
    protected String[] tableTypes(DatabaseMetaData metadata, Set<String> types) throws SQLException {
        log.debug("Using {} dialect to check support for {}", this, types);
        Set<String> uppercaseTypes = new HashSet<>();
        for (String type : types) {
            if (type != null) {
                uppercaseTypes.add(type.toUpperCase(Locale.ROOT));
            }
        }
        Set<String> matchingTableTypes = new HashSet<>();
        try (ResultSet rs = metadata.getTableTypes()) {
            while (rs.next()) {
                String tableType = rs.getString(1);
                if (tableType != null && uppercaseTypes.contains(tableType.toUpperCase(Locale.ROOT))) {
                    matchingTableTypes.add(tableType);
                }
            }
        }
        String[] result = matchingTableTypes.toArray(new String[matchingTableTypes.size()]);
        log.debug("Used {} dialect to find table types: {}", this, result);
        return result;
    }

    @Override
    public IdentifierRules identifierRules() {
        if (identifierRules.get() == null) {
            try (Connection connection = getConnection()) {
                DatabaseMetaData metaData = connection.getMetaData();
                String leadingQuoteStr = metaData.getIdentifierQuoteString();
                String trailingQuoteStr = leadingQuoteStr; // JDBC does not distinguish
                String separator = metaData.getCatalogSeparator();
                if (leadingQuoteStr == null || leadingQuoteStr.isEmpty()) {
                    leadingQuoteStr = defaultIdentifierRules.leadingQuoteString();
                    trailingQuoteStr = defaultIdentifierRules.trailingQuoteString();
                }
                if (separator == null || separator.isEmpty()) {
                    separator = defaultIdentifierRules.identifierDelimiter();
                }
                identifierRules.set(new IdentifierRules(separator, leadingQuoteStr, trailingQuoteStr));
            } catch (SQLException e) {
                if (defaultIdentifierRules != null) {
                    identifierRules.set(defaultIdentifierRules);
                    log.warn("Unable to get identifier metadata; using default rules", e);
                } else {
                    throw new io.openmessaging.connector.api.errors.ConnectException("Unable to get identifier metadata", e);
                }
            }
        }
        return identifierRules.get();
    }

    @Override
    public ExpressionBuilder expressionBuilder() {
        return identifierRules().expressionBuilder()
            .setQuoteIdentifiers(quoteSqlIdentifiers);
    }

    /**
     * Return current time at the database
     *
     * @param conn database connection
     * @param cal  calendar
     * @return the current time at the database
     */
    @Override
    public Timestamp currentTimeOnDB(
            Connection conn,
            Calendar cal
    ) throws SQLException {
        String query = currentTimestampDatabaseQuery();
        assert query != null;
        assert !query.isEmpty();
        try (Statement stmt = conn.createStatement()) {
            log.debug("executing query " + query + " to get current time from database");
            try (ResultSet rs = stmt.executeQuery(query)) {
                if (rs.next()) {
                    return rs.getTimestamp(1, cal);
                } else {
                    throw new io.openmessaging.connector.api.errors.ConnectException(
                            "Unable to get current time from DB using " + this + " and query '" + query + "'"
                    );
                }
            }
        } catch (SQLException e) {
            log.error("Failed to get current time from DB using {} and query '{}'", this, query, e);
            throw e;
        }
    }

    /**
     * Get the query string to determine the current timestamp in the database.
     *
     * @return the query string; never null or empty
     */
    protected String currentTimestampDatabaseQuery() {
        return "SELECT CURRENT_TIMESTAMP";
    }

    @Override
    public boolean tableExists(
            Connection connection,
            TableId tableId
    ) throws SQLException {
        DatabaseMetaData metadata = connection.getMetaData();
        String[] tableTypes = tableTypes(metadata, this.tableTypes);
        String tableTypeDisplay = displayableTableTypes(tableTypes, "/");
        log.info("Checking {} dialect for existence of {} {}", this, tableTypeDisplay, tableId);
        try (ResultSet rs = connection.getMetaData().getTables(
                tableId.catalogName(),
                tableId.schemaName(),
                tableId.tableName(),
                tableTypes
        )) {
            final boolean exists = rs.next();
            log.info(
                    "Using {} dialect {} {} {}",
                    this,
                    tableTypeDisplay,
                    tableId,
                    exists ? "present" : "absent"
            );
            return exists;
        }
    }

    protected String displayableTableTypes(String[] types, String delim) {
        return Arrays.stream(types).sorted().collect(Collectors.joining(delim));
    }

    @Override
    public Map<ColumnId, ColumnDefinition> describeColumns(
            Connection connection,
            String tablePattern,
            String columnPattern
    ) throws SQLException {
        //if the table pattern is fqn, then just use the actual table name
        TableId tableId = parseTableNameToTableId(tablePattern);
        String catalog = tableId.catalogName() != null ? tableId.catalogName() : catalogPattern;
        String schema = tableId.schemaName() != null ? tableId.schemaName() : schemaPattern;
        return describeColumns(connection, catalog, schema, tableId.tableName(), columnPattern);
    }

    @Override
    public Map<ColumnId, ColumnDefinition> describeColumns(
            Connection connection,
            String catalogPattern,
            String schemaPattern,
            String tablePattern,
            String columnPattern
    ) throws SQLException {
        log.debug(
                "Querying {} dialect column metadata for catalog:{} schema:{} table:{}",
                this,
                catalogPattern,
                schemaPattern,
                tablePattern);

        // Get the primary keys of the table(s) ...
        final Set<ColumnId> pkColumns = primaryKeyColumns(
                connection,
                catalogPattern,
                schemaPattern,
                tablePattern
        );
        Map<ColumnId, ColumnDefinition> results = new HashMap<>();
        try (ResultSet rs = connection.getMetaData().getColumns(
                catalogPattern,
                schemaPattern,
                tablePattern,
                columnPattern
        )) {
            final int rsColumnCount = rs.getMetaData().getColumnCount();
            while (rs.next()) {
                final String catalogName = rs.getString(1);
                final String schemaName = rs.getString(2);
                final String tableName = rs.getString(3);
                final TableId tableId = new TableId(catalogName, schemaName, tableName);
                final String columnName = rs.getString(4);
                final ColumnId columnId = new ColumnId(tableId, columnName, null);
                final int jdbcType = rs.getInt(5);
                final String typeName = rs.getString(6);
                final int precision = rs.getInt(7);
                final int scale = rs.getInt(9);
                final String typeClassName = null;
                ColumnDefinition.Nullability nullability;
                final int nullableValue = rs.getInt(11);
                switch (nullableValue) {
                    case DatabaseMetaData.columnNoNulls:
                        nullability = ColumnDefinition.Nullability.NOT_NULL;
                        break;
                    case DatabaseMetaData.columnNullable:
                        nullability = ColumnDefinition.Nullability.NULL;
                        break;
                    case DatabaseMetaData.columnNullableUnknown:
                    default:
                        nullability = ColumnDefinition.Nullability.UNKNOWN;
                        break;
                }
                Boolean autoIncremented = null;
                if (rsColumnCount >= 23) {
                    // Not all drivers include all columns ...
                    String isAutoIncremented = rs.getString(23);
                    if ("yes".equalsIgnoreCase(isAutoIncremented)) {
                        autoIncremented = Boolean.TRUE;
                    } else if ("no".equalsIgnoreCase(isAutoIncremented)) {
                        autoIncremented = Boolean.FALSE;
                    }
                }
                Boolean signed = null;
                Boolean caseSensitive = null;
                Boolean searchable = null;
                Boolean currency = null;
                Integer displaySize = null;
                boolean isPrimaryKey = pkColumns.contains(columnId);
                if (isPrimaryKey) {
                    // Some DBMSes report pks as null
                    nullability = ColumnDefinition.Nullability.NOT_NULL;
                }
                ColumnDefinition defn = columnDefinition(
                        rs,
                        columnId,
                        jdbcType,
                        typeName,
                        typeClassName,
                        nullability,
                        ColumnDefinition.Mutability.UNKNOWN,
                        precision,
                        scale,
                        signed,
                        displaySize,
                        autoIncremented,
                        caseSensitive,
                        searchable,
                        currency,
                        isPrimaryKey
                );
                results.put(columnId, defn);
            }
            return results;
        }
    }

    @Override
    public Map<ColumnId, ColumnDefinition> describeColumns(Connection conn,
                                                           TableId tableId, ResultSetMetaData rsMetadata) throws SQLException {
        ColumnDefAdjuster adjuster;
        if (tableId == null) {
            adjuster = new ColumnDefAdjuster();
        } else {
            String catalog = tableId.catalogName() != null ? tableId.catalogName() : catalogPattern;
            String schema = tableId.schemaName() != null ? tableId.schemaName() : schemaPattern;
            adjuster = ColumnDefAdjuster.create(
                    conn, catalog, schema, tableId.tableName(), null);
        }
        Map<ColumnId, ColumnDefinition> result = new LinkedHashMap<>();
        for (int i = 1; i <= rsMetadata.getColumnCount(); ++i) {
            ColumnDefinition defn = describeColumn(rsMetadata, adjuster, i);
            result.put(defn.id(), defn);
        }
        return result;
    }

    /**
     * Create a definition for the specified column in the result set.
     *
     * @param rsMetadata the result set metadata; may not be null
     * @param column     the column number, starting at 1 for the first column
     * @return the column definition; never null
     * @throws SQLException if there is an error accessing the result set metadata
     */
    protected ColumnDefinition describeColumn(
            ResultSetMetaData rsMetadata,
            ColumnDefAdjuster adjuster,
            int column
    ) throws SQLException {
        String catalog = rsMetadata.getCatalogName(column);
        String schema = rsMetadata.getSchemaName(column);
        String tableName = rsMetadata.getTableName(column);
        TableId tableId = new TableId(catalog, schema, tableName);
        String name = rsMetadata.getColumnName(column);
        String alias = rsMetadata.getColumnLabel(column);
        ColumnId id = new ColumnId(tableId, name, alias);

        ColumnDefinition.Nullability nullability = adjuster.nullable(name);
        if (nullability == null) {
            nullability = columnIsNullAble(rsMetadata, column);
        }

        ColumnDefinition.Mutability mutability = ColumnDefinition.Mutability.MAYBE_WRITABLE;
        if (rsMetadata.isReadOnly(column)) {
            mutability = ColumnDefinition.Mutability.READ_ONLY;
        } else if (rsMetadata.isWritable(column)) {
            mutability = ColumnDefinition.Mutability.MAYBE_WRITABLE;
        } else if (rsMetadata.isDefinitelyWritable(column)) {
            mutability = ColumnDefinition.Mutability.WRITABLE;
        }
        return new ColumnDefinition(
                id,
                rsMetadata.getColumnType(column),
                rsMetadata.getColumnTypeName(column),
                rsMetadata.getColumnClassName(column),
                nullability,
                mutability,
                rsMetadata.getPrecision(column),
                rsMetadata.getScale(column),
                rsMetadata.isSigned(column),
                rsMetadata.getColumnDisplaySize(column),
                rsMetadata.isAutoIncrement(column),
                false,
                rsMetadata.isSearchable(column),
                rsMetadata.isCurrency(column),
                false
        );
    }

    protected ColumnDefinition.Nullability columnIsNullAble(ResultSetMetaData rsMetadata, int column) throws SQLException {
        switch (rsMetadata.isNullable(column)) {
            case ResultSetMetaData.columnNullable:
                return ColumnDefinition.Nullability.NULL;
            case ResultSetMetaData.columnNoNulls:
                return ColumnDefinition.Nullability.NOT_NULL;
            case ResultSetMetaData.columnNullableUnknown:
            default:
                return ColumnDefinition.Nullability.UNKNOWN;
        }
    }

    protected Set<ColumnId> primaryKeyColumns(
            Connection connection,
            String catalogPattern,
            String schemaPattern,
            String tablePattern
    ) throws SQLException {

        // Get the primary keys of the table(s) ...
        final Set<ColumnId> pkColumns = new HashSet<>();
        try (ResultSet rs = connection.getMetaData().getPrimaryKeys(
                catalogPattern, schemaPattern, tablePattern)) {
            while (rs.next()) {
                String catalogName = rs.getString(1);
                String schemaName = rs.getString(2);
                String tableName = rs.getString(3);
                TableId tableId = new TableId(catalogName, schemaName, tableName);
                final String colName = rs.getString(4);
                ColumnId columnId = new ColumnId(tableId, colName);
                pkColumns.add(columnId);
            }
        }
        return pkColumns;
    }

    @Override
    public Map<ColumnId, ColumnDefinition> describeColumnsByQuerying(
            Connection db,
            TableId tableId
    ) throws SQLException {
        String queryStr = "SELECT * FROM {} LIMIT 1";
        String quotedName = expressionBuilder().append(tableId).toString();
        try (PreparedStatement stmt = db.prepareStatement(queryStr)) {
            stmt.setString(1, quotedName);
            try (ResultSet rs = stmt.executeQuery()) {
                ResultSetMetaData rsmd = rs.getMetaData();
                return describeColumns(db, tableId, rsmd);
            }
        }
    }


    @Override
    public TableDefinition describeTable(
            Connection connection,
            TableId tableId
    ) throws SQLException {
        Map<ColumnId, ColumnDefinition> columnDefns = describeColumns(connection, tableId.catalogName(),
                tableId.schemaName(),
                tableId.tableName(), null
        );
        if (columnDefns.isEmpty()) {
            return null;
        }
        TableType tableType = tableTypeFor(connection, tableId);
        return new TableDefinition(tableId, columnDefns.values(), tableType);
    }

    protected TableType tableTypeFor(
            Connection connection,
            TableId tableId
    ) throws SQLException {
        DatabaseMetaData metadata = connection.getMetaData();
        String[] tableTypes = tableTypes(metadata, this.tableTypes);
        String tableTypeDisplay = displayableTableTypes(tableTypes, "/");
        log.info("Checking {} dialect for type of {} {}", this, tableTypeDisplay, tableId);
        try (ResultSet rs = connection.getMetaData().getTables(
                tableId.catalogName(),
                tableId.schemaName(),
                tableId.tableName(),
                tableTypes
        )) {
            if (rs.next()) {
                final String tableType = rs.getString(4);
                try {
                    return TableType.get(tableType);
                } catch (IllegalArgumentException e) {
                    log.warn(
                            "{} dialect found unknown type '{}' for {} {}; using TABLE",
                            this,
                            tableType,
                            tableTypeDisplay,
                            tableId
                    );
                    return TableType.TABLE;
                }
            }
        }
        log.warn(
                "{} dialect did not find type for {} {}; using TABLE",
                this,
                tableTypeDisplay,
                tableId
        );
        return TableType.TABLE;
    }

    /**
     * column definition
     *
     * @return
     */
    protected ColumnDefinition columnDefinition(
            ResultSet resultSet,
            ColumnId id,
            int jdbcType,
            String typeName,
            String classNameForType,
            ColumnDefinition.Nullability nullability,
            ColumnDefinition.Mutability mutability,
            int precision,
            int scale,
            Boolean signedNumbers,
            Integer displaySize,
            Boolean autoIncremented,
            Boolean caseSensitive,
            Boolean searchable,
            Boolean currency,
            Boolean isPrimaryKey
    ) {
        return new ColumnDefinition(
                id,
                jdbcType,
                typeName,
                classNameForType,
                nullability,
                mutability,
                precision,
                scale,
                signedNumbers != null ? signedNumbers.booleanValue() : false,
                displaySize != null ? displaySize.intValue() : 0,
                autoIncremented != null ? autoIncremented.booleanValue() : false,
                caseSensitive != null ? caseSensitive.booleanValue() : false,
                searchable != null ? searchable.booleanValue() : false,
                currency != null ? currency.booleanValue() : false,
                isPrimaryKey != null ? isPrimaryKey.booleanValue() : false
        );
    }

    @Override
    public JdbcColumnConverter createJdbcColumnConverter() {
        return new DefaultColumnConverter(mapNumerics, jdbcDriverInfo.jdbcVersionAtLeast(4, 0), timeZone);
    }

    @Override
    public JdbcRecordBinder getJdbcRecordBinder(PreparedStatement statement, JdbcSinkConfig.PrimaryKeyMode pkMode,
        SchemaPair schemaPair, FieldsMetadata fieldsMetadata, TableDefinition tableDefinition,
        JdbcSinkConfig.InsertMode insertMode) {
        return new DefaultJdbcRecordBinder(statement, tableDefinition, fieldsMetadata, schemaPair, pkMode, insertMode, timeZone);
    }


    @Override
    public String getInsertSql(JdbcSinkConfig config, FieldsMetadata fieldsMetadata, TableId tableId) {
        switch (config.getInsertMode()) {
            case INSERT:
                return this.buildInsertStatement(
                        tableId,
                        asColumns(fieldsMetadata.keyFieldNames, tableId),
                        asColumns(fieldsMetadata.nonKeyFieldNames, tableId)
                );
            case UPSERT:
                if (fieldsMetadata.keyFieldNames.isEmpty()) {
                    throw new ConnectException(String.format(
                            "Write to table '%s' in UPSERT mode requires key field names to be known, check the"
                                    + " primary key configuration",
                            tableId
                    ));
                }
                try {
                    return this.buildUpsertQueryStatement(
                            tableId,
                            asColumns(fieldsMetadata.keyFieldNames, tableId),
                            asColumns(fieldsMetadata.nonKeyFieldNames, tableId)
                    );
                } catch (UnsupportedOperationException e) {
                    throw new ConnectException(String.format(
                            "Write to table '%s' in UPSERT mode is not supported with the %s dialect.",
                            tableId,
                        name()
                    ));
                }
            case UPDATE:
                return this.buildUpdateStatement(
                        tableId,
                        asColumns(fieldsMetadata.keyFieldNames, tableId),
                        asColumns(fieldsMetadata.nonKeyFieldNames, tableId)
                );
            default:
                throw new ConnectException("Invalid insert mode");
        }
    }

    @Override
    public String getDeleteSql(JdbcSinkConfig config, FieldsMetadata fieldsMetadata, TableId tableId) {
        String sql = null;
        if (config.isDeleteEnabled()) {
            switch (config.pkMode) {
                case RECORD_KEY:
                    if (fieldsMetadata.keyFieldNames.isEmpty()) {
                        throw new ConnectException("Require primary keys to support delete");
                    }
                    try {
                        sql = this.buildDeleteStatement(
                                tableId,
                                asColumns(fieldsMetadata.keyFieldNames, tableId)
                        );
                    } catch (UnsupportedOperationException e) {
                        throw new ConnectException(String.format(
                                "Deletes to table '%s' are not supported with the %s dialect.",
                                tableId,
                            name()
                        ));
                    }
                    break;
                default:
                    throw new ConnectException("Deletes are only supported for pk.mode record_key");
            }
        }
        return sql;
    }

    /**
     * create table statement
     *
     * @param table
     * @param fields
     * @return
     */
    @Override
    public String buildCreateTableStatement(
            TableId table,
            Collection<SinkRecordField> fields) {
        ExpressionBuilder builder = expressionBuilder();
        final List<String> pkFieldNames = extractPrimaryKeyFieldNames(fields);
        builder.append("CREATE TABLE ");
        builder.append(table);
        builder.append(" (");
        writeColumnsSpec(builder, fields);
        if (!pkFieldNames.isEmpty()) {
            builder.append(",");
            builder.append(System.lineSeparator());
            builder.append("PRIMARY KEY(");
            builder.appendList()
                    .delimitedBy(",")
                    .transformedBy(ExpressionBuilder.quote())
                    .of(pkFieldNames);
            builder.append(")");
        }
        builder.append(")");
        return builder.toString();
    }

    /**
     * alter table statement
     * @param table
     * @param fields
     * @return
     */
    @Override
    public List<String> buildAlterTable(
            TableId table,
            Collection<SinkRecordField> fields
    ) {
        final boolean newlines = fields.size() > 1;
        final ExpressionBuilder.Transform<SinkRecordField> transform = (builder, field) -> {
            if (newlines) {
                builder.appendNewLine();
            }
            builder.append("ADD ");
            writeColumnSpec(builder, field);
        };
        ExpressionBuilder builder = expressionBuilder();
        builder.append("ALTER TABLE ");
        builder.append(table);
        builder.append(" ");
        builder.appendList()
                .delimitedBy(",")
                .transformedBy(transform)
                .of(fields);
        return Collections.singletonList(builder.toString());
    }

    @Override
    public TimestampIncrementingCriteria criteriaFor(ColumnId incrementingColumn, List<ColumnId> timestampColumns) {
        return new TimestampIncrementingCriteria(
            incrementingColumn, timestampColumns, timeZone);
    }

    protected List<String> extractPrimaryKeyFieldNames(Collection<SinkRecordField> fields) {
        final List<String> pks = new ArrayList<>();
        for (SinkRecordField f : fields) {
            // todo add check column info
            if (f.isPrimaryKey()) {
                pks.add(f.name());
            }
        }
        return pks;
    }

    protected void writeColumnsSpec(
            ExpressionBuilder builder,
            Collection<SinkRecordField> fields
    ) {
        ExpressionBuilder.Transform<SinkRecordField> transform = (b, field) -> {
            b.append(System.lineSeparator());
            writeColumnSpec(b, field);
        };
        builder.appendList().delimitedBy(",").transformedBy(transform).of(fields);
    }

    protected void writeColumnSpec(
            ExpressionBuilder builder,
            SinkRecordField f
    ) {
        builder.appendColumnName(f.name());
        builder.append(" ");
        String sqlType = getSqlType(f);
        builder.append(sqlType);
        if (f.defaultValue() != null) {
            builder.append(" DEFAULT ");
            formatColumnValue(
                    builder,
                    f.schemaType(),
                    f.defaultValue()
            );
        } else if (isColumnOptional(f)) {
            builder.append(" NULL");
        } else {
            builder.append(" NOT NULL");
        }
    }

    protected boolean isColumnOptional(SinkRecordField field) {
        return field.isOptional();
    }

    protected void formatColumnValue(
            ExpressionBuilder builder,
            FieldType type,
            Object value) {
        switch (type) {
            case INT8:
            case INT32:
            case INT64:
            case FLOAT32:
            case FLOAT64:
                // no escaping required
                builder.append(value);
                break;
            case BOOLEAN:
                // 1 & 0 for boolean is more portable rather than TRUE/FALSE
                builder.append((Boolean) value ? '1' : '0');
                break;
            case STRING:
                builder.appendStringQuoted(value);
                break;
            case BYTES:
                final byte[] bytes;
                if (value instanceof ByteBuffer) {
                    final ByteBuffer buffer = ((ByteBuffer) value).slice();
                    bytes = new byte[buffer.remaining()];
                    buffer.get(bytes);
                } else {
                    bytes = (byte[]) value;
                }
                builder.appendBinaryLiteral(bytes);
                break;
            default:
                throw new io.openmessaging.connector.api.errors.ConnectException("Unsupported type for column value: " + type);
        }
    }

    protected String getSqlType(SinkRecordField f) {
        throw new io.openmessaging.connector.api.errors.ConnectException(String.format(
            "%s (%s) type doesn't have a mapping to the SQL database column type", f.schemaName(),
            f.schemaType()
        ));
    }

    private Collection<ColumnId> asColumns(Collection<String> names, TableId tableId) {
        return names.stream()
            .map(name -> new ColumnId(tableId, name))
            .collect(Collectors.toList());
    }

    @Override
    public String toString() {
        return name();
    }
}
