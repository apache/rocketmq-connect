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
package org.apache.rocketmq.connect.jdbc.dialect.impl;

import io.openmessaging.connector.api.data.FieldType;
import io.openmessaging.connector.api.data.Schema;
import io.openmessaging.connector.api.data.SchemaBuilder;
import io.openmessaging.connector.api.data.logical.Date;
import io.openmessaging.connector.api.data.logical.Decimal;
import io.openmessaging.connector.api.data.logical.Time;
import org.apache.rocketmq.connect.jdbc.common.DebeziumTimeTypes;
import org.apache.rocketmq.connect.jdbc.config.AbstractConfig;
import org.apache.rocketmq.connect.jdbc.connector.JdbcSinkConfig;
import org.apache.rocketmq.connect.jdbc.connector.JdbcSourceConfig;
import org.apache.rocketmq.connect.jdbc.dialect.DatabaseDialect;
import org.apache.rocketmq.connect.jdbc.dialect.DatabaseDialectFactory;
import org.apache.rocketmq.connect.jdbc.dialect.DropOptions;
import org.apache.rocketmq.connect.jdbc.dialect.PreparedStatementBinder;
import org.apache.rocketmq.connect.jdbc.dialect.provider.DatabaseDialectProvider;
import org.apache.rocketmq.connect.jdbc.dialect.provider.JdbcUrlInfo;
import org.apache.rocketmq.connect.jdbc.schema.column.ColumnDefAdjuster;
import org.apache.rocketmq.connect.jdbc.schema.column.ColumnDefinition;
import org.apache.rocketmq.connect.jdbc.schema.column.ColumnId;
import org.apache.rocketmq.connect.jdbc.schema.table.TableDefinition;
import org.apache.rocketmq.connect.jdbc.schema.table.TableId;
import org.apache.rocketmq.connect.jdbc.sink.metadata.FieldsMetadata;
import org.apache.rocketmq.connect.jdbc.sink.metadata.SchemaPair;
import org.apache.rocketmq.connect.jdbc.sink.metadata.SinkRecordField;
import org.apache.rocketmq.connect.jdbc.source.TimestampIncrementingCriteria;
import org.apache.rocketmq.connect.jdbc.source.metadata.ColumnMapping;
import org.apache.rocketmq.connect.jdbc.util.DateTimeUtils;
import org.apache.rocketmq.connect.jdbc.util.ExpressionBuilder;
import org.apache.rocketmq.connect.jdbc.util.IdentifierRules;
import org.apache.rocketmq.connect.jdbc.util.JdbcDriverInfo;
import org.apache.rocketmq.connect.jdbc.util.NumericMapping;
import org.apache.rocketmq.connect.jdbc.util.QuoteMethod;
import org.apache.rocketmq.connect.jdbc.util.TableType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.math.BigDecimal;
import java.net.URL;
import java.nio.ByteBuffer;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.sql.Statement;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.ZoneOffset;
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
import java.util.Properties;
import java.util.Queue;
import java.util.Set;
import java.util.TimeZone;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

/**
 * generic database dialect
 */
public class GenericDatabaseDialect implements DatabaseDialect {

    protected static final int NUMERIC_TYPE_SCALE_LOW = -84;
    protected static final int NUMERIC_TYPE_SCALE_HIGH = 127;
    protected static final int NUMERIC_TYPE_SCALE_UNSET = -127;

    private static final int MAX_INTEGER_TYPE_PRECISION = 18;

    /**
     * The provider for GenericDatabaseDialect
     */
    public static class Provider extends DatabaseDialectProvider {
        public Provider() {
            super(GenericDatabaseDialect.class.getSimpleName(), "");
        }

        @Override
        public DatabaseDialect create(AbstractConfig config) {
            return new GenericDatabaseDialect(config);
        }
    }

    /**
     * The provider for GenericDatabaseDialect
     */
    public static class DialectName {
        public static String generateDialectName(Class clazz) {
            return clazz.getSimpleName().replace("DatabaseDialect", "");
        }
    }

    private static final Logger log = LoggerFactory.getLogger(GenericDatabaseDialect.class);

    //    @Deprecated
//    protected final Logger log = LoggerFactory.getLogger(GenericDatabaseDialect.class);
    protected AbstractConfig config;
    /**
     * Whether to map {@code NUMERIC} JDBC types by precision.
     */
    protected NumericMapping mapNumerics = NumericMapping.NONE;
    protected String catalogPattern;
    protected String schemaPattern;
    protected Set<String> tableTypes;
    protected String jdbcUrl;
    protected JdbcUrlInfo jdbcUrlInfo;
    private QuoteMethod quoteSqlIdentifiers = QuoteMethod.ALWAYS;
    private IdentifierRules defaultIdentifierRules;
    private AtomicReference<IdentifierRules> identifierRules = new AtomicReference<>();
    private Queue<Connection> connections = new ConcurrentLinkedQueue<>();
    private volatile JdbcDriverInfo jdbcDriverInfo;
    private int batchMaxRows;
    private TimeZone timeZone;

    public GenericDatabaseDialect(AbstractConfig config) {
        this(config, IdentifierRules.DEFAULT);
    }

    protected GenericDatabaseDialect(
            AbstractConfig config, IdentifierRules defaultIdentifierRules
    ) {
        this.config = config;
        this.defaultIdentifierRules = defaultIdentifierRules;
        this.jdbcUrl = config.getConnectionDbUrl();
        this.jdbcUrlInfo = DatabaseDialectFactory.extractJdbcUrlInfo(jdbcUrl);
        if (config instanceof JdbcSinkConfig) {
            JdbcSinkConfig sinkConfig = (JdbcSinkConfig) config;
            catalogPattern = JdbcSourceConfig.CATALOG_PATTERN_DEFAULT;
            schemaPattern = JdbcSourceConfig.SCHEMA_PATTERN_DEFAULT;
            tableTypes = sinkConfig.tableTypeNames();
            quoteSqlIdentifiers = QuoteMethod.get(config.getQuoteSqlIdentifiers());
            mapNumerics = NumericMapping.NONE;
            batchMaxRows = 0;
            timeZone = sinkConfig.getTimeZone();
        } else {
            JdbcSourceConfig sourceConfig = (JdbcSourceConfig) config;
            catalogPattern = sourceConfig.getCatalogPattern();
            schemaPattern = sourceConfig.getSchemaPattern();
            tableTypes = sourceConfig.getTableTypes().stream().map(TableType::toString).collect(Collectors.toSet());
            quoteSqlIdentifiers = QuoteMethod.get(config.getQuoteSqlIdentifiers());
            mapNumerics = sourceConfig.numericMapping();
            batchMaxRows = sourceConfig.getBatchMaxRows();
            timeZone = sourceConfig.getTimeZone();
        }

    }

    @Override
    public String name() {
        return DialectName.generateDialectName(getClass());
//    return getClass().getSimpleName().replace("DatabaseDialect", "");
    }

    /**
     * init jdbc connection
     *
     * @return
     * @throws SQLException
     */
    @Override
    public Connection getConnection() throws SQLException {
        // These config names are the same for both source and sink configs ...
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
        if (jdbcDriverInfo == null) {
            jdbcDriverInfo = createJdbcDriverInfo(connection);
        }
        connections.add(connection);
        return connection;
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
    public boolean isConnectionValid(
            Connection connection,
            int timeout
    ) throws SQLException {
        if (jdbcDriverInfo().jdbcMajorVersion() >= 4) {
            return connection.isValid(timeout);
        }
        // issue a test query ...
        String query = checkConnectionQuery();
        if (query != null) {
            try (Statement statement = connection.createStatement()) {
                if (statement.execute(query)) {
                    ResultSet rs = null;
                    try {
                        // do nothing with the result set
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
     * check connection query
     *
     * @return the check connection query; may be null if the connection should not be queried
     */
    protected String checkConnectionQuery() {
        return "SELECT 1";
    }

    protected JdbcDriverInfo jdbcDriverInfo() {
        if (jdbcDriverInfo == null) {
            try (Connection connection = getConnection()) {
                jdbcDriverInfo = createJdbcDriverInfo(connection);
            } catch (SQLException e) {
                throw new io.openmessaging.connector.api.errors.ConnectException("Unable to get JDBC driver information", e);
            }
        }
        return jdbcDriverInfo;
    }

    protected JdbcDriverInfo createJdbcDriverInfo(Connection connection) throws SQLException {
        DatabaseMetaData metadata = connection.getMetaData();
        return new JdbcDriverInfo(
                metadata.getJDBCMajorVersion(),
                metadata.getJDBCMinorVersion(),
                metadata.getDriverName(),
                metadata.getDatabaseProductName(),
                metadata.getDatabaseProductVersion()
        );
    }

    /**
     * add connect properties
     *
     * @param properties
     * @return
     */
    protected Properties addConnectionProperties(Properties properties) {
        // do nothiong
        return properties;
    }

    @Override
    public PreparedStatement createPreparedStatement(
            Connection db,
            String query
    ) throws SQLException {
        log.trace("Creating a PreparedStatement '{}'", query);
        PreparedStatement stmt = db.prepareStatement(query);
        initializePreparedStatement(stmt);
        return stmt;
    }

    /**
     * init PreparedStatement
     *
     * @param stmt
     * @throws SQLException
     */
    protected void initializePreparedStatement(PreparedStatement stmt) throws SQLException {
        if (batchMaxRows > 0) {
            stmt.setFetchSize(batchMaxRows);
        }
    }

    @Override
    public TableId parseToTableId(String fqn) {
        List<String> parts = identifierRules().parseQualifiedIdentifier(fqn);
        if (parts.isEmpty()) {
            throw new IllegalArgumentException("Invalid fully qualified name: '" + fqn + "'");
        }
        if (parts.size() == 1) {
            return new TableId(null, null, parts.get(0));
        }
        if (parts.size() == 3) {
            return new TableId(parts.get(0), parts.get(1), parts.get(2));
        }
        assert parts.size() >= 2;
        if (useCatalog()) {
            return new TableId(parts.get(0), null, parts.get(1));
        }
        return new TableId(null, parts.get(0), parts.get(1));
    }

    /**
     * Return whether the database uses JDBC catalogs.
     *
     * @return true if catalogs are used, or false otherwise
     */
    protected boolean useCatalog() {
        return false;
    }

    @Override
    public List<TableId> tableIds(Connection conn) throws SQLException {
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

    protected String catalogPattern() {
        return catalogPattern;
    }

    protected String schemaPattern() {
        return schemaPattern;
    }

    /**
     * Determine whether the table with the specific name is to be included in the tables.
     *
     * <p>This method can be overridden to exclude certain database tables.
     *
     * @param table the identifier of the table; may be null
     * @return true if the table should be included; false otherwise
     */
    protected boolean includeTable(TableId table) {
        return true;
    }

    /**
     * Find the available table types that are returned by the JDBC driver that case insensitively
     * match the specified types.
     *
     * @param metadata the database metadata; may not be null but may be empty if no table types
     * @param types    the case-independent table types that are desired
     * @return the array of table types take directly from the list of available types returned by the
     * JDBC driver; never null
     * @throws SQLException if there is an error with the database connection
     */
    protected String[] tableTypes(
            DatabaseMetaData metadata,
            Set<String> types
    ) throws SQLException {
        log.debug("Using {} dialect to check support for {}", this, types);
        // Compute the uppercase form of the desired types ...
        Set<String> uppercaseTypes = new HashSet<>();
        for (String type : types) {
            if (type != null) {
                uppercaseTypes.add(type.toUpperCase(Locale.ROOT));
            }
        }
        // Now find out the available table types ...
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
            // Otherwise try to get the actual quote string and separator from the database, since
            // many databases allow them to be changed
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
        TableId tableId = parseToTableId(tablePattern);
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
                //final String catalogName = rs.getString(1);
                //final String schemaName = rs.getString(2);
                //final String tableName = rs.getString(3);
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
     * Create a ColumnDefinition with supplied values and the result set from the {@link
     * DatabaseMetaData#getColumns(String, String, String, String)} call. By default that method does
     * not describe whether the column is signed, case sensitive, searchable, currency, or the
     * preferred display size.
     *
     * <p>Subclasses can override this method to extract additional non-standard characteristics from
     * the result set, and override the characteristics determined using the standard JDBC metadata
     * columns and supplied as parameters.
     *
     * @param resultSet        the result set
     * @param id               the column identifier
     * @param jdbcType         the JDBC type of the column
     * @param typeName         the name of the column's type
     * @param classNameForType the name of the class used as instances of the value when {@link
     *                         ResultSet#getObject(int)} is called
     * @param nullability      the nullability of the column
     * @param mutability       the mutability of the column
     * @param precision        the precision of the column for numeric values, or the length for
     *                         non-numeric values
     * @param scale            the scale of the column for numeric values; ignored for other values
     * @param signedNumbers    true if the column holds signed numeric values; null if not known
     * @param displaySize      the preferred display size for the column values; null if not known
     * @param autoIncremented  true if the column is auto-incremented; null if not known
     * @param caseSensitive    true if the column values are case-sensitive; null if not known
     * @param searchable       true if the column is searchable; null if no; null if not known known
     * @param currency         true if the column is a currency value
     * @param isPrimaryKey     true if the column is part of the primary key; null if not known known
     * @return the column definition; never null
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

//  @Override
//  public TimestampIncrementingCriteria criteriaFor(
//          ColumnId incrementingColumn,
//          Long incrementingColumnStep,
//          List<ColumnId> timestampColumns,
//          Long timestampColumnStep
//  ) {
//    return new TimestampIncrementingCriteria(
//            incrementingColumn, incrementingColumnStep, timestampColumns,timestampColumnStep, timeZone);
//  }

    /**
     * Determine the name of the field. By default this is the column alias or name.
     *
     * @param columnDefinition the column definition; never null
     * @return the field name; never null
     */
    protected String fieldNameFor(ColumnDefinition columnDefinition) {
        return columnDefinition.id().aliasOrName();
    }


    @Override
    public String addFieldToSchema(
            ColumnDefinition columnDefn,
            SchemaBuilder schemaBuilder
    ) {
        return addFieldToSchema(columnDefn, schemaBuilder, fieldNameFor(columnDefn), columnDefn.type(),
                columnDefn.isOptional()
        );
    }

    /**
     * add field to schema
     *
     * @param columnDefn
     * @param builder
     * @param fieldName
     * @param sqlType
     * @param optional
     * @return
     */
    @SuppressWarnings("fallthrough")
    protected String addFieldToSchema(
            final ColumnDefinition columnDefn,
            final SchemaBuilder builder,
            final String fieldName,
            final int sqlType,
            final boolean optional
    ) {
        int precision = columnDefn.precision();
        int scale = columnDefn.scale();
        switch (sqlType) {
            case Types.NULL: {
                log.debug("JDBC type 'NULL' not currently supported for column '{}'", fieldName);
                return null;
            }
            case Types.BOOLEAN: {
                builder.field(fieldName, SchemaBuilder.bool().build());
                break;
            }

            // ints <= 8 bits
            case Types.BIT: {
                builder.field(fieldName, SchemaBuilder.int8().build());
                break;
            }

            case Types.TINYINT: {
                if (columnDefn.isSignedNumber()) {
                    builder.field(fieldName, SchemaBuilder.int8().build());
                } else {
                    builder.field(fieldName, SchemaBuilder.int32().build());
                }
                break;
            }

            // 16 bit ints
            case Types.SMALLINT: {
                builder.field(fieldName, SchemaBuilder.int32().build());
                break;
            }

            // 32 bit ints
            case Types.INTEGER: {
                if (columnDefn.isSignedNumber()) {
                    builder.field(fieldName, SchemaBuilder.int32().build());
                } else {
                    builder.field(fieldName, SchemaBuilder.int64().build());
                }
                break;
            }

            // 64 bit ints
            case Types.BIGINT: {
                builder.field(fieldName, SchemaBuilder.int64().build());
                break;
            }

            // REAL is a single precision floating point value, i.e. a Java float
            case Types.REAL: {
                builder.field(fieldName, SchemaBuilder.float32().build());
                break;
            }

            // FLOAT is, confusingly, double precision and effectively the same as DOUBLE. See REAL
            // for single precision
            case Types.FLOAT:
            case Types.DOUBLE:
                builder.field(fieldName, SchemaBuilder.float64().build());
                break;
            case Types.DECIMAL:
                scale = decimalScale(columnDefn);
                SchemaBuilder fieldBuilder = Decimal.builder(scale);
                if (optional) {
                    fieldBuilder.optional();
                }
                builder.field(fieldName, fieldBuilder.build());
                break;

            /**
             * numeric
             */
            case Types.NUMERIC:
                if (mapNumerics == NumericMapping.PRECISION_ONLY) {
                    log.debug("NUMERIC with precision: '{}' and scale: '{}'", precision, scale);
                    if (scale == 0 && precision <= MAX_INTEGER_TYPE_PRECISION) { // integer
                        builder.field(fieldName, integerSchema(optional, precision));
                        break;
                    }
                } else if (mapNumerics == NumericMapping.BEST_FIT) {
                    log.debug("NUMERIC with precision: '{}' and scale: '{}'", precision, scale);
                    if (precision <= MAX_INTEGER_TYPE_PRECISION) { // fits in primitive data types.
                        if (scale < 1 && scale >= NUMERIC_TYPE_SCALE_LOW) { // integer
                            builder.field(fieldName, integerSchema(optional, precision));
                            break;
                        } else if (scale > 0) { // floating point - use double in all cases
                            builder.field(fieldName, SchemaBuilder.float64().build());
                            break;
                        }
                    }
                } else if (mapNumerics == NumericMapping.BEST_FIT_EAGER_DOUBLE) {
                    log.debug("NUMERIC with precision: '{}' and scale: '{}'", precision, scale);
                    if (scale < 1 && scale >= NUMERIC_TYPE_SCALE_LOW) { // integer
                        if (precision <= MAX_INTEGER_TYPE_PRECISION) { // fits in primitive data types.
                            builder.field(fieldName, integerSchema(optional, precision));
                            break;
                        }
                    } else if (scale > 0) { // floating point - use double in all cases
                        builder.field(fieldName, SchemaBuilder.float64().build());
                        break;
                    }
                }

            case Types.CHAR:
            case Types.VARCHAR:
            case Types.LONGVARCHAR:
            case Types.NCHAR:
            case Types.NVARCHAR:
            case Types.LONGNVARCHAR:
            case Types.CLOB:
            case Types.NCLOB:
            case Types.DATALINK:
            case Types.SQLXML: {
                // Some of these types will have fixed size, but we drop this from the schema conversion
                // since only fixed byte arrays can have a fixed size
                builder.field(fieldName, SchemaBuilder.string().build());
                break;
            }

            // Binary == fixed bytes
            // BLOB, VARBINARY, LONGVARBINARY == bytes
            case Types.BINARY:
            case Types.BLOB:
            case Types.VARBINARY:
            case Types.LONGVARBINARY: {
                builder.field(fieldName, SchemaBuilder.bytes().build());
                break;
            }

            // Date is day + moth + year
            case Types.DATE: {
                SchemaBuilder dateSchemaBuilder = Date.builder();
                builder.field(fieldName, dateSchemaBuilder.build());
                break;
            }

            // Time is a time of day -- hour, minute, seconds, nanoseconds
            case Types.TIME: {
                SchemaBuilder timeSchemaBuilder = Time.builder();
                builder.field(fieldName, timeSchemaBuilder.build());
                break;
            }

            // Timestamp is a date + time
            case Types.TIMESTAMP: {
                SchemaBuilder tsSchemaBuilder = io.openmessaging.connector.api.data.logical.Timestamp.builder();
                builder.field(fieldName, tsSchemaBuilder.build());
                break;
            }

            case Types.ARRAY:
            case Types.JAVA_OBJECT:
            case Types.OTHER:
            case Types.DISTINCT:
            case Types.STRUCT:
            case Types.REF:
            case Types.ROWID:
            default: {
                log.warn("JDBC type {} ({}) not currently supported", sqlType, columnDefn.typeName());
                return null;
            }
        }
        return fieldName;
    }

    private Schema integerSchema(boolean optional, int precision) {
        Schema schema;
        if (precision > 9) {
            schema = SchemaBuilder.int64().build();
        } else if (precision > 2) {
            schema = SchemaBuilder.int32().build();
        } else {
            schema = SchemaBuilder.int8().build();
        }
        return schema;
    }

    /**
     * execute ddl
     *
     * @param connection the connection to use
     * @param statements the list of DDL statements to execute
     * @throws SQLException
     */
    @Override
    public void applyDdlStatements(
            Connection connection,
            List<String> statements
    ) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            for (String ddlStatement : statements) {
                statement.executeUpdate(ddlStatement);
            }
        }
    }

    @Override
    public ColumnConverter createColumnConverter(
            ColumnMapping mapping
    ) {
        return columnConverterFor(
                mapping,
                mapping.columnDefn(),
                mapping.columnNumber(),
                jdbcDriverInfo().jdbcVersionAtLeast(4, 0)
        );
    }

    @SuppressWarnings({"deprecation", "fallthrough"})
    protected ColumnConverter columnConverterFor(
            final ColumnMapping mapping,
            final ColumnDefinition defn,
            final int col,
            final boolean isJdbc4
    ) {
        switch (mapping.columnDefn().type()) {

            case Types.BOOLEAN: {
                return rs -> rs.getBoolean(col);
            }

            case Types.BIT: {
                /**
                 * BIT should be either 0 or 1.
                 * TODO: Postgres handles this differently, returning a string "t" or "f". See the
                 * elasticsearch-jdbc plugin for an example of how this is handled
                 */
                return rs -> rs.getByte(col);
            }

            // 8 bits int
            case Types.TINYINT: {
                if (defn.isSignedNumber()) {
                    return rs -> rs.getByte(col);
                } else {
                    return rs -> rs.getShort(col);
                }
            }

            // 16 bits int
            case Types.SMALLINT: {
                if (defn.isSignedNumber()) {
                    return rs -> rs.getShort(col);
                } else {
                    return rs -> rs.getInt(col);
                }
            }

            // 32 bits int
            case Types.INTEGER: {
                if (defn.isSignedNumber()) {
                    return rs -> rs.getInt(col);
                } else {
                    return rs -> rs.getLong(col);
                }
            }

            // 64 bits int
            case Types.BIGINT: {
                return rs -> rs.getLong(col);
            }

            // REAL is a single precision floating point value, i.e. a Java float
            case Types.REAL: {
                return rs -> rs.getFloat(col);
            }

            // FLOAT is, confusingly, double precision and effectively the same as DOUBLE. See REAL
            // for single precision
            case Types.FLOAT:
            case Types.DOUBLE: {
                return rs -> rs.getDouble(col);
            }

            case Types.NUMERIC:
                if (mapNumerics == NumericMapping.PRECISION_ONLY) {
                    int precision = defn.precision();
                    int scale = defn.scale();
                    log.trace("NUMERIC with precision: '{}' and scale: '{}'", precision, scale);
                    if (scale == 0 && precision <= MAX_INTEGER_TYPE_PRECISION) { // integer
                        if (precision > 9) {
                            return rs -> rs.getLong(col);
                        } else if (precision > 4) {
                            return rs -> rs.getInt(col);
                        } else if (precision > 2) {
                            return rs -> rs.getShort(col);
                        } else {
                            return rs -> rs.getByte(col);
                        }
                    }
                } else if (mapNumerics == NumericMapping.BEST_FIT) {
                    int precision = defn.precision();
                    int scale = defn.scale();
                    log.trace("NUMERIC with precision: '{}' and scale: '{}'", precision, scale);
                    if (precision <= MAX_INTEGER_TYPE_PRECISION) { // fits in primitive data types.
                        if (scale < 1 && scale >= NUMERIC_TYPE_SCALE_LOW) { // integer
                            if (precision > 9) {
                                return rs -> rs.getLong(col);
                            } else if (precision > 4) {
                                return rs -> rs.getInt(col);
                            } else if (precision > 2) {
                                return rs -> rs.getShort(col);
                            } else {
                                return rs -> rs.getByte(col);
                            }
                        } else if (scale > 0) { // floating point - use double in all cases
                            return rs -> rs.getDouble(col);
                        }
                    }
                } else if (mapNumerics == NumericMapping.BEST_FIT_EAGER_DOUBLE) {
                    int precision = defn.precision();
                    int scale = defn.scale();
                    log.trace("NUMERIC with precision: '{}' and scale: '{}'", precision, scale);
                    if (scale < 1 && scale >= NUMERIC_TYPE_SCALE_LOW) { // integer
                        if (precision <= MAX_INTEGER_TYPE_PRECISION) { // fits in primitive data types.
                            if (precision > 9) {
                                return rs -> rs.getLong(col);
                            } else if (precision > 4) {
                                return rs -> rs.getInt(col);
                            } else if (precision > 2) {
                                return rs -> rs.getShort(col);
                            } else {
                                return rs -> rs.getByte(col);
                            }
                        }
                    } else if (scale > 0) { // floating point - use double in all cases
                        return rs -> rs.getDouble(col);
                    }
                }
                // fallthrough

            case Types.DECIMAL: {
                final int precision = defn.precision();
                log.debug("DECIMAL with precision: '{}' and scale: '{}'", precision, defn.scale());
                final int scale = decimalScale(defn);
                return rs -> rs.getBigDecimal(col, scale);
            }

            case Types.CHAR:
            case Types.VARCHAR:
            case Types.LONGVARCHAR: {
                return rs -> rs.getString(col);
            }

            case Types.NCHAR:
            case Types.NVARCHAR:
            case Types.LONGNVARCHAR: {
                return rs -> rs.getNString(col);
            }

            // Binary == fixed, VARBINARY and LONGVARBINARY == bytes
            case Types.BINARY:
            case Types.VARBINARY:
            case Types.LONGVARBINARY: {
                return rs -> rs.getBytes(col);
            }

            // Date is day + month + year
            case Types.DATE: {
                return rs -> rs.getDate(col,
                        DateTimeUtils.getTimeZoneCalendar(TimeZone.getTimeZone(ZoneOffset.UTC)));
            }

            // Time is a time of day -- hour, minute, seconds, nanoseconds
            case Types.TIME: {
                return rs -> rs.getTime(col, DateTimeUtils.getTimeZoneCalendar(timeZone));
            }

            // Timestamp is a date + time
            case Types.TIMESTAMP: {
                return rs -> rs.getTimestamp(col, DateTimeUtils.getTimeZoneCalendar(timeZone));
            }

            // Datalink is basically a URL -> string
            case Types.DATALINK: {
                return rs -> {
                    URL url = rs.getURL(col);
                    return url != null ? url.toString() : null;
                };
            }

            // BLOB == fixed
            case Types.BLOB: {
                return rs -> {
                    Blob blob = rs.getBlob(col);
                    if (blob == null) {
                        return null;
                    } else {
                        try {
                            if (blob.length() > Integer.MAX_VALUE) {
                                throw new IOException("Can't process BLOBs longer than " + Integer.MAX_VALUE);
                            }
                            return blob.getBytes(1, (int) blob.length());
                        } finally {
                            if (isJdbc4) {
                                free(blob);
                            }
                        }
                    }
                };
            }
            case Types.CLOB:
                return rs -> {
                    Clob clob = rs.getClob(col);
                    if (clob == null) {
                        return null;
                    } else {
                        try {
                            if (clob.length() > Integer.MAX_VALUE) {
                                throw new IOException("Can't process CLOBs longer than " + Integer.MAX_VALUE);
                            }
                            return clob.getSubString(1, (int) clob.length());
                        } finally {
                            if (isJdbc4) {
                                free(clob);
                            }
                        }
                    }
                };
            case Types.NCLOB: {
                return rs -> {
                    Clob clob = rs.getNClob(col);
                    if (clob == null) {
                        return null;
                    } else {
                        try {
                            if (clob.length() > Integer.MAX_VALUE) {
                                throw new IOException("Can't process NCLOBs longer than " + Integer.MAX_VALUE);
                            }
                            return clob.getSubString(1, (int) clob.length());
                        } finally {
                            if (isJdbc4) {
                                free(clob);
                            }
                        }
                    }
                };
            }

            // XML -> string
            case Types.SQLXML: {
                return rs -> {
                    SQLXML xml = rs.getSQLXML(col);
                    return xml != null ? xml.getString() : null;
                };
            }

            case Types.NULL:
            case Types.ARRAY:
            case Types.JAVA_OBJECT:
            case Types.OTHER:
            case Types.DISTINCT:
            case Types.STRUCT:
            case Types.REF:
            case Types.ROWID:
            default: {
                // These are not currently supported, but we don't want to log something for every single
                // record we translate. There will already be errors logged for the schema translation
                break;
            }
        }
        return null;
    }

    protected int decimalScale(ColumnDefinition defn) {
        return defn.scale() == NUMERIC_TYPE_SCALE_UNSET ? NUMERIC_TYPE_SCALE_HIGH : defn.scale();
    }

    /**
     * Called when the object has been fully read and {@link Blob#free()} should be called.
     *
     * @param blob the Blob; never null
     * @throws SQLException if there is a problem calling free()
     */
    protected void free(Blob blob) throws SQLException {
        blob.free();
    }

    /**
     * Called when the object has been fully read and {@link Clob#free()} should be called.
     *
     * @param clob the Clob; never null
     * @throws SQLException if there is a problem calling free()
     */
    protected void free(Clob clob) throws SQLException {
        clob.free();
    }

    @Override
    public String buildInsertStatement(
            TableId table,
            Collection<ColumnId> keyColumns,
            Collection<ColumnId> nonKeyColumns
    ) {
        ExpressionBuilder builder = expressionBuilder();
        builder.append("INSERT INTO ");
        builder.append(table);
        builder.append("(");
        builder.appendList()
                .delimitedBy(",")
                .transformedBy(ExpressionBuilder.columnNames())
                .of(keyColumns, nonKeyColumns);
        builder.append(") VALUES(");
        builder.appendMultiple(",", "?", keyColumns.size() + nonKeyColumns.size());
        builder.append(")");
        return builder.toString();
    }

    @Override
    public String buildUpdateStatement(
            TableId table,
            Collection<ColumnId> keyColumns,
            Collection<ColumnId> nonKeyColumns
    ) {
        ExpressionBuilder builder = expressionBuilder();
        builder.append("UPDATE ");
        builder.append(table);
        builder.append(" SET ");
        builder.appendList()
                .delimitedBy(", ")
                .transformedBy(ExpressionBuilder.columnNamesWith(" = ?"))
                .of(nonKeyColumns);
        if (!keyColumns.isEmpty()) {
            builder.append(" WHERE ");
            builder.appendList()
                    .delimitedBy(" AND ")
                    .transformedBy(ExpressionBuilder.columnNamesWith(" = ?"))
                    .of(keyColumns);
        }
        return builder.toString();
    }

    @Override
    public String buildUpsertQueryStatement(
            TableId table,
            Collection<ColumnId> keyColumns,
            Collection<ColumnId> nonKeyColumns
    ) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String buildDeleteStatement(
            TableId table,
            Collection<ColumnId> keyColumns
    ) {
        ExpressionBuilder builder = expressionBuilder();
        builder.append("DELETE FROM ");
        builder.append(table);
        if (!keyColumns.isEmpty()) {
            builder.append(" WHERE ");
            builder.appendList()
                    .delimitedBy(" AND ")
                    .transformedBy(ExpressionBuilder.columnNamesWith(" = ?"))
                    .of(keyColumns);
        }
        return builder.toString();
    }

    /**
     * table moode
     *
     * @return
     */
    @Override
    public String buildSelectTableMode() {
        return "SELECT * FROM ";
    }

    @Override
    public void buildSelectTable(ExpressionBuilder builder, TableId tableId) {
        String mode = buildSelectTableMode();
        builder.append(mode).append(tableId);
    }

    @Override
    public StatementBinder statementBinder(
            PreparedStatement statement,
            JdbcSinkConfig.PrimaryKeyMode pkMode,
            SchemaPair schemaPair,
            FieldsMetadata fieldsMetadata,
            TableDefinition tableDefinition,
            JdbcSinkConfig.InsertMode insertMode) {
        return new PreparedStatementBinder(
                this,
                statement,
                pkMode,
                schemaPair,
                fieldsMetadata,
                tableDefinition,
                insertMode
        );
    }

    @Override
    public void bindField(
            PreparedStatement statement,
            int index, Schema schema,
            Object value,
            ColumnDefinition colDef) throws SQLException {
        if (value == null) {
            Integer type = getSqlTypeForSchema(schema);
            if (type != null) {
                statement.setNull(index, type);
            } else {
                statement.setObject(index, null);
            }
        } else {
            boolean bound = maybeBindLogical(statement, index, schema, value);
            if (!bound) {
                bound = maybeBindDebeziumLogical(statement, index, schema, value);
            }
            if (!bound) {
                bound = maybeBindPrimitive(statement, index, schema, value);
            }
            if (!bound) {
                throw new io.openmessaging.connector.api.errors.ConnectException("Unsupported source data type: " + schema.getFieldType());
            }
        }
    }

    protected boolean maybeBindLogical(
            PreparedStatement statement,
            int index,
            Schema schema,
            Object value
    ) throws SQLException {
        if (schema.getName() != null) {
            switch (schema.getName()) {
                case Decimal.LOGICAL_NAME:
                    statement.setBigDecimal(index, (BigDecimal) value);
                    return true;
                case Date.LOGICAL_NAME:
                    java.sql.Date date;
                    if (value instanceof java.util.Date) {
                        date = new java.sql.Date(((java.util.Date) value).getTime());
                    } else {
                        date = new java.sql.Date((int) value);
                    }
                    statement.setDate(
                            index, date,
                            DateTimeUtils.getTimeZoneCalendar(timeZone)
                    );
                    return true;
                case Time.LOGICAL_NAME:
                    java.sql.Time time;
                    if (value instanceof java.util.Date) {
                        time = new java.sql.Time(((java.util.Date) value).getTime());
                    } else {
                        time = new java.sql.Time((int) value);
                    }
                    statement.setTime(
                            index, time,
                            DateTimeUtils.getTimeZoneCalendar(timeZone)
                    );
                    return true;
                case io.openmessaging.connector.api.data.logical.Timestamp.LOGICAL_NAME:
                    Timestamp timestamp;
                    if (value instanceof java.util.Date) {
                        timestamp = new Timestamp(((java.util.Date) value).getTime());
                    } else {
                        timestamp = new Timestamp((long) value);
                    }
                    statement.setTimestamp(
                            index, timestamp,
                            DateTimeUtils.getTimeZoneCalendar(timeZone)
                    );
                    return true;
                default:
                    return false;
            }
        }
        return false;
    }


    @Override
    public TimestampIncrementingCriteria criteriaFor(ColumnId incrementingColumn, List<ColumnId> timestampColumns) {
        return new TimestampIncrementingCriteria(
                incrementingColumn, timestampColumns, timeZone);
    }

    @Override
    public Long getMinTimestampValue(Connection con, String tableOrQuery, List<String> timestampColumns) throws SQLException {
        if (timestampColumns == null || timestampColumns.isEmpty()) {
            return null;
        }
        StringBuilder builder = new StringBuilder();
        builder.append("SELECT ");
        boolean appendComma = false;
        for (String column : timestampColumns) {
            builder.append("MIN(");
            builder.append(column);
            builder.append(")");
            if (appendComma) {
                builder.append(",");
            } else {
                appendComma = true;
            }
        }
        builder.append(" FROM ");
        builder.append(tableOrQuery);
        String querySql = builder.toString();
        PreparedStatement st = con.prepareStatement(querySql);
        ResultSet resultSet = st.executeQuery();
        ResultSetMetaData metaData = resultSet.getMetaData();
        long minTimestampValue = Long.MAX_VALUE;
        for (int i = 1; i <= metaData.getColumnCount(); ++i) {
            long t = resultSet.getLong(i);
            minTimestampValue = Math.min(minTimestampValue, t);
        }
        st.close();
        return minTimestampValue;
    }

    /**
     * Dialects not supporting `setObject(index, null)` can override this method
     * to provide a specific sqlType, as per the JDBC documentation
     * https://docs.oracle.com/javase/7/docs/api/java/sql/PreparedStatement.html
     *
     * @param schema the schema
     * @return the SQL type
     */
    protected Integer getSqlTypeForSchema(Schema schema) {
        return null;
    }

    protected boolean maybeBindDebeziumLogical(
            PreparedStatement statement,
            int index,
            Schema schema,
            Object value
    ) throws SQLException {
       return DebeziumTimeTypes.maybeBindDebeziumLogical(statement, index, schema, value, timeZone);
    }
    protected boolean maybeBindPrimitive(
            PreparedStatement statement,
            int index,
            Schema schema,
            Object value
    ) throws SQLException {
        switch (schema.getFieldType()) {
            case INT8:
                statement.setByte(index, Byte.parseByte(value.toString()));
                break;
            case INT32:
                statement.setInt(index, Integer.parseInt(value.toString()));
                break;
            case INT64:
                statement.setLong(index, Long.parseLong(value.toString()));
                break;
            case FLOAT32:
                statement.setFloat(index, Float.parseFloat(value.toString()));
                break;
            case FLOAT64:
                statement.setDouble(index, Double.parseDouble(value.toString()));
                break;
            case BOOLEAN:
                statement.setBoolean(index, Boolean.parseBoolean(value.toString()));
                break;
            case STRING:
                statement.setString(index, (String) value);
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
                statement.setBytes(index, bytes);
                break;
            case DATETIME:
                java.sql.Date date;
                if (value instanceof java.util.Date) {
                    date = new java.sql.Date(((java.util.Date) value).getTime());
                } else {
                    date = new java.sql.Date((int) value);
                }
                statement.setDate(
                        index, date,
                        DateTimeUtils.getTimeZoneCalendar(timeZone)
                );


                break;
            default:
                return false;
        }
        return true;
    }

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

    @Override
    public String buildDropTableStatement(
            TableId table,
            DropOptions options
    ) {
        ExpressionBuilder builder = expressionBuilder();

        builder.append("DROP TABLE ");
        builder.append(table);
        if (options.ifExists()) {
            builder.append(" IF EXISTS");
        }
        if (options.cascade()) {
            builder.append(" CASCADE");
        }
        return builder.toString();
    }

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
    public void validateColumnTypes(
            ResultSetMetaData rsMetadata,
            List<ColumnId> columns
    ) throws io.openmessaging.connector.api.errors.ConnectException {
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


    /**
     * @param url
     * @return
     */
    protected String sanitizedUrl(String url) {
        // Only replace standard URL-type properties ...
        return url.replaceAll("(?i)([?&]([^=&]*)password([^=&]*)=)[^&]*", "$1****");
    }

    @Override
    public String identifier() {
        return name() + " database " + sanitizedUrl(jdbcUrl);
    }

    @Override
    public String toString() {
        return name();
    }
}
