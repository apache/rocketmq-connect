package org.apache.rocketmq.connect.jdbc.dialect.impl;
import org.apache.rocketmq.connect.jdbc.config.AbstractConfig;
import org.apache.rocketmq.connect.jdbc.dialect.DatabaseDialect;
import org.apache.rocketmq.connect.jdbc.dialect.provider.DatabaseDialectProvider;
import org.apache.rocketmq.connect.jdbc.schema.column.ColumnId;
import org.apache.rocketmq.connect.jdbc.schema.table.TableId;
import org.apache.rocketmq.connect.jdbc.sink.metadata.SinkRecordField;
import org.apache.rocketmq.connect.jdbc.util.IdentifierRules;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;

/**
 *  ClickHouse Dialect
 */
public class ClickHouseDatabaseDialect extends GenericDatabaseDialect {

    private final static Logger log = LoggerFactory.getLogger(ClickHouseDatabaseDialect.class);

    /**
     * The provider for {@link ClickHouseDatabaseDialect}.
     */

    public static class Provider extends DatabaseDialectProvider {
        public Provider() throws ClassNotFoundException {
            super(ClickHouseDatabaseDialect.class.getSimpleName(), "clickhouse");
            Class.forName("com.clickhouse.jdbc.ClickHouseDriver");
        }

        @Override
        public DatabaseDialect create(AbstractConfig config) {
            return new ClickHouseDatabaseDialect(config);
        }
    }

    public ClickHouseDatabaseDialect(AbstractConfig config) {
        super(config, new IdentifierRules(".", "`", "`"));
    }

    @Override
    protected void initializePreparedStatement(PreparedStatement stmt) throws SQLException {
        stmt.setFetchSize(Integer.MIN_VALUE);
        log.trace("Initializing PreparedStatement fetch direction to FETCH_FORWARD for '{}'", stmt);
        stmt.setFetchDirection(ResultSet.FETCH_FORWARD);
    }


    // 在这里作clickhouse的数据字段类型转换
    @Override
    protected String getSqlType(SinkRecordField field) {
        switch (field.schemaType()) {
            case STRING:
                return "String";
            case INT8:
                return "Int8";
            case INT16:
                return "Int16";
            case INT32:
                return "Int32";
            case INT64:
                return "Int64";
            case FLOAT32:
                return "Float32";
            case FLOAT64:
                return "Float64";
            default:
                return super.getSqlType(field);
        }
    }

    @Override
    public String buildUpsertQueryStatement(TableId table,
                                            Collection<ColumnId> keyColumns,
                                            Collection<ColumnId> nonKeyColumns) {
        return super.buildInsertStatement(table, keyColumns, nonKeyColumns);
    }



    @Override
    public String buildInsertStatement(TableId table,
                                       Collection<ColumnId> keyColumns,
                                       Collection<ColumnId> nonKeyColumns) {
        return super.buildInsertStatement(table, keyColumns, nonKeyColumns);
    }
}
