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
package org.apache.rocketmq.connect.jdbc.source;

import io.openmessaging.connector.api.data.Field;
import io.openmessaging.connector.api.data.Schema;
import io.openmessaging.connector.api.data.Struct;
import io.openmessaging.connector.api.errors.ConnectException;
import org.apache.rocketmq.connect.jdbc.schema.column.ColumnId;
import org.apache.rocketmq.connect.jdbc.source.offset.TimestampIncrementingOffset;
import org.apache.rocketmq.connect.jdbc.util.DateTimeUtils;
import org.apache.rocketmq.connect.jdbc.util.ExpressionBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Collections;
import java.util.List;
import java.util.TimeZone;
import java.util.stream.Collectors;

public class TimestampIncrementingCriteria {

    /**
     * The values that can be used in a statement's WHERE clause.
     */
    public interface CriteriaValues {

        /**
         * Get the beginning of the time period.
         *
         * @return the beginning timestamp; may be null
         * @throws SQLException if there is a problem accessing the value
         */
        Timestamp beginTimestampValue() throws SQLException;

        /**
         * Get the end of the time period.
         *
         * @return the ending timestamp; never null
         * @throws SQLException if there is a problem accessing the value
         */
        Timestamp endTimestampValue(Timestamp beginTime) throws SQLException;

        /**
         * Get the last incremented value seen.
         *
         * @return the last incremented value from one of the rows
         * @throws SQLException if there is a problem accessing the value
         */
        Long lastIncrementedValue() throws SQLException;
    }

    protected static final BigDecimal LONG_MAX_VALUE_AS_BIGDEC = new BigDecimal(Long.MAX_VALUE);

    protected final Logger log = LoggerFactory.getLogger(getClass());
    protected final List<ColumnId> timestampColumns;
    protected final ColumnId incrementingColumn;
    protected final TimeZone timeZone;

    public TimestampIncrementingCriteria(
            ColumnId incrementingColumn,
            List<ColumnId> timestampColumns,
            TimeZone timeZone
    ) {
        this.timestampColumns =
                timestampColumns != null ? timestampColumns : Collections.<ColumnId>emptyList();
        this.incrementingColumn = incrementingColumn;
        this.timeZone = timeZone;
    }

    protected boolean hasTimestampColumns() {
        return !timestampColumns.isEmpty();
    }

    protected boolean hasIncrementedColumn() {
        return incrementingColumn != null;
    }

    /**
     * Build the WHERE clause for the columns used in this criteria.
     *
     * @param builder the string builder to which the WHERE clause should be appended; never null
     */
    public void whereClause(ExpressionBuilder builder) {
        if (hasTimestampColumns() && hasIncrementedColumn()) {
            timestampIncrementingWhereClause(builder);
        } else if (hasTimestampColumns()) {
            timestampWhereClause(builder);
        } else if (hasIncrementedColumn()) {
            incrementingWhereClause(builder);
        }
    }

    /**
     * Set the query parameters on the prepared statement whose WHERE clause was generated with the
     * previous call to {@link #whereClause(ExpressionBuilder)}.
     *
     * @param stmt   the prepared statement; never null
     * @param values the values that can be used in the criteria parameters; never null
     * @throws SQLException if there is a problem using the prepared statement
     */
    public void setQueryParameters(
            PreparedStatement stmt,
            CriteriaValues values
    ) throws SQLException {
        if (hasTimestampColumns() && hasIncrementedColumn()) {
            setQueryParametersTimestampIncrementing(stmt, values);
        } else if (hasTimestampColumns()) {
            setQueryParametersTimestamp(stmt, values);
        } else if (hasIncrementedColumn()) {
            setQueryParametersIncrementing(stmt, values);
        }
    }

    protected void setQueryParametersTimestampIncrementing(
            PreparedStatement stmt,
            CriteriaValues values
    ) throws SQLException {
        Timestamp beginTime = values.beginTimestampValue();
        Timestamp endTime = values.endTimestampValue(beginTime);
        Long incOffset = values.lastIncrementedValue();
        stmt.setTimestamp(1, endTime, DateTimeUtils.getTimeZoneCalendar(timeZone));
        stmt.setTimestamp(2, beginTime, DateTimeUtils.getTimeZoneCalendar(timeZone));
        stmt.setLong(3, incOffset);
        stmt.setTimestamp(4, beginTime, DateTimeUtils.getTimeZoneCalendar(timeZone));
        log.debug(
                "Executing prepared statement with start time value = {} end time = {} and incrementing"
                        + " value = {}", DateTimeUtils.formatTimestamp(beginTime, timeZone),
                DateTimeUtils.formatTimestamp(endTime, timeZone), incOffset
        );
    }

    protected void setQueryParametersIncrementing(
            PreparedStatement stmt,
            CriteriaValues values
    ) throws SQLException {
        Long incOffset = values.lastIncrementedValue();
        stmt.setLong(1, incOffset);
        log.debug("Executing prepared statement with incrementing value = {}", incOffset);
    }

    protected void setQueryParametersTimestamp(
            PreparedStatement stmt,
            CriteriaValues values
    ) throws SQLException {
        Timestamp beginTime = values.beginTimestampValue();
        Timestamp endTime = values.endTimestampValue(beginTime);
        stmt.setTimestamp(1, beginTime, DateTimeUtils.getTimeZoneCalendar(timeZone));
        stmt.setTimestamp(2, endTime, DateTimeUtils.getTimeZoneCalendar(timeZone));
        log.debug("Executing prepared statement with timestamp value = {} end time = {}",
                DateTimeUtils.formatTimestamp(beginTime, timeZone),
                DateTimeUtils.formatTimestamp(endTime, timeZone)
        );
    }

    /**
     * Extract the offset values from the row.
     *
     * @param schema         the record's schema; never null
     * @param record         the record's struct; never null
     * @param previousOffset a previous timestamp offset if the table has timestamp columns
     * @return the timestamp for this row; may not be null
     */
    public TimestampIncrementingOffset extractValues(
            Schema schema,
            Struct record,
            TimestampIncrementingOffset previousOffset
    ) {
        Timestamp extractedTimestamp = null;
        if (hasTimestampColumns()) {
            extractedTimestamp = extractOffsetTimestamp(schema, record);
            assert previousOffset == null || (previousOffset.getTimestampOffset() != null
                    && previousOffset.getTimestampOffset().compareTo(
                    extractedTimestamp) <= 0
            );
        }
        Long extractedId = null;
        if (hasIncrementedColumn()) {
            extractedId = extractOffsetIncrementedId(schema, record);
            // If we are only using an incrementing column, then this must be incrementing.
            // If we are also using a timestamp, then we may see updates to older rows.
            assert previousOffset == null || previousOffset.getIncrementingOffset() == -1L
                    || extractedId > previousOffset.getIncrementingOffset() || hasTimestampColumns();
        }
        return new TimestampIncrementingOffset(extractedTimestamp, extractedId);
    }

    /**
     * Extract the timestamp from the row.
     *
     * @param schema the record's schema; never null
     * @param record the record's struct; never null
     * @return the timestamp for this row; may not be null
     */
    protected Timestamp extractOffsetTimestamp(
            Schema schema,
            Struct record
    ) {
        for (ColumnId timestampColumn : timestampColumns) {
            Field field = schema.getField(timestampColumn.name());
            Timestamp ts = (Timestamp) record.get(field);
            if (ts != null) {
                return ts;
            }
        }
        return null;
    }

    /**
     * Extract the incrementing column value from the row.
     *
     * @param schema the record's schema; never null
     * @param record the record's struct; never null
     * @return the incrementing ID for this row; may not be null
     */
    protected Long extractOffsetIncrementedId(
            Schema schema,
            Struct record
    ) {
        final Long extractedId;
        final Field field = schema.getField(incrementingColumn.name());
        if (field == null) {
            throw new ConnectException("Incrementing column " + incrementingColumn.name() + " not found in "
                    + schema.getFields().stream().map(f -> f.getName()).collect(Collectors.joining(",")));
        }

        final Schema incrementingColumnSchema = field.getSchema();
        final Object incrementingColumnValue = record.get(field);
        if (incrementingColumnValue == null) {
            throw new ConnectException(
                    "Null value for incrementing column of type: " + incrementingColumnSchema.getFieldType());
        } else if (isIntegralPrimitiveType(incrementingColumnValue)) {
            extractedId = ((Number) incrementingColumnValue).longValue();
        } else if (isLongFromString(incrementingColumnValue)) {
            extractedId = Long.parseLong((String) incrementingColumnValue);
        } else {
            throw new ConnectException(
                    "Invalid type for incrementing column: " + incrementingColumnSchema.getFieldType());
        }
        log.trace("Extracted incrementing column value: {}", extractedId);
        return extractedId;
    }

    protected boolean isLongFromString(Object incrementingColumnValue) {
        if (!(incrementingColumnValue instanceof String)) {
            return false;
        }
        String value = (String) incrementingColumnValue;
        try {
            Long.parseLong(value);
            return true;
        } catch (NumberFormatException e) {
            return false;
        }
    }

    protected boolean isIntegralPrimitiveType(Object incrementingColumnValue) {
        return incrementingColumnValue instanceof Long || incrementingColumnValue instanceof Integer
                || incrementingColumnValue instanceof Short || incrementingColumnValue instanceof Byte;
    }

    protected String coalesceTimestampColumns(ExpressionBuilder builder) {
        if (timestampColumns.size() == 1) {
            builder.append(timestampColumns.get(0));
        } else {
            builder.append("COALESCE(");
            builder.appendList().delimitedBy(",").of(timestampColumns);
            builder.append(")");
        }
        return builder.toString();
    }

    protected void timestampIncrementingWhereClause(ExpressionBuilder builder) {
        // This version combines two possible conditions. The first checks timestamp == last
        // timestamp and incrementing > last incrementing. The timestamp alone would include
        // duplicates, but adding the incrementing condition ensures no duplicates, e.g. you would
        // get only the row with id = 23:
        //  timestamp 1234, id 22 <- last
        //  timestamp 1234, id 23
        // The second check only uses the timestamp >= last timestamp. This covers everything new,
        // even if it is an update of the existing row. If we previously had:
        //  timestamp 1234, id 22 <- last
        // and then these rows were written:
        //  timestamp 1235, id 22
        //  timestamp 1236, id 23
        // We should capture both id = 22 (an update) and id = 23 (a new row)
        // now
        // where ts < ? AND (( ts = ? AND id > ? ) OR ts > ?) ORDER BY ts,id ASC
        // change to
        // where ts < ? AND (( ts = ? AND id > ? AND id <= ?) OR ts > ?) ORDER BY ts,id ASC
        builder.append(" WHERE ");
        coalesceTimestampColumns(builder);
        builder.append(" < ? AND ((");
        coalesceTimestampColumns(builder);
        builder.append(" = ? AND ");
        builder.append(incrementingColumn);
        builder.append(" > ?");
        builder.append(") OR ");
        coalesceTimestampColumns(builder);
        builder.append(" > ?)");
        builder.append(" ORDER BY ");
        coalesceTimestampColumns(builder);
        builder.append(",");
        builder.append(incrementingColumn);
        builder.append(" ASC");
    }

    // where id > ? order by id asc
    // where id > ? and id < ? order by id asc
    protected void incrementingWhereClause(ExpressionBuilder builder) {
        builder.append(" WHERE ");
        builder.append(incrementingColumn);
        builder.append(" > ?");
        builder.append(" ORDER BY ");
        builder.append(incrementingColumn);
        builder.append(" ASC");
    }

    protected void timestampWhereClause(ExpressionBuilder builder) {
        builder.append(" WHERE ");
        coalesceTimestampColumns(builder);
        builder.append(" > ? AND ");
        coalesceTimestampColumns(builder);
        builder.append(" < ? ORDER BY ");
        coalesceTimestampColumns(builder);
        builder.append(" ASC");
    }

}
