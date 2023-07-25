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
package org.apache.rocketmq.connect.jdbc.binder;

import io.openmessaging.connector.api.data.Schema;
import io.openmessaging.connector.api.data.logical.Date;
import io.openmessaging.connector.api.data.logical.Decimal;
import io.openmessaging.connector.api.data.logical.Time;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.ZoneId;
import java.util.Objects;
import java.util.TimeZone;
import org.apache.rocketmq.connect.jdbc.common.DebeziumTimeTypes;
import org.apache.rocketmq.connect.jdbc.schema.table.TableDefinition;
import org.apache.rocketmq.connect.jdbc.sink.JdbcSinkConfig;
import org.apache.rocketmq.connect.jdbc.sink.metadata.FieldsMetadata;
import org.apache.rocketmq.connect.jdbc.sink.metadata.SchemaPair;
import org.apache.rocketmq.connect.jdbc.util.DateTimeUtils;

/**
 * Jdbc record binder
 */
public class DefaultJdbcRecordBinder extends AbstractJdbcRecordBinder {

    private TimeZone timeZone = TimeZone.getTimeZone(ZoneId.of(JdbcSinkConfig.DB_TIMEZONE_DEFAULT));

    public DefaultJdbcRecordBinder(PreparedStatement statement, TableDefinition tableDefinition,
        FieldsMetadata fieldsMetadata,
        SchemaPair schemaPair, JdbcSinkConfig.PrimaryKeyMode pkMode, JdbcSinkConfig.InsertMode insertMode,
        TimeZone timeZone) {
        super(statement, tableDefinition, fieldsMetadata, schemaPair, pkMode, insertMode);
        if (Objects.nonNull(this.timeZone)) {
            this.timeZone = timeZone;
        }
    }

    @Override
    public void bindField(int index, Schema schema, Object value, String fieldName) throws SQLException {
        PreparedStatement statement = getPreparedStatement();
        if (Objects.isNull(value)) {
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

    protected boolean maybeBindLogical(PreparedStatement statement, int index, Schema schema,
        Object value) throws SQLException {
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

    /**
     * Dialects not supporting `setObject(index, null)` can override this method
     * to provide a specific sqlType, as per the JDBC documentation
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
}
