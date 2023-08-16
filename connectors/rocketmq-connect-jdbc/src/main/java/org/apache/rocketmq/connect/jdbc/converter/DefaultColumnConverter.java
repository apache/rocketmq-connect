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

package org.apache.rocketmq.connect.jdbc.converter;

import io.openmessaging.connector.api.data.Schema;
import io.openmessaging.connector.api.data.SchemaBuilder;
import io.openmessaging.connector.api.data.logical.Date;
import io.openmessaging.connector.api.data.logical.Decimal;
import io.openmessaging.connector.api.data.logical.Time;
import java.io.IOException;
import java.net.URL;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.sql.Types;
import java.time.ZoneOffset;
import java.util.TimeZone;
import org.apache.rocketmq.connect.jdbc.schema.column.ColumnDefinition;
import org.apache.rocketmq.connect.jdbc.util.DateTimeUtils;
import org.apache.rocketmq.connect.jdbc.util.NumericMapping;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DefaultColumnConverter implements JdbcColumnConverter {

    protected static final int NUMERIC_TYPE_SCALE_LOW = -84;
    protected static final int NUMERIC_TYPE_SCALE_HIGH = 127;
    protected static final int NUMERIC_TYPE_SCALE_UNSET = -127;
    private static final Logger log = LoggerFactory.getLogger(DefaultColumnConverter.class);
    // The maximum precision that can be achieved in a signed 64-bit integer is 2^63 ~= 9.223372e+18
    private static final int MAX_INTEGER_TYPE_PRECISION = 18;

    private final NumericMapping numericMapping;
    private final boolean isJdbc4;
    private final TimeZone timeZone;

    public DefaultColumnConverter(NumericMapping numericMapping, boolean isJdbc4, TimeZone timeZone) {
        this.numericMapping = numericMapping;
        this.isJdbc4 = isJdbc4;
        this.timeZone = timeZone;
    }

    @Override
    public Object convertToConnectFieldValue(ResultSet rs, ColumnDefinition columnDefinition, int columnNumber) throws SQLException, IOException {
        switch (columnDefinition.type()) {
            case Types.BOOLEAN:
                return rs.getBoolean(columnNumber);

            case Types.BIT:
                return rs.getByte(columnNumber);

            // 8 bits int
            case Types.TINYINT:
                if (columnDefinition.isSignedNumber()) {
                    return rs.getByte(columnNumber);
                } else {
                    return rs.getShort(columnNumber);
                }

                // 16 bits int
            case Types.SMALLINT:
                if (columnDefinition.isSignedNumber()) {
                    return rs.getShort(columnNumber);
                } else {
                    return rs.getInt(columnNumber);
                }

                // 32 bits int
            case Types.INTEGER:
                if (columnDefinition.isSignedNumber()) {
                    return rs.getInt(columnNumber);
                } else {
                    return rs.getLong(columnNumber);
                }

                // 64 bits int
            case Types.BIGINT:
                return rs.getLong(columnNumber);

            // REAL is a single precision floating point value, i.e. a Java float
            case Types.REAL:
                return rs.getFloat(columnNumber);

            // FLOAT is, confusingly, double precision and effectively the same as DOUBLE. See REAL
            // for single precision
            case Types.FLOAT:
            case Types.DOUBLE:
                return rs.getDouble(columnNumber);

            case Types.NUMERIC:
                if (numericMapping == NumericMapping.PRECISION_ONLY) {
                    int precision = columnDefinition.precision();
                    int scale = columnDefinition.scale();
                    log.trace("NUMERIC with precision: '{}' and scale: '{}'", precision, scale);
                    if (scale == 0 && precision <= MAX_INTEGER_TYPE_PRECISION) { // integer
                        if (precision > 9) {
                            return rs.getLong(columnNumber);
                        } else if (precision > 4) {
                            return rs.getInt(columnNumber);
                        } else if (precision > 2) {
                            return rs.getShort(columnNumber);
                        } else {
                            return rs.getByte(columnNumber);
                        }
                    }
                } else if (numericMapping == NumericMapping.BEST_FIT) {
                    int precision = columnDefinition.precision();
                    int scale = columnDefinition.scale();
                    log.trace("NUMERIC with precision: '{}' and scale: '{}'", precision, scale);
                    if (precision <= MAX_INTEGER_TYPE_PRECISION) { // fits in primitive data types.
                        if (scale < 1 && scale >= NUMERIC_TYPE_SCALE_LOW) { // integer
                            if (precision > 9) {
                                return rs.getLong(columnNumber);
                            } else if (precision > 4) {
                                return rs.getInt(columnNumber);
                            } else if (precision > 2) {
                                return rs.getShort(columnNumber);
                            } else {
                                return rs.getByte(columnNumber);
                            }
                        } else if (scale > 0) { // floating point - use double in all cases
                            return rs.getDouble(columnNumber);
                        }
                    }
                } else if (numericMapping == NumericMapping.BEST_FIT_EAGER_DOUBLE) {
                    int precision = columnDefinition.precision();
                    int scale = columnDefinition.scale();
                    log.trace("NUMERIC with precision: '{}' and scale: '{}'", precision, scale);
                    if (scale < 1 && scale >= NUMERIC_TYPE_SCALE_LOW) { // integer
                        if (precision <= MAX_INTEGER_TYPE_PRECISION) { // fits in primitive data types.
                            if (precision > 9) {
                                return rs.getLong(columnNumber);
                            } else if (precision > 4) {
                                return rs.getInt(columnNumber);
                            } else if (precision > 2) {
                                return rs.getShort(columnNumber);
                            } else {
                                return rs.getByte(columnNumber);
                            }
                        }
                    } else if (scale > 0) { // floating point - use double in all cases
                        return rs.getDouble(columnNumber);
                    }
                }
                // fallthrough

            case Types.DECIMAL:
                final int precision = columnDefinition.precision();
                log.debug("DECIMAL with precision: '{}' and scale: '{}'", precision, columnDefinition.scale());
                final int scale = decimalScale(columnDefinition);
                return rs.getBigDecimal(columnNumber, scale);

            case Types.CHAR:
            case Types.VARCHAR:
            case Types.LONGVARCHAR:
                return rs.getString(columnNumber);

            case Types.NCHAR:
            case Types.NVARCHAR:
            case Types.LONGNVARCHAR:
                return rs.getNString(columnNumber);

            // Binary == fixed, VARBINARY and LONGVARBINARY == bytes
            case Types.BINARY:
            case Types.VARBINARY:
            case Types.LONGVARBINARY:
                return rs.getBytes(columnNumber);

            // Date is day + month + year
            case Types.DATE:
                return rs.getDate(columnNumber,
                    DateTimeUtils.getTimeZoneCalendar(TimeZone.getTimeZone(ZoneOffset.UTC)));

            // Time is a time of day -- hour, minute, seconds, nanoseconds
            case Types.TIME:
                return rs.getTime(columnNumber, DateTimeUtils.getTimeZoneCalendar(timeZone));

            // Timestamp is a date + time
            case Types.TIMESTAMP:
                return rs.getTimestamp(columnNumber, DateTimeUtils.getTimeZoneCalendar(timeZone));

            // Datalink is basically a URL -> string
            case Types.DATALINK:
                URL url = rs.getURL(columnNumber);
                return (url != null) ? url.toString() : null;

            // BLOB == fixed
            case Types.BLOB:
                Blob blob = rs.getBlob(columnNumber);
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

            case Types.CLOB:
                Clob clob = rs.getClob(columnNumber);
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
            case Types.NCLOB:
                Clob nClob = rs.getNClob(columnNumber);
                if (nClob == null) {
                    return null;
                } else {
                    try {
                        if (nClob.length() > Integer.MAX_VALUE) {
                            throw new IOException("Can't process NCLOBs longer than " + Integer.MAX_VALUE);
                        }
                        return nClob.getSubString(1, (int) nClob.length());
                    } finally {
                        if (isJdbc4) {
                            free(nClob);
                        }
                    }
                }

                // XML -> string
            case Types.SQLXML:
                SQLXML xml = rs.getSQLXML(columnNumber);
                return xml != null ? xml.getString() : null;

            case Types.NULL:
            case Types.ARRAY:
            case Types.JAVA_OBJECT:
            case Types.OTHER:
            case Types.DISTINCT:
            case Types.STRUCT:
            case Types.REF:
            case Types.ROWID:
            default:
                // These are not currently supported
                log.warn("JDBC type {} ({}) not supported", columnDefinition.type(), columnDefinition.typeName());
                break;
        }
        return null;
    }

    protected void free(Blob blob) throws SQLException {
        blob.free();
    }

    protected void free(Clob clob) throws SQLException {
        clob.free();
    }

    protected int decimalScale(ColumnDefinition defn) {
        return defn.scale() == NUMERIC_TYPE_SCALE_UNSET ? NUMERIC_TYPE_SCALE_HIGH : defn.scale();
    }

    @Override
    public String convertToConnectFieldSchema(ColumnDefinition columnDefinition, SchemaBuilder builder) {
        int precision = columnDefinition.precision();
        int scale = columnDefinition.scale();
        int sqlType = columnDefinition.type();
        boolean optional = columnDefinition.isOptional();
        String fieldName = fieldNameFor(columnDefinition);
        switch (sqlType) {
            case Types.NULL:
                log.debug("JDBC type 'NULL' not currently supported for column '{}'", fieldName);
                return null;

            case Types.BOOLEAN:
                builder.field(fieldName, SchemaBuilder.bool().build());
                break;

            case Types.BIT:
                builder.field(fieldName, SchemaBuilder.int8().build());
                break;

            case Types.TINYINT:
                if (columnDefinition.isSignedNumber()) {
                    builder.field(fieldName, SchemaBuilder.int8().build());
                } else {
                    builder.field(fieldName, SchemaBuilder.int32().build());
                }
                break;

            case Types.SMALLINT:
                builder.field(fieldName, SchemaBuilder.int32().build());
                break;

            case Types.INTEGER:
                if (columnDefinition.isSignedNumber()) {
                    builder.field(fieldName, SchemaBuilder.int32().build());
                } else {
                    builder.field(fieldName, SchemaBuilder.int64().build());
                }
                break;

            case Types.BIGINT:
                builder.field(fieldName, SchemaBuilder.int64().build());
                break;

            case Types.REAL:
                builder.field(fieldName, SchemaBuilder.float32().build());
                break;

            case Types.FLOAT:
            case Types.DOUBLE:
                builder.field(fieldName, SchemaBuilder.float64().build());
                break;
            case Types.NUMERIC:
                if (numericMapping == NumericMapping.PRECISION_ONLY) {
                    log.debug("NUMERIC with precision: '{}' and scale: '{}'", precision, scale);
                    if (scale == 0 && precision <= MAX_INTEGER_TYPE_PRECISION) { // integer
                        builder.field(fieldName, integerSchema(optional, precision));
                        break;
                    }
                } else if (numericMapping == NumericMapping.BEST_FIT) {
                    log.debug("NUMERIC with precision: '{}' and scale: '{}'", precision, scale);
                    if (precision <= MAX_INTEGER_TYPE_PRECISION) {
                        if (scale < 1 && scale >= NUMERIC_TYPE_SCALE_LOW) {
                            builder.field(fieldName, integerSchema(optional, precision));
                            break;
                        } else if (scale > 0) {
                            builder.field(fieldName, SchemaBuilder.float64().build());
                            break;
                        }
                    }
                } else if (numericMapping == NumericMapping.BEST_FIT_EAGER_DOUBLE) {
                    log.debug("NUMERIC with precision: '{}' and scale: '{}'", precision, scale);
                    if (scale < 1 && scale >= NUMERIC_TYPE_SCALE_LOW) { // integer
                        if (precision <= MAX_INTEGER_TYPE_PRECISION) { // fits in primitive data types.
                            builder.field(fieldName, integerSchema(optional, precision));
                            break;
                        }
                    } else if (scale > 0) {
                        builder.field(fieldName, SchemaBuilder.float64().build());
                        break;
                    }
                }
            case Types.DECIMAL:
                scale = decimalScale(columnDefinition);
                SchemaBuilder fieldBuilder = Decimal.builder(scale);
                if (optional) {
                    fieldBuilder.optional();
                }
                builder.field(fieldName, fieldBuilder.build());
                break;

            case Types.CHAR:
            case Types.VARCHAR:
            case Types.LONGVARCHAR:
            case Types.NCHAR:
            case Types.NVARCHAR:
            case Types.LONGNVARCHAR:
            case Types.CLOB:
            case Types.NCLOB:
            case Types.DATALINK:
            case Types.SQLXML:
                builder.field(fieldName, SchemaBuilder.string().build());
                break;

            case Types.BINARY:
            case Types.BLOB:
            case Types.VARBINARY:
            case Types.LONGVARBINARY:
                builder.field(fieldName, SchemaBuilder.bytes().build());
                break;

            case Types.DATE:
                SchemaBuilder dateSchemaBuilder = Date.builder();
                builder.field(fieldName, dateSchemaBuilder.build());
                break;

            case Types.TIME:
                SchemaBuilder timeSchemaBuilder = Time.builder();
                builder.field(fieldName, timeSchemaBuilder.build());
                break;

            case Types.TIMESTAMP:
                SchemaBuilder tsSchemaBuilder = io.openmessaging.connector.api.data.logical.Timestamp.builder();
                builder.field(fieldName, tsSchemaBuilder.build());
                break;

            case Types.ARRAY:
            case Types.JAVA_OBJECT:
            case Types.OTHER:
            case Types.DISTINCT:
            case Types.STRUCT:
            case Types.REF:
            case Types.ROWID:
            default:
                log.warn("JDBC type {} ({}) not supported", sqlType, columnDefinition.typeName());
                return null;
        }
        return fieldName;
    }

    /**
     * Determine the name of the field. By default this is the column alias or name.
     */
    protected String fieldNameFor(ColumnDefinition columnDefinition) {
        return columnDefinition.id().aliasOrName();
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
        if (optional) {
            schema.setOptional(true);
        }
        return schema;
    }

}