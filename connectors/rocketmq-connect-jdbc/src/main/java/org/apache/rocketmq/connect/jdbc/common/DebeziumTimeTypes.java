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

package org.apache.rocketmq.connect.jdbc.common;

import io.debezium.time.Date;
import io.debezium.time.ZonedTimestamp;
import io.openmessaging.connector.api.data.Schema;
import org.apache.rocketmq.connect.jdbc.util.DateTimeUtils;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.TimeZone;


/**
 * debezium time type
 */
public class DebeziumTimeTypes {
    public static final String DATE = "io.debezium.time.Date";
    public static final String INTERVAL = "io.debezium.time.Interval";
    public static final String MICRO_DURATION = "io.debezium.time.MicroDuration";
    public static final String MICRO_TIME = "io.debezium.time.MicroTime";
    public static final String MICRO_TIMESTAMP = "io.debezium.time.MicroTimestamp";
    public static final String NANO_DURATION = "io.debezium.time.NanoDuration";
    public static final String NANO_TIME = "io.debezium.time.NanoTime";
    public static final String NANO_TIMESTAMP = "io.debezium.time.NanoTimestamp";
    public static final String TIME = "io.debezium.time.Time";
    public static final String TIMESTAMP = "io.debezium.time.Timestamp";
    public static final String YEAR = "io.debezium.time.Year";
    public static final String ZONED_TIME = "io.debezium.time.ZonedTime";
    public static final String ZONED_TIMESTAMP = "io.debezium.time.ZonedTimestamp";


    /**
     * maybe bind debezium logical
     *
     * @param statement
     * @param index
     * @param schema
     * @param value
     * @param timeZone
     * @return
     * @throws SQLException
     */
    public static boolean maybeBindDebeziumLogical(
            PreparedStatement statement,
            int index,
            Schema schema,
            Object value,
            TimeZone timeZone
    ) throws SQLException {
        if (schema.getName() != null) {
            switch (schema.getName()) {
                case Date.SCHEMA_NAME:
                    statement.setDate(index,
                            new java.sql.Date(
                                    (long) DebeziumTimeTypes.toMillsTimestamp(Date.SCHEMA_NAME, value)
                            ),
                            DateTimeUtils.getTimeZoneCalendar(timeZone)
                    );
                    return true;
                case io.debezium.time.Timestamp.SCHEMA_NAME:

                    statement.setTimestamp(index,
                            new java.sql.Timestamp((long) DebeziumTimeTypes.toMillsTimestamp(io.debezium.time.Timestamp.SCHEMA_NAME, value)),
                            DateTimeUtils.getTimeZoneCalendar(timeZone)
                    );
                    return true;
                case ZonedTimestamp.SCHEMA_NAME:

                    statement.setTimestamp(index,
                            new java.sql.Timestamp((long) toMillsTimestamp(ZonedTimestamp.SCHEMA_NAME, value)),
                            DateTimeUtils.getTimeZoneCalendar(timeZone)
                    );
                    return true;
                default:
                    return false;
            }
        }
        return false;
    }


    private static Object toMillsTimestamp(String schemaName, Object value) {
        if (schemaName == null) {
            return value;
        }
        switch (schemaName) {
            case ZonedTimestamp.SCHEMA_NAME:
                DateTimeFormatter formatter = ZonedTimestamp.FORMATTER;
                LocalDateTime localDateTime = LocalDateTime.parse(value.toString(), formatter);
                return localDateTime.toInstant(ZoneOffset.ofHours(0)).toEpochMilli();

            case Date.SCHEMA_NAME:
                return LocalDate.ofEpochDay(Long.valueOf((int) value))
                        .atStartOfDay(ZoneOffset.ofHours(0))
                        .toInstant()
                        .toEpochMilli();
            default:
                return value;
        }
    }

}
