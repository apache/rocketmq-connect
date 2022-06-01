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

package org.apache.rocketmq.connect.jdbc.schema.column.parser;

import io.openmessaging.connector.api.data.Schema;
import io.openmessaging.connector.api.data.SchemaBuilder;
import io.openmessaging.connector.api.errors.ConnectException;

import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;

public class TimeColumnParser {
    public static final String LOGICAL_NAME = "org.apache.rocketmq.connect.jdbc.schema.column.parser.TimeColumnParser";
    private static final long MILLIS_PER_DAY = 86400000L;
    private static final TimeZone UTC = TimeZone.getTimeZone("UTC");
    public static final Schema SCHEMA = builder().build();

    public static SchemaBuilder builder() {
        return SchemaBuilder.int32().name("org.apache.rocketmq.connect.jdbc.schema.column.parser.TimeColumnParser");
    }

    public static int fromLogical(Schema schema, Date value) {
        if (!LOGICAL_NAME.equals(schema.getName())) {
            throw new ConnectException("Requested conversion of Time object but the schema does not match.");
        } else {
            Calendar calendar = Calendar.getInstance(UTC);
            calendar.setTime(value);
            long unixMillis = calendar.getTimeInMillis();
            if (unixMillis >= 0L && unixMillis <= 86400000L) {
                return (int) unixMillis;
            } else {
                throw new ConnectException("Kafka Connect Time type should not have any date fields set to non-zero values.");
            }
        }
    }

    public static Date toLogical(Schema schema, int value) {
        if (!LOGICAL_NAME.equals(schema.getName())) {
            throw new ConnectException("Requested conversion of Date object but the schema does not match.");
        } else if (value >= 0 && (long) value <= 86400000L) {
            return new Date((long) value);
        } else {
            throw new ConnectException("Time values must use number of milliseconds greater than 0 and less than 86400000");
        }
    }

}
