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

import java.time.LocalDate;
import java.time.ZoneOffset;


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

    public static Object toMillsTimestamp(String schemaName, Object value){
        if(schemaName == null){
            return value;
        }
        switch (schemaName){
            case DATE:
                return LocalDate.ofEpochDay((long)value)
                        .atStartOfDay(ZoneOffset.ofHours(8))
                        .toInstant()
                        .toEpochMilli();
            case TIMESTAMP:
                return value;
            default:
                return value;
        }
    }

}
