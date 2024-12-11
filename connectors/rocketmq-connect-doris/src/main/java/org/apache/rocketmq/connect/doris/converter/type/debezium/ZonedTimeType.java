/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.rocketmq.connect.doris.converter.type.debezium;

import io.debezium.time.ZonedTime;
import io.openmessaging.connector.api.errors.ConnectException;
import java.time.OffsetTime;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import org.apache.rocketmq.connect.doris.converter.type.AbstractTimeType;

public class ZonedTimeType extends AbstractTimeType {

    public static final ZonedTimeType INSTANCE = new ZonedTimeType();
    // The ZonedTime of debezium type only contains three types of hours, minutes and seconds
    private final DateTimeFormatter TIME_FORMATTER = DateTimeFormatter.ofPattern("HH:mm:ss");

    @Override
    public String[] getRegistrationKeys() {
        return new String[] {ZonedTime.SCHEMA_NAME};
    }

    @Override
    public Object getValue(Object sourceValue) {
        if (sourceValue == null) {
            return null;
        }
        if (sourceValue instanceof String) {
            OffsetTime offsetTime =
                OffsetTime.parse((String) sourceValue, ZonedTime.FORMATTER)
                    .withOffsetSameInstant(
                        ZonedDateTime.now(getDatabaseTimeZone().toZoneId())
                            .getOffset());
            return offsetTime.format(TIME_FORMATTER);
        }

        throw new ConnectException(
            String.format(
                "Unexpected %s value '%s' with type '%s'",
                getClass().getSimpleName(), sourceValue, sourceValue.getClass().getName()));
    }
}
