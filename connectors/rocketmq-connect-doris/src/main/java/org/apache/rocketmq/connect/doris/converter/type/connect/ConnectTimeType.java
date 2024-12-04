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

package org.apache.rocketmq.connect.doris.converter.type.connect;

import io.openmessaging.connector.api.data.logical.Time;
import io.openmessaging.connector.api.errors.ConnectException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Date;
import org.apache.rocketmq.connect.doris.converter.type.AbstractTimeType;
import org.apache.rocketmq.connect.doris.converter.type.util.DateTimeUtils;

public class ConnectTimeType extends AbstractTimeType {

    public static final ConnectTimeType INSTANCE = new ConnectTimeType();

    @Override
    public String[] getRegistrationKeys() {
        return new String[] {Time.LOGICAL_NAME};
    }

    @Override
    public Object getValue(Object sourceValue) {
        if (sourceValue == null) {
            return null;
        }
        if (sourceValue instanceof Date) {

            final LocalTime localTime = DateTimeUtils.toLocalTimeFromUtcDate((Date) sourceValue);
            final LocalDateTime localDateTime = localTime.atDate(LocalDate.now());
            return localDateTime.toLocalTime();
        }

        throw new ConnectException(
            String.format(
                "Unexpected %s value '%s' with type '%s'",
                getClass().getSimpleName(), sourceValue, sourceValue.getClass().getName()));
    }
}
