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

package org.apache.rocketmq.connect.jdbc.schema.column;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

public class DateTimeColumnParser extends ColumnParser {

    private static SimpleDateFormat dateTimeFormat;
    private static SimpleDateFormat dateTimeUtcFormat;

    static {
        dateTimeFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        dateTimeUtcFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        dateTimeUtcFormat.setTimeZone(TimeZone.getTimeZone("GMT+8"));
    }

    @Override
    public Object getValue(Object value) {

        if (value == null) {
            return null;
        }

        if (value instanceof Timestamp) {
            return dateTimeFormat.format(value);
        }

        if (value instanceof Long) {
            return dateTimeUtcFormat.format(new Date((Long) value));
        }

        return value;
    }
}
