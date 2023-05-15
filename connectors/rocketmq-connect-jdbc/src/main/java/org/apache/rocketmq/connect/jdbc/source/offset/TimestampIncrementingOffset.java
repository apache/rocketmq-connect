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
package org.apache.rocketmq.connect.jdbc.source.offset;

import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Timestamp incrementing offset
 */
public class TimestampIncrementingOffset {
    private static final Logger log = LoggerFactory.getLogger(TimestampIncrementingOffset.class);
    public static final String INCREMENTING_FIELD = "incrementing";
    public static final String TIMESTAMP_FIELD = "timestamp";
    public static final String TIMESTAMP_NANOS_FIELD = "timestamp_nanos";

    private final Long incrementingOffset;
    private final Timestamp timestampOffset;

    public TimestampIncrementingOffset(Timestamp timestampOffset, Long incrementingOffset) {
        this.timestampOffset = timestampOffset;
        this.incrementingOffset = incrementingOffset;
    }

    public long getIncrementingOffset() {
        return incrementingOffset == null ? -1 : incrementingOffset;
    }

    public Timestamp getTimestampOffset() {
        return timestampOffset != null ? timestampOffset : new Timestamp(0L);
    }

    public Map<String, Object> toMap() {
        Map<String, Object> map = new HashMap<>(3);
        if (incrementingOffset != null) {
            map.put(INCREMENTING_FIELD, incrementingOffset);
        }
        if (timestampOffset != null) {
            map.put(TIMESTAMP_FIELD, timestampOffset.getTime());
            map.put(TIMESTAMP_NANOS_FIELD, (long) timestampOffset.getNanos());
        }
        return map;
    }

    public static TimestampIncrementingOffset fromMap(Map<String, ?> map) {
        if (map == null || map.isEmpty()) {
            return new TimestampIncrementingOffset(null, null);
        }

        final Object increment = map.get(INCREMENTING_FIELD);
        Long incr = increment == null ? 0 : Long.valueOf(increment.toString());
        Long millis = (Long) map.get(TIMESTAMP_FIELD);
        Timestamp ts = null;
        if (millis != null) {
            log.trace("millis is not null");
            ts = new Timestamp(millis);
            Long nanos = (Long) map.get(TIMESTAMP_NANOS_FIELD);
            if (nanos != null) {
                log.trace("Nanos is not null");
                ts.setNanos(nanos.intValue());
            }
        }
        return new TimestampIncrementingOffset(ts, incr);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        TimestampIncrementingOffset that = (TimestampIncrementingOffset) o;

        if (incrementingOffset != null
                ? !incrementingOffset.equals(that.incrementingOffset)
                : that.incrementingOffset != null) {
            return false;
        }
        return timestampOffset != null
                ? timestampOffset.equals(that.timestampOffset)
                : that.timestampOffset == null;

    }

    @Override
    public int hashCode() {
        int result = incrementingOffset != null ? incrementingOffset.hashCode() : 0;
        result = 31 * result + (timestampOffset != null ? timestampOffset.hashCode() : 0);
        return result;
    }
}
