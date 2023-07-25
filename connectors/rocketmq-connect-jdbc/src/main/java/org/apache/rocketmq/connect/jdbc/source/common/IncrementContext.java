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

package org.apache.rocketmq.connect.jdbc.source.common;

import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import org.apache.rocketmq.connect.jdbc.schema.table.TableId;

public class IncrementContext extends QueryContext {
    public IncrementContext(
        QueryMode mode,
        TableId tableId,
        String querySql,
        String topicPrefix,
        String offsetSuffix,
        String querySuffix,
        int batchMaxSize,
        List<String> timestampColumnNames,
        String incrementingColumnName,
        Map<String, Object> offsetMap,
        Long timestampDelay,
        TimeZone timeZone
    ) {
        super(mode, tableId, querySql, topicPrefix, offsetSuffix, querySuffix, batchMaxSize);
        this.timestampColumnNames = timestampColumnNames;
        this.incrementingColumnName = incrementingColumnName;
        this.offsetMap = offsetMap;
        this.timestampDelay = timestampDelay;
        this.timeZone = timeZone;
    }

    private List<String> timestampColumnNames;
    private String incrementingColumnName;
    private Map<String, Object> offsetMap;
    private Long timestampDelay;
    private TimeZone timeZone;

    public List<String> getTimestampColumnNames() {
        return timestampColumnNames;
    }

    public void setTimestampColumnNames(List<String> timestampColumnNames) {
        this.timestampColumnNames = timestampColumnNames;
    }

    public String getIncrementingColumnName() {
        return incrementingColumnName;
    }

    public void setIncrementingColumnName(String incrementingColumnName) {
        this.incrementingColumnName = incrementingColumnName;
    }

    public Map<String, Object> getOffsetMap() {
        return offsetMap;
    }

    public void setOffsetMap(Map<String, Object> offsetMap) {
        this.offsetMap = offsetMap;
    }

    public Long getTimestampDelay() {
        return timestampDelay;
    }

    public void setTimestampDelay(Long timestampDelay) {
        this.timestampDelay = timestampDelay;
    }

    public TimeZone getTimeZone() {
        return timeZone;
    }

    public void setTimeZone(TimeZone timeZone) {
        this.timeZone = timeZone;
    }
}
