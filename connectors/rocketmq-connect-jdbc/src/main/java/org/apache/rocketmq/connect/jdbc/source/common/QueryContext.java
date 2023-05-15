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

import org.apache.rocketmq.connect.jdbc.schema.table.TableId;

public class QueryContext {

    public QueryContext() {
    }

    public QueryContext(QueryMode mode, TableId tableId, String querySql, String topicPrefix, String offsetSuffix,
        String querySuffix, int batchMaxSize) {
        this.mode = mode;
        this.tableId = tableId;
        this.querySql = querySql;
        this.topicPrefix = topicPrefix;
        this.offsetSuffix = offsetSuffix;
        this.querySuffix = querySuffix;
        this.batchMaxSize = batchMaxSize;
        this.lastUpdate = 0;
    }

    private QueryMode mode;
    private TableId tableId;
    private String querySql;
    private String topicPrefix;
    private String offsetSuffix;
    private String querySuffix;
    private int batchMaxSize;
    private long lastUpdate = 0;

    public QueryMode getMode() {
        return mode;
    }

    public void setMode(QueryMode mode) {
        this.mode = mode;
    }

    public TableId getTableId() {
        return tableId;
    }

    public void setTableId(TableId tableId) {
        this.tableId = tableId;
    }

    public String getQuerySql() {
        return querySql;
    }

    public void setQuerySql(String querySql) {
        this.querySql = querySql;
    }

    public String getTopicPrefix() {
        return topicPrefix;
    }

    public void setTopicPrefix(String topicPrefix) {
        this.topicPrefix = topicPrefix;
    }

    public String getOffsetSuffix() {
        return offsetSuffix;
    }

    public void setOffsetSuffix(String offsetSuffix) {
        this.offsetSuffix = offsetSuffix;
    }

    public String getQuerySuffix() {
        return querySuffix;
    }

    public void setQuerySuffix(String querySuffix) {
        this.querySuffix = querySuffix;
    }

    public int getBatchMaxSize() {
        return batchMaxSize;
    }

    public void setBatchMaxSize(int batchMaxSize) {
        this.batchMaxSize = batchMaxSize;
    }

    public long getLastUpdate() {
        return lastUpdate;
    }

    public void setLastUpdate(long lastUpdate) {
        this.lastUpdate = lastUpdate;
    }
}
