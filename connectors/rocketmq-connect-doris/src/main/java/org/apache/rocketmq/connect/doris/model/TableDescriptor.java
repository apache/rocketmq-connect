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

package org.apache.rocketmq.connect.doris.model;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class TableDescriptor {
    private final String tableName;
    private final String tableType;
    private final Map<String, ColumnDescriptor> columns = new LinkedHashMap<>();

    private TableDescriptor(String tableName, String tableType, List<ColumnDescriptor> columns) {
        this.tableName = tableName;
        this.tableType = tableType;
        columns.forEach(c -> this.columns.put(c.getColumnName(), c));
    }

    public static Builder builder() {
        return new Builder();
    }

    public String getTableName() {
        return tableName;
    }

    public String getTableType() {
        return tableType;
    }

    public Collection<ColumnDescriptor> getColumns() {
        return columns.values();
    }

    public ColumnDescriptor getColumnByName(String columnName) {
        return columns.get(columnName);
    }

    public boolean hasColumn(String columnName) {
        return columns.containsKey(columnName);
    }

    public static class Builder {
        private String schemaName;
        private String tableName;
        private String tableType;
        private final List<ColumnDescriptor> columns = new ArrayList<>();

        private Builder() {
        }

        public Builder schemaName(String schemaName) {
            this.schemaName = schemaName;
            return this;
        }

        public Builder tableName(String tableName) {
            this.tableName = tableName;
            return this;
        }

        public Builder type(String tableType) {
            this.tableType = tableType;
            return this;
        }

        public Builder column(ColumnDescriptor column) {
            this.columns.add(column);
            return this;
        }

        public Builder columns(List<ColumnDescriptor> columns) {
            this.columns.addAll(columns);
            return this;
        }

        public TableDescriptor build() {
            return new TableDescriptor(tableName, tableType, columns);
        }
    }
}
