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
package org.apache.rocketmq.connect.doris.schema.table;

import org.apache.rocketmq.connect.doris.schema.column.ColumnDefinition;
import org.apache.rocketmq.connect.doris.util.TableType;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;


/**
 * A description of a table.
 */
public class TableDefinition {
    private final TableId id;
    private final Map<String, String> pkColumnNames = new LinkedHashMap<>();

    public TableDefinition(TableId id, Iterable<ColumnDefinition> columns, TableType type) {
        this.id = id;
    }

    public TableId id() {
        return id;
    }

    @Override
    public int hashCode() {
        return id.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return true;
        }
        if (obj instanceof TableDefinition) {
            TableDefinition that = (TableDefinition) obj;
            return Objects.equals(this.id(), that.id());
        }
        return false;
    }

    @Override
    public String toString() {
        return String.format("Table{name='%s', type=%s columns=%s}", id);
    }
}
