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

import java.util.Objects;
import org.apache.rocketmq.connect.jdbc.schema.table.TableId;
import org.apache.rocketmq.connect.jdbc.util.ExpressionBuilder;
import org.apache.rocketmq.connect.jdbc.util.QuoteMethod;

/**
 * column id
 */
public class ColumnId implements ExpressionBuilder.Expressable {

    private final TableId tableId;
    private final String name;
    private final String alias;
    private final int hash;

    public ColumnId(
            TableId tableId,
            String columnName
    ) {
        this(tableId, columnName, null);
    }

    public ColumnId(
            TableId tableId,
            String columnName,
            String alias
    ) {
        assert columnName != null;
        this.tableId = tableId;
        this.name = columnName;
        this.alias = alias != null && !alias.trim().isEmpty() ? alias : name;
        this.hash = Objects.hash(this.tableId, this.name);
    }

    public TableId tableId() {
        return tableId;
    }

    public String name() {
        return name;
    }

    public String aliasOrName() {
        return alias;
    }

    @Override
    public void appendTo(ExpressionBuilder builder, boolean useQuotes) {
        appendTo(builder, useQuotes ? QuoteMethod.ALWAYS : QuoteMethod.NEVER);
    }

    @Override
    public void appendTo(
            ExpressionBuilder builder,
            QuoteMethod useQuotes
    ) {
        if (tableId != null) {
            builder.append(tableId);
            builder.appendIdentifierDelimiter();
        }
        builder.appendColumnName(this.name, useQuotes);
    }

    @Override
    public int hashCode() {
        return hash;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        if (obj instanceof ColumnId) {
            ColumnId that = (ColumnId) obj;
            return Objects.equals(this.name, that.name) && Objects.equals(this.alias, that.alias)
                    && Objects.equals(this.tableId, that.tableId);
        }
        return false;
    }

    @Override
    public String toString() {
        return ExpressionBuilder.create().append(this).toString();
    }
}
