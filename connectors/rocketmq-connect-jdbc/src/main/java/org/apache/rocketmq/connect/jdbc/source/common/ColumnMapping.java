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

import io.openmessaging.connector.api.data.Field;
import java.util.Objects;
import org.apache.rocketmq.connect.jdbc.schema.column.ColumnDefinition;


/**
 * column mapping
 */
public class ColumnMapping {
    private final Field field;
    private final ColumnDefinition columnDefinition;
    private final int columnNumber;
    private final int hash;

    public ColumnMapping(
        ColumnDefinition columnDefinition,
        int columnNumber,
        Field field
    ) {
        assert columnDefinition != null;
        assert field != null;
        assert columnNumber > 0;
        this.columnDefinition = columnDefinition;
        this.field = field;
        this.columnNumber = columnNumber;
        this.hash = Objects.hash(this.columnNumber, this.columnDefinition, this.field);
    }

    public Field field() {
        return field;
    }

    public ColumnDefinition columnDefinition() {
        return columnDefinition;
    }

    public int columnNumber() {
        return columnNumber;
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
        if (obj instanceof ColumnMapping) {
            ColumnMapping that = (ColumnMapping) obj;
            return this.columnNumber == that.columnNumber && Objects.equals(
                this.columnDefinition, that.columnDefinition) && Objects.equals(this.field, that.field);
        }
        return false;
    }

    @Override
    public String toString() {
        return field.getName() + " (col=" + columnNumber + ", " + columnDefinition + ")";
    }
}
