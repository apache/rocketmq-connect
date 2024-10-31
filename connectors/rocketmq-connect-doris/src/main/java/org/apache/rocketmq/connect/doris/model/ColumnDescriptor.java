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

import java.util.Objects;

public class ColumnDescriptor {
    private final String columnName;
    private final String typeName;
    private final String comment;
    private final String defaultValue;

    private ColumnDescriptor(
        String columnName, String typeName, String comment, String defaultValue) {
        this.columnName = columnName;
        this.typeName = typeName;
        this.comment = comment;
        this.defaultValue = defaultValue;
    }

    public String getColumnName() {
        return columnName;
    }

    public String getTypeName() {
        return typeName;
    }

    public String getDefaultValue() {
        return defaultValue;
    }

    public String getComment() {
        return comment;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private String columnName;
        private String typeName;
        private String comment;
        private String defaultValue;

        public Builder columnName(String columnName) {
            this.columnName = columnName;
            return this;
        }

        public Builder typeName(String typeName) {
            this.typeName = typeName;
            return this;
        }

        public Builder comment(String comment) {
            this.comment = comment;
            return this;
        }

        public Builder defaultValue(String defaultValue) {
            this.defaultValue = defaultValue;
            return this;
        }

        public ColumnDescriptor build() {
            Objects.requireNonNull(columnName, "A column name is required");
            Objects.requireNonNull(typeName, "A type name is required");

            return new ColumnDescriptor(columnName, typeName, comment, defaultValue);
        }
    }
}
