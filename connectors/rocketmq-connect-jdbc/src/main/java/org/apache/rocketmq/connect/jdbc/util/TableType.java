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
package org.apache.rocketmq.connect.jdbc.util;

import java.util.Collection;
import java.util.EnumSet;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * table type
 */
public enum TableType {

    TABLE("TABLE", "Table"),
    VIEW("VIEW", "View");

    private final String value;
    private final String capitalCase;

    TableType(String value, String capitalCase) {
        this.value = value.toUpperCase();
        this.capitalCase = capitalCase;
    }

    public String capitalized() {
        return capitalCase;
    }

    public String jdbcName() {
        return value;
    }

    @Override
    public String toString() {
        return value;
    }

    public static TableType get(String name) {
        if (name != null) {
            name = name.trim();
        }
        for (TableType method : values()) {
            if (method.toString().equalsIgnoreCase(name)) {
                return method;
            }
        }
        throw new IllegalArgumentException("No matching QuoteMethod found for '" + name + "'");
    }

    public static EnumSet<TableType> parse(Collection<String> values) {
        Set<TableType> types = values.stream().map(TableType::get).collect(Collectors.toSet());
        return EnumSet.copyOf(types);
    }

}