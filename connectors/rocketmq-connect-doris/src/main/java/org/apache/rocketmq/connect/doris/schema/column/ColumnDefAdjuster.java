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
package org.apache.rocketmq.connect.doris.schema.column;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

public class ColumnDefAdjuster {
    Map<String, ColumnDefinition.Nullability> nullable = new HashMap<>();

    public ColumnDefAdjuster() {
    }

    public static ColumnDefAdjuster create(Connection conn,
                                           String catalogPattern,
                                           String schemaPattern,
                                           String tablePattern,
                                           String columnPattern) {
        ColumnDefAdjuster adjuster = new ColumnDefAdjuster();
        try (ResultSet rs = conn.getMetaData().getColumns(
                catalogPattern, schemaPattern, tablePattern, columnPattern)) {
            final int rsColumnCount = rs.getMetaData().getColumnCount();
            while (rs.next()) {
                final String columnName = rs.getString(4);
                ColumnDefinition.Nullability nullability;
                final int nullableValue = rs.getInt(11);
                switch (nullableValue) {
                    case DatabaseMetaData.columnNoNulls:
                        nullability = ColumnDefinition.Nullability.NOT_NULL;
                        break;
                    case DatabaseMetaData.columnNullable:
                        nullability = ColumnDefinition.Nullability.NULL;
                        break;
                    case DatabaseMetaData.columnNullableUnknown:
                    default:
                        nullability = ColumnDefinition.Nullability.UNKNOWN;
                        break;
                }
                adjuster.nullable.put(columnName, nullability);
            }
        } catch (SQLException e) {
            //pass
        }

        return adjuster;
    }

    public ColumnDefinition.Nullability nullable(String columnName) {
        if (nullable == null || !nullable.containsKey(columnName)) {
            return null;
        }
        return nullable.get(columnName);
    }
}
