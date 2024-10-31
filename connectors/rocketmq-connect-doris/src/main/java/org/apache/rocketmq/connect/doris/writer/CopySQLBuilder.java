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

package org.apache.rocketmq.connect.doris.writer;

import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.StringJoiner;

public class CopySQLBuilder {
    private static final String COPY_SYNC = "copy.async";
    private final String database;
    private final String table;
    private final List<String> fileList;
    private final Properties properties;

    private final boolean enableDelete;

    public CopySQLBuilder(
        String database, String table, List<String> fileList, boolean enableDelete) {
        this.database = database;
        this.table = table;
        this.fileList = fileList;
        this.enableDelete = enableDelete;
        this.properties = new Properties();
    }

    public String buildCopySQL() {
        StringBuilder sb = new StringBuilder();
        sb.append("COPY INTO ")
            .append(database)
            .append(".")
            .append(table)
            .append(" FROM @~('{")
            .append(String.join(",", fileList))
            .append("}') ")
            .append("PROPERTIES (");

        // copy into must be sync
        properties.put(COPY_SYNC, false);
        // default json
        properties.put("file.type", "json");
        properties.put("file.strip_outer_array", "false");
        properties.put("copy.use_delete_sign", enableDelete);
        StringJoiner props = new StringJoiner(",");
        for (Map.Entry<Object, Object> entry : properties.entrySet()) {
            String key = String.valueOf(entry.getKey());
            String value = String.valueOf(entry.getValue());
            String prop = String.format("'%s'='%s'", key, value);
            props.add(prop);
        }
        sb.append(props).append(")");
        return sb.toString();
    }
}
