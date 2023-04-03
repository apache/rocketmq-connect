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

package org.apache.rocketmq.connect.hive.config;

import io.openmessaging.connector.api.data.Schema;
import io.openmessaging.connector.api.data.SchemaBuilder;
import java.sql.Timestamp;
import java.util.Date;

public class SchemaManger {

    public static Schema getSchema(Object obj) {
        if (obj instanceof Integer) {
            return SchemaBuilder.int32().build();
        } else if (obj instanceof Long) {
            return SchemaBuilder.int64().build();
        } else if (obj instanceof String) {
            return SchemaBuilder.string().build();
        } else if (obj instanceof Date) {
            return SchemaBuilder.time().build();
        } else if (obj instanceof Timestamp) {
            return SchemaBuilder.timestamp().build();
        }
        return SchemaBuilder.string().build();
    }
}
