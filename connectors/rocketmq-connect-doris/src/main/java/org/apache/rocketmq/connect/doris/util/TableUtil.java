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

package org.apache.rocketmq.connect.doris.util;

import io.openmessaging.connector.api.data.ConnectRecord;
import io.openmessaging.connector.api.data.Struct;
import org.apache.rocketmq.connect.doris.schema.table.TableId;

public class TableUtil {

    public static TableId parseToTableId(String fqn) {
        return new TableId(null, null, fqn);
    }
    public static TableId destinationTable(ConnectRecord record) {
        // todo table from header
        Struct struct = (Struct) record.getData();
        return new TableId(null, null, struct.getSchema().getName());
    }
}
