#set( $symbol_pound = '#' )
#set( $symbol_dollar = '$' )
#set( $symbol_escape = '\' )
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

package ${package}.sink;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import io.openmessaging.KeyValue;
import io.openmessaging.connector.api.component.task.sink.SinkTask;
import io.openmessaging.connector.api.data.ConnectRecord;
import io.openmessaging.connector.api.data.Field;
import io.openmessaging.connector.api.data.Struct;
import io.openmessaging.connector.api.errors.ConnectException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import ${package}.helper.${dbNameToCamel}HelperClient;
import ${package}.config.${dbNameToCamel}SinkConfig;

public class ${dbNameToCamel}SinkTask extends SinkTask {

    public ${dbNameToCamel}SinkConfig config;

    private ${dbNameToCamel}HelperClient helperClient;

    @Override public void put(List<ConnectRecord> sinkRecords) throws ConnectException {
        if (sinkRecords == null || sinkRecords.size() < 1) {
            return;
        }
        for (ConnectRecord record : sinkRecords) {
            String table = record.getSchema().getName();
            final List<Field> fields = record.getSchema().getFields();
            final Struct structData = (Struct) record.getData();

            // FIXME: Write your code here
            throw new RuntimeException("Method not implemented");
        }
    }

    @Override public void start(KeyValue keyValue) {
        this.config = new ${dbNameToCamel}SinkConfig();
        this.config.load(keyValue);
        this.helperClient = new ${dbNameToCamel}HelperClient(this.config);
        if (!helperClient.ping()) {
            throw new RuntimeException("Cannot connect to ${dbNameToLowerCase} server!");
        }
    }

    @Override public void stop() {
        this.helperClient.stop();
    }
}
