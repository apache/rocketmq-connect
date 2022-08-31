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
package org.apache.rocketmq.connect.doris.dialect;

//import io.openmessaging.connector.api.data.ConnectRecord;
//import io.openmessaging.connector.api.data.Schema;
//import io.openmessaging.connector.api.data.SchemaBuilder;
import org.apache.rocketmq.connect.doris.schema.table.TableId;
//import org.apache.rocketmq.connect.doris.connector.DorisSinkConfig;
import org.apache.rocketmq.connect.doris.dialect.provider.ConnectionProvider;
//import org.apache.rocketmq.connect.doris.schema.column.ColumnDefinition;
//import org.apache.rocketmq.connect.doris.schema.column.ColumnId;
//import org.apache.rocketmq.connect.doris.schema.table.TableDefinition;
//import org.apache.rocketmq.connect.doris.schema.table.TableId;
//import org.apache.rocketmq.connect.doris.sink.metadata.FieldsMetadata;
//import org.apache.rocketmq.connect.doris.sink.metadata.SchemaPair;
//import org.apache.rocketmq.connect.doris.sink.metadata.SinkRecordField;
import org.apache.rocketmq.connect.doris.util.IdentifierRules;


/**
 * database dialect
 */
public interface DatabaseDialect extends ConnectionProvider {

    /**
     * dialect name
     *
     * @return
     */
    String name();

    /**
     * get dialect class
     *
     * @return
     */
    default Class getDialectClass() {
        return this.getClass();
    }

    /**
     * parse to Table Id
     *
     * @param fqn
     * @return
     */
    TableId parseToTableId(String fqn);

    /**
     * Get the identifier rules for this database.
     *
     * @return the identifier rules
     */
    IdentifierRules identifierRules();
}