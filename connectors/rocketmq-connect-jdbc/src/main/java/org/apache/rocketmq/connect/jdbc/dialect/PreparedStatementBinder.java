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
package org.apache.rocketmq.connect.jdbc.dialect;

import io.openmessaging.connector.api.data.ConnectRecord;
import io.openmessaging.connector.api.data.Field;
import io.openmessaging.connector.api.data.Schema;
import io.openmessaging.connector.api.data.Struct;
import io.openmessaging.connector.api.errors.ConnectException;
import org.apache.rocketmq.connect.jdbc.connector.JdbcSinkConfig;
import org.apache.rocketmq.connect.jdbc.sink.metadata.FieldsMetadata;
import org.apache.rocketmq.connect.jdbc.sink.metadata.SchemaPair;
import org.apache.rocketmq.connect.jdbc.schema.column.ColumnDefinition;
import org.apache.rocketmq.connect.jdbc.schema.table.TableDefinition;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Objects;

/**
 * prepared statement binder
 */
public class PreparedStatementBinder implements DatabaseDialect.StatementBinder {

    private final JdbcSinkConfig.PrimaryKeyMode pkMode;
    private final PreparedStatement statement;
    private final SchemaPair schemaPair;
    private final FieldsMetadata fieldsMetadata;
    private final JdbcSinkConfig.InsertMode insertMode;
    private final DatabaseDialect dialect;
    private final TableDefinition tabDef;

    public PreparedStatementBinder(
            DatabaseDialect dialect,
            PreparedStatement statement,
            JdbcSinkConfig.PrimaryKeyMode pkMode,
            SchemaPair schemaPair,
            FieldsMetadata fieldsMetadata,
            TableDefinition tabDef,
            JdbcSinkConfig.InsertMode insertMode
    ) {
        this.dialect = dialect;
        this.pkMode = pkMode;
        this.statement = statement;
        this.schemaPair = schemaPair;
        this.fieldsMetadata = fieldsMetadata;
        this.insertMode = insertMode;
        this.tabDef = tabDef;
    }

    @Override
    public void bindRecord(ConnectRecord record) throws SQLException {
        final boolean isDelete = Objects.isNull(record.getData());

        int index = 1;
        if (isDelete) {
            bindKeyFields(record, index);
        } else {
            switch (insertMode) {
                case INSERT:
                case UPSERT:
                    index = bindKeyFields(record, index);
                    bindNonKeyFields(record, index);
                    break;

                case UPDATE:
                    index = bindNonKeyFields(record, index);
                    bindKeyFields(record, index);
                    break;
                default:
                    throw new AssertionError();

            }
        }
        statement.addBatch();
    }

    protected int bindKeyFields(ConnectRecord record, int index) throws SQLException {
        switch (pkMode) {
            case NONE:
                if (!fieldsMetadata.keyFieldNames.isEmpty()) {
                    throw new AssertionError();
                }
                break;
            case RECORD_KEY: {
                if (schemaPair.keySchema.getFieldType().isPrimitive()) {
                    assert fieldsMetadata.keyFieldNames.size() == 1;
                    bindField(index++, schemaPair.keySchema, record.getKey(),
                            fieldsMetadata.keyFieldNames.iterator().next());
                } else {
                    for (String fieldName : fieldsMetadata.keyFieldNames) {
                        final Field field = schemaPair.keySchema.getField(fieldName);
                        bindField(index++, field.getSchema(), ((Struct) record.getKey()).get(field), fieldName);
                    }
                }
            }
            break;
            case RECORD_VALUE: {
                Struct struct = (Struct) record.getData();
                for (String fieldName : fieldsMetadata.keyFieldNames) {
                    final Field field = schemaPair.schema.getField(fieldName);
                    bindField(index++, field.getSchema(), struct.get(fieldName), fieldName);
                }
            }
            break;
            default:
                throw new ConnectException("Unknown primary key mode: " + pkMode);
        }
        return index;
    }

    protected int bindNonKeyFields(
            ConnectRecord record,
            int index
    ) throws SQLException {
        Struct struct = (Struct) record.getData();
        for (final String fieldName : fieldsMetadata.nonKeyFieldNames) {
            final Field field = record.getSchema().getField(fieldName);
            bindField(index++, field.getSchema(), struct.get(fieldName), fieldName);
        }
        return index;
    }

    protected void bindField(int index, Schema schema, Object value, String fieldName)
            throws SQLException {
        ColumnDefinition colDef = tabDef == null ? null : tabDef.definitionForColumn(fieldName);
        dialect.bindField(statement, index, schema, value, colDef);
    }
}
