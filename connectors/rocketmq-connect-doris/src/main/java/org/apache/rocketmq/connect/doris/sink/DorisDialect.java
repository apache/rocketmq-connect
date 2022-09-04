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

package org.apache.rocketmq.connect.doris.sink;

import io.openmessaging.connector.api.data.ConnectRecord;
import io.openmessaging.connector.api.data.Field;
import io.openmessaging.connector.api.data.Struct;
import org.apache.rocketmq.connect.doris.exception.TableAlterOrCreateException;
import com.alibaba.fastjson.JSON;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class DorisDialect {
    public static String convertToUpdateJsonString(ConnectRecord record, boolean isFirst) {
        try {
            Struct struct = (Struct) record.getData();
            Map<String, String> keyValue = new HashMap<>();
            for (Field field: struct.getSchema().getFields()) {
                bindValue(keyValue, field, struct.getValues()[field.getIndex()]);
            }
            return isFirst ? "" : "," + JSON.toJSON(keyValue).toString();
        } catch (TableAlterOrCreateException tace) {
            throw tace;
        }
    }

    public static String convertToUpdateJsonString(ConnectRecord record) {
        try {
            Struct struct = (Struct) record.getData();
            Map<String, String> keyValue = new HashMap<>();
            for (Field field: struct.getSchema().getFields()) {
                bindValue(keyValue, field, struct.getValues()[field.getIndex()]);
            }
            return JSON.toJSON(keyValue).toString();
        } catch (TableAlterOrCreateException tace) {
            throw tace;
        }
    }

    public static String convertToDeleteJsonString(ConnectRecord record) {
        try {
            // it seems that doris doesn't support delete via stream load
            return "";
        } catch (TableAlterOrCreateException tace) {
            throw tace;
        }
    }
    
    private static void bindValue(Map<String, String> keyValue, Field field, Object value) {
        switch (field.getSchema().getFieldType()) {
            case INT8:
            case BOOLEAN:
            case FLOAT64:
            case INT32:
            case INT64:
            case FLOAT32:
                if (value == null) {
                    keyValue.put(field.getName(), "null");
                } else {
                    keyValue.put(field.getName(), value.toString());
                }
                break;
            case STRING:
                if (value == null) {
                    keyValue.put(field.getName(), "null");
                } else {
                    keyValue.put(field.getName(), (String) value);
                }
                break;
            case BYTES:
                if (value == null) {
                    keyValue.put(field.getName(), "null");
                } else {
                    final byte[] bytes;
                    if (value instanceof ByteBuffer) {
                        final ByteBuffer buffer = ((ByteBuffer) value).slice();
                        bytes = new byte[buffer.remaining()];
                        buffer.get(bytes);
                    } else if (value instanceof BigDecimal) {
                        keyValue.put(field.getName(), value.toString());
                        break;
                    } else {
                        bytes = (byte[]) value;
                    }
                    keyValue.put(field.getName(), Arrays.toString(bytes));
                }
                break;
            case DATETIME:
                if (value == null) {
                    keyValue.put(field.getName(), "null");
                } else {
                    java.sql.Date date;
                    if (value instanceof java.util.Date) {
                        date = new java.sql.Date(((java.util.Date) value).getTime());
                    } else {
                        date = new java.sql.Date((int) value);
                    }
                    keyValue.put(
                            field.getName(), date.toString()
                    );
                }
                break;
            default:
                throw new TableAlterOrCreateException("Field type not found " + field);
        }
    }
}
