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
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.rocketmq.connect.transforms;

import io.openmessaging.connector.api.component.Transform;
import io.openmessaging.connector.api.data.ConnectRecord;
import io.openmessaging.connector.api.data.FieldType;
import io.openmessaging.connector.api.data.Schema;
import io.openmessaging.connector.api.data.SchemaAndValue;
import io.openmessaging.connector.api.data.Struct;
import io.openmessaging.connector.api.data.logical.Decimal;
import io.openmessaging.connector.api.data.logical.Time;
import io.openmessaging.connector.api.data.logical.Timestamp;
import org.apache.rocketmq.connect.transforms.util.SchemaHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.util.Date;
import java.util.List;
import java.util.Map;

public abstract class BaseTransformation<R extends ConnectRecord> implements Transform<R> {
    private static final Logger log = LoggerFactory.getLogger(BaseTransformation.class);

    protected SchemaAndValue processMap(R record, Map<String, Object> input) {
        throw new UnsupportedOperationException("MAP is not a supported type.");
    }

    protected SchemaAndValue processStruct(R record, Schema inputSchema, Struct input) {
        throw new UnsupportedOperationException("STRUCT is not a supported type.");
    }

    protected SchemaAndValue processString(R record, Schema inputSchema, String input) {
        throw new UnsupportedOperationException("STRING is not a supported type.");
    }

    protected SchemaAndValue processBytes(R record, Schema inputSchema, byte[] input) {
        throw new UnsupportedOperationException("BYTES is not a supported type.");
    }

    protected SchemaAndValue processInt8(R record, Schema inputSchema, byte input) {
        throw new UnsupportedOperationException("INT8 is not a supported type.");
    }

    protected SchemaAndValue processInt16(R record, Schema inputSchema, short input) {
        throw new UnsupportedOperationException("INT16 is not a supported type.");
    }

    protected SchemaAndValue processInt32(R record, Schema inputSchema, int input) {
        throw new UnsupportedOperationException("INT32 is not a supported type.");
    }

    protected SchemaAndValue processInt64(R record, Schema inputSchema, long input) {
        throw new UnsupportedOperationException("INT64 is not a supported type.");
    }

    protected SchemaAndValue processBoolean(R record, Schema inputSchema, boolean input) {
        throw new UnsupportedOperationException("BOOLEAN is not a supported type.");
    }

    protected SchemaAndValue processTimestamp(R record, Schema inputSchema, Date input) {
        throw new UnsupportedOperationException("Timestamp is not a supported type.");
    }

    protected SchemaAndValue processDate(R record, Schema inputSchema, Date input) {
        throw new UnsupportedOperationException("Date is not a supported type.");
    }

    protected SchemaAndValue processTime(R record, Schema inputSchema, Date input) {
        throw new UnsupportedOperationException("Time is not a supported type.");
    }

    protected SchemaAndValue processDecimal(R record, Schema inputSchema, BigDecimal input) {
        throw new UnsupportedOperationException("Decimal is not a supported type.");
    }

    protected SchemaAndValue processFloat64(R record, Schema inputSchema, double input) {
        throw new UnsupportedOperationException("FLOAT64 is not a supported type.");
    }

    protected SchemaAndValue processFloat32(R record, Schema inputSchema, float input) {
        throw new UnsupportedOperationException("FLOAT32 is not a supported type.");
    }

    protected SchemaAndValue processArray(R record, Schema inputSchema, List<Object> input) {
        throw new UnsupportedOperationException("ARRAY is not a supported type.");
    }

    protected SchemaAndValue processMap(R record, Schema inputSchema, Map<Object, Object> input) {
        throw new UnsupportedOperationException("MAP is not a supported type.");
    }


    private static final Schema OPTIONAL_TIMESTAMP = Timestamp.builder().optional().build();
    protected SchemaAndValue process(R record, Schema inputSchema, Object input) {
        final SchemaAndValue result;
        if (null == inputSchema && null == input) {
            return new SchemaAndValue(
                    null,
                    null
            );
        }
        if (input instanceof Map) {
            log.trace("process() - Processing as map");
            result = processMap(record, (Map<String, Object>) input);
            return result;
        }

        if (null == inputSchema) {
            log.trace("process() - Determining schema");
            inputSchema = SchemaHelper.schema(input);
        }

        String schemaName = inputSchema.getName();
        FieldType schemaType =  inputSchema.getFieldType();
        log.trace("process() - Input has as schema. schema = {}", inputSchema);
        if (FieldType.STRUCT == schemaType) {
            result = processStruct(record, inputSchema, (Struct) input);
        } else if (Timestamp.LOGICAL_NAME.equals(schemaName)) {
            result = processTimestamp(record, inputSchema, (Date) input);
        } else if (io.openmessaging.connector.api.data.logical.Date.LOGICAL_NAME.equals(schemaName)) {
            result = processDate(record, inputSchema, (Date) input);
        } else if (Time.LOGICAL_NAME.equals(schemaName)) {
            result = processTime(record, inputSchema, (Date) input);
        } else if (Decimal.LOGICAL_NAME.equals(schemaName)) {
            result = processDecimal(record, inputSchema, (BigDecimal) input);
        } else if (FieldType.STRING == schemaType) {
            result = processString(record, inputSchema, (String) input);
        } else if (FieldType.BYTES == schemaType) {
            result = processBytes(record, inputSchema, (byte[]) input);
        } else if (FieldType.INT8 == schemaType) {
            result = processInt8(record, inputSchema, (byte) input);
        } else if (FieldType.INT16 == schemaType) {
            result = processInt16(record, inputSchema, (short) input);
        } else if (FieldType.INT32 == schemaType) {
            result = processInt32(record, inputSchema, (int) input);
        } else if (FieldType.INT64 == schemaType) {
            result = processInt64(record, inputSchema, (long) input);
        } else if (FieldType.FLOAT32 == schemaType) {
            result = processFloat32(record, inputSchema, (float) input);
        } else if (FieldType.FLOAT64 == schemaType) {
            result = processFloat64(record, inputSchema, (double) input);
        } else if (FieldType.ARRAY == schemaType) {
            result = processArray(record, inputSchema, (List<Object>) input);
        } else if (FieldType.MAP == schemaType) {
            result = processMap(record, inputSchema, (Map<Object, Object>) input);
        } else {
            throw new UnsupportedOperationException(
                    String.format(
                            "Schema is not supported. type='%s' name='%s'",
                            schemaType,
                            schemaName
                    )
            );
        }

        return result;
    }

    @Override
    public void stop() {
        // NO-op
    }
}