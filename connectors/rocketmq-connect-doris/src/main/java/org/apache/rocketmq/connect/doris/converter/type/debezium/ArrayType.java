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

package org.apache.rocketmq.connect.doris.converter.type.debezium;

import io.openmessaging.connector.api.data.Schema;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.apache.rocketmq.connect.doris.cfg.DorisOptions;
import org.apache.rocketmq.connect.doris.converter.RecordTypeRegister;
import org.apache.rocketmq.connect.doris.converter.type.AbstractType;
import org.apache.rocketmq.connect.doris.converter.type.Type;
import org.apache.rocketmq.connect.doris.converter.type.doris.DorisType;

public class ArrayType extends AbstractType {
    private static final String ARRAY_TYPE_TEMPLATE = "%s<%s>";
    public static final ArrayType INSTANCE = new ArrayType();
    private DorisOptions dorisOptions;
    private RecordTypeRegister recordTypeRegister;

    @Override
    public void configure(DorisOptions dorisOptions) {
        if (this.dorisOptions == null && this.recordTypeRegister == null) {
            this.dorisOptions = dorisOptions;
            registerNestedArrayType();
        }
    }

    @Override
    public String[] getRegistrationKeys() {
        return new String[] {"ARRAY"};
    }

    @Override
    public String getTypeName(Schema schema) {
        if (schema.getValueSchema().isOptional()) {
            Schema valueSchema = schema.getValueSchema();
            String type =
                Objects.nonNull(valueSchema.getName())
                    ? valueSchema.getName()
                    : valueSchema.getFieldType().name();
            if (recordTypeRegister == null) {
                registerNestedArrayType();
            }
            Type valueType = recordTypeRegister.getTypeRegistry().get(type);
            if (valueType == null) {
                return DorisType.STRING;
            }
            String typeName = valueType.getTypeName(schema);
            return String.format(ARRAY_TYPE_TEMPLATE, DorisType.ARRAY, typeName);
        }
        return DorisType.STRING;
    }

    @Override
    public Object getValue(Object sourceValue, Schema schema) {

        if (sourceValue == null) {
            return null;
        }
        Schema valueSchema = schema.getValueSchema();
        String type =
            Objects.nonNull(valueSchema.getName())
                ? valueSchema.getName()
                : valueSchema.getFieldType().name();

        if (sourceValue instanceof List) {
            List<Object> resultList = new ArrayList<>();
            ArrayList<?> convertedValue = (ArrayList<?>) sourceValue;
            if (recordTypeRegister == null) {
                registerNestedArrayType();
            }
            Type valueType = recordTypeRegister.getTypeRegistry().get(type);
            if (valueType == null) {
                return sourceValue;
            }

            for (Object value : convertedValue) {
                resultList.add(valueType.getValue(value, valueSchema));
            }
            return resultList;
        }

        return sourceValue;
    }

    private void registerNestedArrayType() {
        this.recordTypeRegister = new RecordTypeRegister(dorisOptions);
    }
}
