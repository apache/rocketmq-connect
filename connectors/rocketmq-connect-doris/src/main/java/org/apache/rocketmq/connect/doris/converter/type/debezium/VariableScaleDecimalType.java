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

import io.debezium.data.SpecialValueDecimal;
import io.debezium.data.VariableScaleDecimal;
import io.openmessaging.connector.api.data.Schema;
import io.openmessaging.connector.api.data.Struct;
import io.openmessaging.connector.api.errors.ConnectException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Optional;
import org.apache.rocketmq.connect.doris.converter.type.AbstractType;
import org.apache.rocketmq.connect.doris.converter.type.doris.DorisType;

public class VariableScaleDecimalType extends AbstractType {

    public static final VariableScaleDecimalType INSTANCE = new VariableScaleDecimalType();

    @Override
    public String[] getRegistrationKeys() {
        return new String[] {VariableScaleDecimal.LOGICAL_NAME};
    }

    @Override
    public Object getValue(Object sourceValue) {
        if (sourceValue == null) {
            return null;
        }
        if (sourceValue instanceof Struct) {
            Optional<BigDecimal> bigDecimalValue = toLogical((Struct) sourceValue).getDecimalValue();
            return bigDecimalValue.get();
        }

        throw new ConnectException(
            String.format(
                "Unexpected %s value '%s' with type '%s'",
                getClass().getSimpleName(), sourceValue, sourceValue.getClass().getName()));
    }

    @Override
    public String getTypeName(Schema schema) {
        // The data passed by VariableScaleDecimal data types does not provide adequate information to
        // resolve the precision and scale for the data type, so instead we're going to default to the
        // maximum double-based data types for the dialect, using DOUBLE.
        return DorisType.DOUBLE;
    }

    @Override
    public boolean isNumber() {
        return true;
    }

    private static SpecialValueDecimal toLogical(final Struct value) {
        return new SpecialValueDecimal(
            new BigDecimal(new BigInteger(value.getBytes("value")), value.getInt32("scale")));
    }
}
