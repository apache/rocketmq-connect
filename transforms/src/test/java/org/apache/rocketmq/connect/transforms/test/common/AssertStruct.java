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
package org.apache.rocketmq.connect.transforms.test.common;

import com.google.common.base.Strings;
import com.google.common.io.BaseEncoding;
import io.openmessaging.connector.api.data.Field;
import io.openmessaging.connector.api.data.Struct;
import io.openmessaging.connector.api.data.logical.Decimal;
import io.openmessaging.connector.api.data.logical.Time;
import io.openmessaging.connector.api.data.logical.Timestamp;
import org.junit.jupiter.api.Assertions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class AssertStruct {
    private static final Logger log = LoggerFactory.getLogger(AssertStruct.class);

    public AssertStruct() {
    }

    static <T> T castAndVerify(Class<T> cls, Struct struct, Field field, boolean expected) {
        Object value = struct.get(field.getName());
        String prefix = String.format("%s('%s') ", expected ? "expected" : "actual", field.getName());
        if (!field.getSchema().isOptional()) {
            Assertions.assertNotNull(value, prefix + "has a require schema. Should not be null.");
        }

        if (null == value) {
            return null;
        } else {
            Assertions.assertTrue(cls.isInstance(value), String.format(prefix + "should be a %s", cls.getSimpleName()));
            return cls.cast(value);
        }
    }

    public static void assertStruct(Struct expected, Struct actual, String message) {
        String prefix = Strings.isNullOrEmpty(message) ? "" : message + ": ";
        if (null == expected) {
            Assertions.assertNull(actual, prefix + "actual should be null.");
        } else {
            AssertSchema.assertSchema(expected.schema(), actual.schema(), "schema does not match.");
            Iterator var4 = expected.schema().getFields().iterator();

            while(true) {
                while(var4.hasNext()) {
                    Field expectedField = (Field)var4.next();
                    log.trace("assertStruct() - testing field '{}'", expectedField.getName());
                    Object expectedValue = expected.get(expectedField.getName());
                    Object actualValue = actual.get(expectedField.getName());
                    if (Decimal.LOGICAL_NAME.equals(expectedField.getSchema().getName())) {
                        BigDecimal expectedDecimal = (BigDecimal)castAndVerify(BigDecimal.class, expected, expectedField, true);
                        BigDecimal actualDecimal = (BigDecimal)castAndVerify(BigDecimal.class, actual, expectedField, false);
                        Assertions.assertEquals(expectedDecimal, actualDecimal, prefix + expectedField.getName() + " does not match.");
                    } else if (!Timestamp.LOGICAL_NAME.equals(expectedField.getSchema().getName()) && !io.openmessaging.connector.api.data.logical.Date.LOGICAL_NAME.equals(expectedField.getSchema().getName()) && !Time.LOGICAL_NAME.equals(expectedField.getSchema().getName())) {
                        switch(expectedField.getSchema().getFieldType()) {
                        case ARRAY:
                            List<Object> expectedArray = (List)castAndVerify(List.class, expected, expectedField, true);
                            List<Object> actualArray = (List)castAndVerify(List.class, actual, expectedField, false);
                            Assertions.assertEquals(expectedArray, actualArray, prefix + expectedField.getName() + " does not match.");
                            break;
                        case MAP:
                            Map<Object, Object> expectedMap = (Map)castAndVerify(Map.class, expected, expectedField, true);
                            Map<Object, Object> actualMap = (Map)castAndVerify(Map.class, actual, expectedField, false);
                            Assertions.assertEquals(expectedMap, actualMap, prefix + expectedField.getName() + " does not match.");
                            break;
                        case STRUCT:
                            Struct expectedStruct = (Struct)castAndVerify(Struct.class, expected, expectedField, true);
                            Struct actualStruct = (Struct)castAndVerify(Struct.class, actual, expectedField, false);
                            assertStruct(expectedStruct, actualStruct, prefix + expectedField.getName() + " does not match.");
                            break;
                        case BYTES:
                            byte[] expectedByteArray = (byte[])castAndVerify(byte[].class, expected, expectedField, true);
                            byte[] actualByteArray = (byte[])castAndVerify(byte[].class, actual, expectedField, false);
                            Assertions.assertEquals(null == expectedByteArray ? "" : BaseEncoding.base32Hex().encode(expectedByteArray).toString(), null == actualByteArray ? "" : BaseEncoding.base32Hex().encode(actualByteArray).toString(), prefix + expectedField.getName() + " does not match.");
                            break;
                        default:
                            Assertions.assertEquals(expectedValue, actualValue, prefix + expectedField.getName() + " does not match.");
                        }
                    } else {
                        Date expectedDate = (Date)castAndVerify(Date.class, expected, expectedField, true);
                        Date actualDate = (Date)castAndVerify(Date.class, actual, expectedField, false);
                        Assertions.assertEquals(expectedDate, actualDate, prefix + expectedField.getName() + " does not match.");
                    }
                }

                return;
            }
        }
    }

    public static void assertStruct(Struct expected, Struct actual) {
        assertStruct(expected, actual, (String)null);
    }
}
