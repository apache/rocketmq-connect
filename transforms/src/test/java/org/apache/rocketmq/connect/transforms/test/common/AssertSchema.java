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
import io.openmessaging.connector.api.data.Field;
import io.openmessaging.connector.api.data.Schema;
import org.junit.jupiter.api.Assertions;

import java.util.List;

public class AssertSchema {
    private AssertSchema() {
    }

    public static void assertSchema(Schema expected, Schema actual) {
        assertSchema(expected, actual, (String)null);
    }

    public static void assertSchema(Schema expected, Schema actual, String message) {
        String prefix = Strings.isNullOrEmpty(message) ? "" : message + ": ";
        if (null == expected) {
            Assertions.assertNull(actual, prefix + "actual should not be null.");
        } else {
            Assertions.assertNotNull(expected, prefix + "expected schema should not be null.");
            Assertions.assertNotNull(actual, prefix + "actual schema should not be null.");
            Assertions.assertEquals(expected.getName(), actual.getName(), prefix + "schema.name() should match.");
            Assertions.assertEquals(expected.getFieldType(), actual.getFieldType(), prefix + "schema.type() should match.");
            Assertions.assertEquals(expected.getDefaultValue(), actual.getDefaultValue(), prefix + "schema.defaultValue() should match.");
            Assertions.assertEquals(expected.isOptional(), actual.isOptional(), prefix + "schema.isOptional() should match.");
            Assertions.assertEquals(expected.getDoc(), actual.getDoc(), prefix + "schema.doc() should match.");
            Assertions.assertEquals(expected.getVersion(), actual.getVersion(), prefix + "schema.version() should match.");
            GenericAssertions.assertMap(expected.getParameters(), actual.getParameters(), prefix + "schema.parameters() should match.");
            if (null != expected.getDefaultValue()) {
                Assertions.assertNotNull(actual.getDefaultValue(), "actual.defaultValue() should not be null.");
                Class<?> expectedType = null;
                switch(expected.getFieldType()) {
                case INT8:
                    expectedType = Byte.class;
                    break;
                case INT16:
                    expectedType = Short.class;
                    break;
                case INT32:
                    expectedType = Integer.class;
                    break;
                case INT64:
                    expectedType = Long.class;
                    break;
                case FLOAT32:
                    expectedType = Float.class;
                    break;
                case FLOAT64:
                    expectedType = Float.class;
                }

                if (null != expectedType) {
                    Assertions.assertTrue(actual.getDefaultValue().getClass().isAssignableFrom(expectedType), String.format("actual.defaultValue() should be a %s", expectedType.getSimpleName()));
                }
            }

            switch(expected.getFieldType()) {
            case ARRAY:
                assertSchema(expected.getValueSchema(), actual.getValueSchema(), message + "valueSchema does not match.");
                break;
            case MAP:
                assertSchema(expected.getKeySchema(), actual.getKeySchema(), message + "keySchema does not match.");
                assertSchema(expected.getValueSchema(), actual.getValueSchema(), message + "valueSchema does not match.");
                break;
            case STRUCT:
                List<Field> expectedFields = expected.getFields();
                List<Field> actualFields = actual.getFields();
                Assertions.assertEquals(expectedFields.size(), actualFields.size(), prefix + "Number of fields do not match.");

                for(int i = 0; i < expectedFields.size(); ++i) {
                    Field expectedField = (Field)expectedFields.get(i);
                    Field actualField = (Field)actualFields.get(i);
                    assertField(expectedField, actualField, "index " + i);
                }
            }

        }
    }

    public static void assertField(Field expected, Field actual, String message) {
        String prefix = Strings.isNullOrEmpty(message) ? "" : message + ": ";
        if (null == expected) {
            Assertions.assertNull(actual, prefix + "actual should be null.");
        } else {
            Assertions.assertEquals(expected.getName(), actual.getName(), prefix + "name does not match");
            Assertions.assertEquals(expected.getIndex(), actual.getIndex(), prefix + "name does not match");
            assertSchema(expected.getSchema(), actual.getSchema(), prefix + "schema does not match");
        }
    }

    public static void assertField(Field expected, Field actual) {
        assertField(expected, actual, (String)null);
    }
}
