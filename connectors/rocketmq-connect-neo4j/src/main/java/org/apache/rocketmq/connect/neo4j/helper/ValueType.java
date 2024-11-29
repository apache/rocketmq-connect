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

package org.apache.rocketmq.connect.neo4j.helper;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

import com.alibaba.fastjson.JSONObject;

public enum ValueType {
    /**
     * transfer gdb element object value to DataX Column data
     * <p>
     * int, long -> LongColumn
     * float, double -> DoubleColumn
     * bool -> BooleanColumn
     * string -> StringColumn
     */
    INT(Integer.class, "int", ValueTypeHolder::longColumnMapper),
    INTEGER(Integer.class, "integer", ValueTypeHolder::longColumnMapper),
    LONG(Long.class, "long", ValueTypeHolder::longColumnMapper),
    DOUBLE(Double.class, "double", ValueTypeHolder::doubleColumnMapper),
    FLOAT(Float.class, "float", ValueTypeHolder::doubleColumnMapper),
    BOOLEAN(Boolean.class, "boolean", ValueTypeHolder::boolColumnMapper),
    STRING(String.class, "string", ValueTypeHolder::stringColumnMapper),
    ;

    private Class<?> type = null;
    private String shortName = null;
    private Function<Object, Object> columnFunc = null;

    ValueType(Class<?> type, String name, Function<Object, Object> columnFunc) {
        this.type = type;
        this.shortName = name;
        this.columnFunc = columnFunc;

        ValueTypeHolder.shortName2type.put(shortName, this);
    }

    public static ValueType fromShortName(String name) {
        return ValueTypeHolder.shortName2type.get(name);
    }

    public Object applyObject(Object value) {
        if (value == null) {
            return null;
        }
        return columnFunc.apply(value);
    }

    private static class ValueTypeHolder {
        private static Map<String, ValueType> shortName2type = new HashMap<>();

        private static Long longColumnMapper(Object o) {
            long v;
            if (o instanceof Integer) {
                v = (int)o;
            } else if (o instanceof Long) {
                v = (long)o;
            } else if (o instanceof String) {
                v = Long.valueOf((String)o);
            } else {
                throw new RuntimeException("Failed to cast " + o.getClass() + " to Long");
            }

            return v;
        }

        private static Double doubleColumnMapper(Object o) {
            double v;
            if (o instanceof Integer) {
                v = (double)(int)o;
            } else if (o instanceof Long) {
                v = (double)(long)o;
            } else if (o instanceof Float) {
                v = (double)(float)o;
            } else if (o instanceof Double) {
                v = (double)o;
            } else if (o instanceof String) {
                v = Double.valueOf((String)o);
            } else {
                throw new RuntimeException("Failed to cast " + o.getClass() + " to Double");
            }

            return v;
        }

        private static Boolean boolColumnMapper(Object o) {
            boolean v;
            if (o instanceof Integer) {
                v = ((int)o != 0);
            } else if (o instanceof Long) {
                v = ((long)o != 0);
            } else if (o instanceof Boolean) {
                v = (boolean)o;
            } else if (o instanceof String) {
                v = Boolean.valueOf((String)o);
            } else {
                throw new RuntimeException("Failed to cast " + o.getClass() + " to Boolean");
            }

            return v;
        }

        private static String stringColumnMapper(Object o) {
            if (o instanceof String) {
                return (String)o;
            } else if (MappingRuleFactory.isPrimitive(o)) {
                return String.valueOf(o);
            } else {
                return JSONObject.toJSONString(o);
            }
        }
    }

}
