/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rocketmq.connect.runtime.converter.record.json;

import com.alibaba.fastjson.JSONObject;

import java.util.concurrent.ConcurrentHashMap;

/**
 * json schema
 */
public class JsonSchema {

    public static final String ENVELOPE_SCHEMA_FIELD_NAME = "schema";
    public static final String ENVELOPE_PAYLOAD_FIELD_NAME = "payload";
    public static final String SCHEMA_TYPE_FIELD_NAME = "type";
    public static final String SCHEMA_OPTIONAL_FIELD_NAME = "optional";
    public static final String SCHEMA_NAME_FIELD_NAME = "name";
    public static final String SCHEMA_VERSION_FIELD_NAME = "version";
    public static final String SCHEMA_DOC_FIELD_NAME = "doc";
    public static final String SCHEMA_PARAMETERS_FIELD_NAME = "parameters";
    public static final String SCHEMA_DEFAULT_FIELD_NAME = "default";
    public static final String ARRAY_ITEMS_FIELD_NAME = "items";
    public static final String MAP_KEY_FIELD_NAME = "keys";
    public static final String MAP_VALUE_FIELD_NAME = "values";
    public static final String STRUCT_FIELDS_FIELD_NAME = "fields";
    public static final String STRUCT_FIELD_NAME_FIELD_NAME = "field";
    public static final String BOOLEAN_TYPE_NAME = "boolean";
    public static final String INT8_TYPE_NAME = "int8";
    public static final String INT16_TYPE_NAME = "int16";
    public static final String INT32_TYPE_NAME = "int32";
    public static final String INT64_TYPE_NAME = "int64";
    public static final String FLOAT_TYPE_NAME = "float";
    public static final String DOUBLE_TYPE_NAME = "double";
    public static final String BYTES_TYPE_NAME = "bytes";
    public static final String STRING_TYPE_NAME = "string";
    public static final String ARRAY_TYPE_NAME = "array";
    public static final String MAP_TYPE_NAME = "map";
    public static final String STRUCT_TYPE_NAME = "struct";

    public static JSONObject BOOLEAN_SCHEMA() {
        return newJSONObject(SCHEMA_TYPE_FIELD_NAME, BOOLEAN_TYPE_NAME);
    }

    public static JSONObject INT8_SCHEMA() {
        return newJSONObject(SCHEMA_TYPE_FIELD_NAME, INT8_TYPE_NAME);
    }

    public static JSONObject INT16_SCHEMA() {
        return newJSONObject(SCHEMA_TYPE_FIELD_NAME, INT16_TYPE_NAME);
    }

    public static JSONObject INT32_SCHEMA() {
        return newJSONObject(SCHEMA_TYPE_FIELD_NAME, INT32_TYPE_NAME);
    }

    public static JSONObject INT64_SCHEMA() {
        return newJSONObject(SCHEMA_TYPE_FIELD_NAME, INT64_TYPE_NAME);
    }

    public static JSONObject FLOAT_SCHEMA() {
        return newJSONObject(SCHEMA_TYPE_FIELD_NAME, FLOAT_TYPE_NAME);
    }

    public static JSONObject DOUBLE_SCHEMA() {
        return newJSONObject(SCHEMA_TYPE_FIELD_NAME, DOUBLE_TYPE_NAME);
    }

    public static JSONObject BYTES_SCHEMA() {
        return newJSONObject(SCHEMA_TYPE_FIELD_NAME, BYTES_TYPE_NAME);
    }

    public static JSONObject STRING_SCHEMA() {
        return newJSONObject(SCHEMA_TYPE_FIELD_NAME, STRING_TYPE_NAME);
    }

    private static JSONObject newJSONObject(String key, Object value) {
        JSONObject object = new JSONObject(new ConcurrentHashMap<>());
        object.put(key, value);
        return object;
    }


    public static JSONObject envelope(JSONObject schema, Object payload) {
        JSONObject result = new JSONObject();
        result.put(ENVELOPE_SCHEMA_FIELD_NAME, schema);
        result.put(ENVELOPE_PAYLOAD_FIELD_NAME, payload);
        return result;
    }

    static class Envelope {
        public JSONObject schema;
        public Object payload;

        public Envelope(JSONObject schema, Object payload) {
            this.schema = schema;
            this.payload = payload;
        }

        public JSONObject toJsonNode() {
            return envelope(schema, payload);
        }
    }
}
