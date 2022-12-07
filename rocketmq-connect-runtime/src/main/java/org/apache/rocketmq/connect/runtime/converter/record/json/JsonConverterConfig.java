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

import java.util.Locale;
import java.util.Map;

/**
 * Configuration options for {@link JsonConverter} instances.
 */
public class JsonConverterConfig {

    public static final String SCHEMAS_ENABLE_CONFIG = "schemas.enable";
    public static final boolean SCHEMAS_ENABLE_DEFAULT = true;
    public static final String SCHEMAS_CACHE_SIZE_CONFIG = "schemas.cache.size";
    public static final int SCHEMAS_CACHE_SIZE_DEFAULT = 1000;
    public static final String DECIMAL_FORMAT_CONFIG = "decimal.format";
    public static final String DECIMAL_FORMAT_DEFAULT = DecimalFormat.BASE64.name();
    private static final String SCHEMAS_ENABLE_DOC = "Include schemas within each of the serialized values and keys.";
    private static final String SCHEMAS_ENABLE_DISPLAY = "Enable Schemas";
    private static final String SCHEMAS_CACHE_SIZE_DOC = "The maximum number of schemas that can be cached in this converter instance.";
    private static final String SCHEMAS_CACHE_SIZE_DISPLAY = "Schema Cache Size";
    private static final String DECIMAL_FORMAT_DOC = "Controls which format this converter will serialize decimals in."
            + " This value is case insensitive and can be either 'BASE64' (default) or 'NUMERIC'";
    private static final String DECIMAL_FORMAT_DISPLAY = "Decimal Format";

    // cached config values
    private final boolean schemasEnabled;
    private final DecimalFormat decimalFormat;
    private final int cacheSize;

    public JsonConverterConfig(Map<String, ?> props) {
        // schema.enabled
        if (props.containsKey(SCHEMAS_ENABLE_CONFIG)) {
            this.schemasEnabled = (Boolean) props.get(SCHEMAS_ENABLE_CONFIG);
        } else {
            this.schemasEnabled = SCHEMAS_ENABLE_DEFAULT;
        }
        // decimal format
        if (props.containsKey(DECIMAL_FORMAT_CONFIG)) {
            this.decimalFormat = DecimalFormat.valueOf(String.valueOf(props.get(DECIMAL_FORMAT_CONFIG)).toUpperCase(Locale.ROOT));
        } else {
            decimalFormat = DecimalFormat.valueOf(DECIMAL_FORMAT_DEFAULT);
        }
        this.cacheSize = props.containsKey(SCHEMAS_CACHE_SIZE_CONFIG) ? (Integer) props.get(SCHEMAS_CACHE_SIZE_CONFIG) : SCHEMAS_CACHE_SIZE_DEFAULT;

    }

    /**
     * return cache size
     *
     * @return
     */
    public int cacheSize() {
        return cacheSize;
    }

    /**
     * Return whether schemas are enabled.
     *
     * @return true if enabled, or false otherwise
     */
    public boolean schemasEnabled() {
        return schemasEnabled;
    }

    /**
     * Get the serialization format for decimal types.
     *
     * @return the decimal serialization format
     */
    public DecimalFormat decimalFormat() {
        return decimalFormat;
    }

}
