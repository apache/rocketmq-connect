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

package org.apache.rocketmq.schema.common;

import java.util.Map;

/**
 * abstract converter config
 */
public abstract class AbstractConverterConfig {
    /**
     * Set schema registry url
     */
    public static final String SCHEMA_REGISTRY_URL = "schema.registry.url";

    /**
     * Convert message key or value
     */
    public static final String IS_KEY = "isKey";
    /**
     * Specify serializer and deserializer registry id
     */
    public static final String SERDE_SCHEMA_REGISTRY_ID = "serde.schema.registry.id";
    /**
     * Specify schema cache size
     */
    public static final String SCHEMAS_CACHE_SIZE_CONFIG = "schemas.cache.size";
    /**
     * auto registry schema
     */
    public static final String AUTO_REGISTER_SCHEMAS = "auto.register.schemas";
    /**
     * use latest version
     */
    public static final String USE_LATEST_VERSION = "use.latest.version";
    private static final boolean IS_KEY_DEFAULT = false;
    private static final int SCHEMAS_CACHE_SIZE_DEFAULT = 1000;
    private static final boolean AUTO_REGISTER_SCHEMAS_DEFAULT = true;
    private static final boolean USE_LATEST_VERSION_DEFAULT = false;

    private final String schemaRegistryUrl;
    private final boolean isKey;
    private final Long serdeSchemaRegistryId;

    private final Integer schemasCacheSize;
    private final boolean autoRegisterSchemas;
    private final boolean useLatestVersion;

    /**
     * abstract converter config
     *
     * @param props
     */
    public AbstractConverterConfig(Map<String, ?> props) {
        if (!props.containsKey(SCHEMA_REGISTRY_URL)) {
            throw new IllegalArgumentException("Config [" + SCHEMA_REGISTRY_URL + "] can not empty");
        }
        this.schemaRegistryUrl = String.valueOf(props.get(SCHEMA_REGISTRY_URL));
        if (!props.containsKey(IS_KEY)) {
            throw new IllegalArgumentException("Config [" + IS_KEY + "] can not empty, Please specify the type of serialization");
        }
        this.isKey = Boolean.valueOf(props.get(IS_KEY).toString());
        serdeSchemaRegistryId = props.containsKey(SERDE_SCHEMA_REGISTRY_ID) ?
                Long.valueOf(props.get(SERDE_SCHEMA_REGISTRY_ID).toString()) : null;
        schemasCacheSize = props.containsKey(SCHEMAS_CACHE_SIZE_CONFIG) ?
                Integer.parseInt(props.get(SCHEMAS_CACHE_SIZE_CONFIG).toString()) : SCHEMAS_CACHE_SIZE_DEFAULT;
        autoRegisterSchemas = props.containsKey(AUTO_REGISTER_SCHEMAS) ?
                Boolean.valueOf(props.get(AUTO_REGISTER_SCHEMAS).toString()) : AUTO_REGISTER_SCHEMAS_DEFAULT;
        useLatestVersion = props.containsKey(USE_LATEST_VERSION) ?
                Boolean.valueOf(props.get(USE_LATEST_VERSION).toString()) : USE_LATEST_VERSION_DEFAULT;
    }

    public String getSchemaRegistryUrl() {
        return schemaRegistryUrl;
    }

    public boolean isKey() {
        return isKey;
    }

    public Long getSerdeSchemaRegistryId() {
        return serdeSchemaRegistryId;
    }

    public Integer getSchemasCacheSize() {
        return schemasCacheSize;
    }

    public boolean isAutoRegisterSchemas() {
        return autoRegisterSchemas;
    }

    public boolean isUseLatestVersion() {
        return useLatestVersion;
    }
}
