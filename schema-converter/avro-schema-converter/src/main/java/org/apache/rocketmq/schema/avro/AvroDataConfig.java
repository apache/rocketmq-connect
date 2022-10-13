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

package org.apache.rocketmq.schema.avro;


import java.util.Map;


/**
 * avro data config
 */
public class AvroDataConfig {

    public static final String ENHANCED_AVRO_SCHEMA_SUPPORT_CONFIG = "enhanced.avro.schema.support";
    public static final String CONNECT_META_DATA_CONFIG = "connect.meta.data";
    public static final String SCHEMAS_CACHE_SIZE_CONFIG = "schemas.cache.config";
    private static final boolean ENHANCED_AVRO_SCHEMA_SUPPORT_DEFAULT = false;
    private static final String ENHANCED_AVRO_SCHEMA_SUPPORT_DOC =
            "Toggle for enabling/disabling enhanced avro schema support: Enum symbol preservation and "
                    + "Package Name awareness";
    private static final boolean CONNECT_META_DATA_DEFAULT = true;
    private static final String CONNECT_META_DATA_DOC =
            "Toggle for enabling/disabling connect converter to add its meta data to the output schema "
                    + "or not";
    private static final int SCHEMAS_CACHE_SIZE_DEFAULT = 1000;
    private static final String SCHEMAS_CACHE_SIZE_DOC =
            "Size of the converted schemas cache";

    private final Map<?, ?> props;

    public AvroDataConfig(Map<?, ?> props) {
        this.props = props;
    }

    public boolean isEnhancedAvroSchemaSupport() {
        return props.containsKey(ENHANCED_AVRO_SCHEMA_SUPPORT_CONFIG) ?
                Boolean.valueOf(props.get(ENHANCED_AVRO_SCHEMA_SUPPORT_CONFIG).toString()) : ENHANCED_AVRO_SCHEMA_SUPPORT_DEFAULT;
    }

    public boolean isConnectMetaData() {
        return props.containsKey(CONNECT_META_DATA_CONFIG) ?
                Boolean.valueOf(props.get(CONNECT_META_DATA_CONFIG).toString()) : CONNECT_META_DATA_DEFAULT;
    }

    public int getSchemasCacheSize() {
        return props.containsKey(SCHEMAS_CACHE_SIZE_CONFIG) ?
                Integer.parseInt(props.get(CONNECT_META_DATA_CONFIG).toString()) : SCHEMAS_CACHE_SIZE_DEFAULT;
    }

}
