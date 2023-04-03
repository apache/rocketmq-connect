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

import org.apache.rocketmq.schema.common.AbstractConverterConfig;

import java.util.Map;

/**
 * Avro converter config
 */
public class AvroConverterConfig extends AbstractConverterConfig {

    public static final String SCHEMA_REFLECTION_CONFIG = "schema.reflection";
    public static final boolean SCHEMA_REFLECTION_DEFAULT = false;
    public static final String SCHEMA_REFLECTION_DOC =
            "If true, uses the reflection API when serializing/deserializing ";

    public static final String AVRO_USE_LOGICAL_TYPE_CONVERTERS_CONFIG = "avro.use.logical.type.converters";
    public static final boolean AVRO_USE_LOGICAL_TYPE_CONVERTERS_DEFAULT = false;
    public static final String AVRO_USE_LOGICAL_TYPE_CONVERTERS_DOC =
            "If true, use logical type converter in generic record";


    public static final String SPECIFIC_AVRO_READER_CONFIG = "specific.avro.reader";
    public static final boolean SPECIFIC_AVRO_READER_DEFAULT = false;
    public static final String SPECIFIC_AVRO_READER_DOC =
            "If true, tries to look up the SpecificRecord class ";

    public static final String AVRO_REFLECTION_ALLOW_NULL_CONFIG = "avro.reflection.allow.null";
    public static final boolean AVRO_REFLECTION_ALLOW_NULL_DEFAULT = false;
    public static final String AVRO_REFLECTION_ALLOW_NULL_DOC =
            "If true, allows null field values used in ReflectionAvroDeserializer";


    private final Map<String, ?> props;

    public AvroConverterConfig(Map<String, ?> props) {
        super(props);
        this.props = props;
    }


    public boolean useSchemaReflection() {
        return props.containsKey(SCHEMA_REFLECTION_CONFIG) ?
                Boolean.valueOf(props.get(SCHEMA_REFLECTION_CONFIG).toString()) : SCHEMA_REFLECTION_DEFAULT;
    }

    public boolean avroUseLogicalTypeConverters() {
        return props.containsKey(AVRO_USE_LOGICAL_TYPE_CONVERTERS_CONFIG) ?
                Boolean.valueOf(props.get(AVRO_USE_LOGICAL_TYPE_CONVERTERS_CONFIG).toString()) : AVRO_USE_LOGICAL_TYPE_CONVERTERS_DEFAULT;
    }

    public boolean specificAvroReaderConfig() {
        return props.containsKey(SPECIFIC_AVRO_READER_CONFIG) ?
                Boolean.valueOf(props.get(SPECIFIC_AVRO_READER_CONFIG).toString()) : SPECIFIC_AVRO_READER_DEFAULT;

    }

    public boolean avroReflectionAllowNullConfig() {
        return props.containsKey(AVRO_REFLECTION_ALLOW_NULL_CONFIG) ?
                Boolean.valueOf(props.get(AVRO_REFLECTION_ALLOW_NULL_CONFIG).toString()) : AVRO_REFLECTION_ALLOW_NULL_DEFAULT;

    }
}
