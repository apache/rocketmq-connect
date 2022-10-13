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

import org.apache.rocketmq.schema.registry.common.model.SchemaType;

import java.util.Objects;

/**
 * parsed schema
 */
public interface ParsedSchema<T> {
    /**
     * get schema type
     *
     * @return
     */
    SchemaType schemaType();

    /**
     * raw schema
     *
     * @return
     */
    T rawSchema();

    /**
     * get schema version
     *
     * @return
     */
    Integer version();

    /**
     * validate data
     */
    default void validate() {
    }

    /**
     * get schema string
     *
     * @return
     */
    String idl();


    /**
     * get schema name
     *
     * @return
     */
    String name();

    /**
     * deep equals
     *
     * @param schema
     * @return
     */
    default boolean deepEquals(ParsedSchema schema) {
        return Objects.equals(rawSchema(), schema.rawSchema());
    }
}
