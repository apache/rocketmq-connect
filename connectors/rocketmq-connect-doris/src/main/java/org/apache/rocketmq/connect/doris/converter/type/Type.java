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

package org.apache.rocketmq.connect.doris.converter.type;

import io.openmessaging.connector.api.data.Schema;
import org.apache.rocketmq.connect.doris.cfg.DorisOptions;

/**
 * A type indicates the type of each column of kafka record, including various column types of
 * debezium and connect.
 */
public interface Type {

    /**
     * Allows a type to perform initialization/configuration tasks based on user configs.
     */
    void configure(DorisOptions dorisOptions);

    /**
     * Returns the names that this type will be mapped as.
     *
     * <p>For example, when creating a custom mapping for {@code io.debezium.data.Bits}, a type
     * could be registered using the {@code LOGICAL_NAME} of the schema if the type is to be used
     * when a schema name is identified; otherwise it could be registered as the raw column type
     * when column type propagation is enabled.
     */
    String[] getRegistrationKeys();

    /**
     * Get the actual converted value based on the column type.
     */
    Object getValue(Object sourceValue);

    default Object getValue(Object sourceValue, Schema schema) {
        return getValue(sourceValue);
    }

    String getTypeName(Schema schema);

    boolean isNumber();
}
