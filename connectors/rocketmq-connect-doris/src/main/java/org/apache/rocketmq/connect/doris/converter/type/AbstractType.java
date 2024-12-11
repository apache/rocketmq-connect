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
import java.util.Objects;
import java.util.Optional;
import org.apache.rocketmq.connect.doris.cfg.DorisOptions;
import org.apache.rocketmq.connect.doris.converter.type.util.SchemaUtils;

/**
 * An abstract implementation of {@link Type}, which all types should extend.
 */
public abstract class AbstractType implements Type {

    @Override
    public void configure(DorisOptions dorisOptions) {
    }

    @Override
    public Object getValue(Object sourceValue) {
        return sourceValue;
    }

    @Override
    public boolean isNumber() {
        return false;
    }

    @Override
    public String toString() {
        return getClass().getSimpleName();
    }

    protected Optional<String> getSchemaParameter(Schema schema, String parameterName) {
        if (!Objects.isNull(schema.getParameters())) {
            return Optional.ofNullable(schema.getParameters().get(parameterName));
        }
        return Optional.empty();
    }

    protected Optional<String> getSourceColumnType(Schema schema) {
        return SchemaUtils.getSourceColumnType(schema);
    }

    protected Optional<String> getSourceColumnLength(Schema schema) {
        return SchemaUtils.getSourceColumnLength(schema);
    }

    protected Optional<String> getSourceColumnPrecision(Schema schema) {
        return SchemaUtils.getSourceColumnPrecision(schema);
    }
}
