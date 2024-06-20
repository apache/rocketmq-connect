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
import java.util.Optional;
import org.apache.rocketmq.connect.doris.converter.type.doris.DorisType;
import org.apache.rocketmq.connect.doris.converter.type.doris.DorisTypeProperties;

/**
 * An abstract temporal implementation of {@link Type} for {@code TIME} based columns.
 */
public abstract class AbstractTimeType extends AbstractTemporalType {

    @Override
    public String getTypeName(Schema schema) {
        // NOTE:
        // The MySQL connector does not use the __debezium.source.column.scale parameter to pass
        // the time column's precision but instead uses the __debezium.source.column.length key
        // which differs from all other connector implementations.
        //
        final int precision = getTimePrecision(schema);
        return String.format(
            "%s(%s)",
            DorisType.DATETIME,
            Math.min(precision, DorisTypeProperties.MAX_SUPPORTED_DATE_TIME_PRECISION));
    }

    protected int getTimePrecision(Schema schema) {
        final String length = getSourceColumnLength(schema).orElse("0");
        final Optional<String> scale = getSourceColumnPrecision(schema);
        return scale.map(Integer::parseInt).orElseGet(() -> Integer.parseInt(length));
    }
}
