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

package org.apache.rocketmq.connect.doris.converter;

import java.util.HashMap;
import java.util.Map;
import org.apache.rocketmq.connect.doris.cfg.DorisOptions;
import org.apache.rocketmq.connect.doris.converter.type.Type;
import org.apache.rocketmq.connect.doris.converter.type.connect.ConnectBooleanType;
import org.apache.rocketmq.connect.doris.converter.type.connect.ConnectBytesType;
import org.apache.rocketmq.connect.doris.converter.type.connect.ConnectDateType;
import org.apache.rocketmq.connect.doris.converter.type.connect.ConnectDecimalType;
import org.apache.rocketmq.connect.doris.converter.type.connect.ConnectFloat32Type;
import org.apache.rocketmq.connect.doris.converter.type.connect.ConnectFloat64Type;
import org.apache.rocketmq.connect.doris.converter.type.connect.ConnectInt16Type;
import org.apache.rocketmq.connect.doris.converter.type.connect.ConnectInt32Type;
import org.apache.rocketmq.connect.doris.converter.type.connect.ConnectInt64Type;
import org.apache.rocketmq.connect.doris.converter.type.connect.ConnectInt8Type;
import org.apache.rocketmq.connect.doris.converter.type.connect.ConnectMapToConnectStringType;
import org.apache.rocketmq.connect.doris.converter.type.connect.ConnectStringType;
import org.apache.rocketmq.connect.doris.converter.type.connect.ConnectTimeType;
import org.apache.rocketmq.connect.doris.converter.type.connect.ConnectTimestampType;
import org.apache.rocketmq.connect.doris.converter.type.debezium.ArrayType;
import org.apache.rocketmq.connect.doris.converter.type.debezium.DateType;
import org.apache.rocketmq.connect.doris.converter.type.debezium.GeographyType;
import org.apache.rocketmq.connect.doris.converter.type.debezium.GeometryType;
import org.apache.rocketmq.connect.doris.converter.type.debezium.MicroTimeType;
import org.apache.rocketmq.connect.doris.converter.type.debezium.MicroTimestampType;
import org.apache.rocketmq.connect.doris.converter.type.debezium.NanoTimeType;
import org.apache.rocketmq.connect.doris.converter.type.debezium.NanoTimestampType;
import org.apache.rocketmq.connect.doris.converter.type.debezium.PointType;
import org.apache.rocketmq.connect.doris.converter.type.debezium.TimeType;
import org.apache.rocketmq.connect.doris.converter.type.debezium.TimestampType;
import org.apache.rocketmq.connect.doris.converter.type.debezium.VariableScaleDecimalType;
import org.apache.rocketmq.connect.doris.converter.type.debezium.ZonedTimeType;
import org.apache.rocketmq.connect.doris.converter.type.debezium.ZonedTimestampType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RecordTypeRegister {

    private static final Logger LOG = LoggerFactory.getLogger(RecordTypeRegister.class);

    private final Map<String, Type> typeRegistry = new HashMap<>();
    private final DorisOptions dorisOptions;

    public RecordTypeRegister(DorisOptions dorisOptions) {
        this.dorisOptions = dorisOptions;
        registerTypes();
    }

    protected void registerTypes() {
        // Supported common Debezium data types
        registerType(DateType.INSTANCE);
        registerType(TimeType.INSTANCE);
        registerType(MicroTimeType.INSTANCE);
        registerType(TimestampType.INSTANCE);
        registerType(MicroTimestampType.INSTANCE);
        registerType(NanoTimeType.INSTANCE);
        registerType(NanoTimestampType.INSTANCE);
        registerType(ZonedTimeType.INSTANCE);
        registerType(ZonedTimestampType.INSTANCE);
        registerType(VariableScaleDecimalType.INSTANCE);
        registerType(PointType.INSTANCE);
        registerType(GeographyType.INSTANCE);
        registerType(GeometryType.INSTANCE);
        registerType(ArrayType.INSTANCE);

        // Supported connect data types
        registerType(ConnectBooleanType.INSTANCE);
        registerType(ConnectBytesType.INSTANCE);
        registerType(ConnectDateType.INSTANCE);
        registerType(ConnectDecimalType.INSTANCE);
        registerType(ConnectFloat32Type.INSTANCE);
        registerType(ConnectFloat64Type.INSTANCE);
        registerType(ConnectInt8Type.INSTANCE);
        registerType(ConnectInt16Type.INSTANCE);
        registerType(ConnectInt32Type.INSTANCE);
        registerType(ConnectInt64Type.INSTANCE);
        registerType(ConnectStringType.INSTANCE);
        registerType(ConnectTimestampType.INSTANCE);
        registerType(ConnectTimeType.INSTANCE);
        registerType(ConnectMapToConnectStringType.INSTANCE);
    }

    protected void registerType(Type type) {
        type.configure(dorisOptions);
        for (String key : type.getRegistrationKeys()) {
            final Type existing = typeRegistry.put(key, type);
            if (existing != null) {
                LOG.debug(
                    "Type replaced [{}]: {} -> {}",
                    key,
                    existing.getClass().getName(),
                    type.getClass().getName());
            } else {
                LOG.debug("Type registered [{}]: {}", key, type.getClass().getName());
            }
        }
    }

    public Map<String, Type> getTypeRegistry() {
        return typeRegistry;
    }
}
