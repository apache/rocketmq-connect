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
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.apache.rocketmq.connect.runtime.rest.entities;

import io.openmessaging.connector.api.component.connector.Connector;
import io.openmessaging.connector.api.component.task.sink.SinkConnector;
import io.openmessaging.connector.api.component.task.source.SourceConnector;

import java.util.Locale;

/**
 * connector type
 */
public enum ConnectorType {
    SOURCE, SINK, UNKNOWN;

    public static ConnectorType from(Class<? extends Connector> clazz) {
        if (SinkConnector.class.isAssignableFrom(clazz)) {
            return SINK;
        }
        if (SourceConnector.class.isAssignableFrom(clazz)) {
            return SOURCE;
        }

        return UNKNOWN;
    }

    public static ConnectorType forValue(String value) {
        return ConnectorType.valueOf(value.toUpperCase(Locale.ROOT));
    }

    @Override
    public String toString() {
        return super.toString().toLowerCase(Locale.ROOT);
    }
}
