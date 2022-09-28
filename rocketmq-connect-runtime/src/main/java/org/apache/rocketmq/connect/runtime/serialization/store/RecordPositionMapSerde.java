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

package org.apache.rocketmq.connect.runtime.serialization.store;

import io.openmessaging.connector.api.data.RecordOffset;
import org.apache.rocketmq.connect.runtime.serialization.Deserializer;
import org.apache.rocketmq.connect.runtime.serialization.Serializer;
import org.apache.rocketmq.connect.runtime.serialization.WrapperSerde;
import org.apache.rocketmq.connect.runtime.store.ExtendRecordPartition;

import java.util.Map;

/**
 * Byte Map to byte[].
 */
public class RecordPositionMapSerde extends WrapperSerde<Map<ExtendRecordPartition, RecordOffset>> {

    public RecordPositionMapSerde(Serializer<Map<ExtendRecordPartition, RecordOffset>> serializer, Deserializer<Map<ExtendRecordPartition, RecordOffset>> deserializer) {
        super(serializer, deserializer);
    }

    /**
     * serializer and deserializer
     *
     * @return
     */
    public static RecordPositionMapSerde serde() {
        return new RecordPositionMapSerde(new RecordPositionMapSerializer(), new RecordPositionMapDeserializer());
    }
}
