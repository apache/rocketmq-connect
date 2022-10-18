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

package org.apache.rocketmq.connect.runtime.serialization.store;

import io.openmessaging.connector.api.data.RecordOffset;
import java.util.Map;
import org.apache.rocketmq.connect.runtime.serialization.Deserializer;
import org.apache.rocketmq.connect.runtime.serialization.Serializer;
import org.apache.rocketmq.connect.runtime.store.ExtendRecordPartition;
import org.assertj.core.api.Assertions;
import org.junit.Test;

public class RecordPositionMapSerdeTest {

    @Test
    public void serdeTest() {
        final RecordPositionMapSerde serde = RecordPositionMapSerde.serde();
        final Serializer<Map<ExtendRecordPartition, RecordOffset>> serializer = serde.serializer();
        final Deserializer<Map<ExtendRecordPartition, RecordOffset>> deserializer = serde.deserializer();
        Assertions.assertThat(serializer instanceof RecordPositionMapSerializer).isTrue();
        Assertions.assertThat(deserializer instanceof RecordPositionMapDeserializer).isTrue();

    }

}
