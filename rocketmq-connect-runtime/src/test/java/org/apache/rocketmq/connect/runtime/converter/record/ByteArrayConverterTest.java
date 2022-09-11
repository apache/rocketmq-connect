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

package org.apache.rocketmq.connect.runtime.converter.record;

import io.openmessaging.connector.api.data.SchemaAndValue;
import io.openmessaging.connector.api.data.SchemaBuilder;
import org.assertj.core.api.Assertions;
import org.junit.Test;

import java.nio.charset.StandardCharsets;

public class ByteArrayConverterTest {

    private static final String TOPIC = "topic";
    private static final String TEST_OBJECT = "Hello World";
    private ByteArrayConverter byteArrayConverter = new ByteArrayConverter();

    @Test
    public void fromConnectDataTest() {
        final byte[] bytes = byteArrayConverter.fromConnectData(TOPIC, SchemaBuilder.bytes().build(), TEST_OBJECT.getBytes(StandardCharsets.UTF_8));
        assert TEST_OBJECT.equals(new String(bytes));

        Assertions.assertThatThrownBy(() -> byteArrayConverter.fromConnectData(TOPIC, SchemaBuilder.struct().build(), TEST_OBJECT.getBytes(StandardCharsets.UTF_8)));
    }

    @Test
    public void toConnectDataTest() {
        final SchemaAndValue schemaAndValue = byteArrayConverter.toConnectData(TOPIC, TEST_OBJECT.getBytes(StandardCharsets.UTF_8));
        assert TEST_OBJECT.equals(new String((byte[]) schemaAndValue.value()));
    }
}
