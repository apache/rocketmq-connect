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
package org.apache.rocketmq.connect.transforms.test;

import io.openmessaging.KeyValue;
import io.openmessaging.connector.api.data.ConnectRecord;
import io.openmessaging.connector.api.data.RecordOffset;
import io.openmessaging.connector.api.data.RecordPartition;
import io.openmessaging.connector.api.data.Schema;
import io.openmessaging.connector.api.data.SchemaBuilder;
import io.openmessaging.connector.api.data.Struct;
import io.openmessaging.connector.api.data.logical.Decimal;
import io.openmessaging.internal.DefaultKeyValue;
import org.apache.rocketmq.connect.transforms.SetMaximumPrecision;
import org.apache.rocketmq.connect.transforms.SetMaximumPrecisionConfig;
import org.apache.rocketmq.connect.transforms.util.SchemaUtil;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.HashMap;

import static org.apache.rocketmq.connect.transforms.test.common.AssertStruct.assertStruct;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class SetMaximumPrecisionTest {
  ConnectRecord record(Struct struct) {
    return new ConnectRecord(
            new RecordPartition(new HashMap<>()),
            new RecordOffset(new HashMap<>()),
            System.currentTimeMillis(),
            null,
            null,
            struct.schema(),
            struct
    );
  }

  @Test
  public void noop() {
    Schema schema = SchemaBuilder.struct()
        .field("first", SchemaUtil.STRING_SCHEMA)
        .field("last", SchemaUtil.STRING_SCHEMA)
        .field("email", SchemaUtil.STRING_SCHEMA)
        .build();
    Struct struct = new Struct(schema)
        .put("first", "test")
        .put("last", "user")
        .put("first", "none@none.com");
    ConnectRecord record = record(struct);
    SetMaximumPrecision.Value transform = new SetMaximumPrecision.Value();

    KeyValue defaultKeyValue = new DefaultKeyValue();
    defaultKeyValue.put(SetMaximumPrecisionConfig.MAX_PRECISION_CONFIG, 32);
    transform.start(defaultKeyValue);
    ConnectRecord actual = transform.doTransform(record);
    assertNotNull(actual);
    assertStruct((Struct) record.getData(), (Struct) actual.getData());
  }

  @Test
  public void convert() {
    final Schema inputSchema = SchemaBuilder.struct()
        .field("first", Decimal.schema(5))
        .field(
            "second",
            Decimal.builder(5)
                .parameter(SetMaximumPrecision.CONNECT_AVRO_DECIMAL_PRECISION_PROP, "16")
                .optional()
                .build()
        )
        .field(
            "third",
            Decimal.builder(5)
                .parameter(SetMaximumPrecision.CONNECT_AVRO_DECIMAL_PRECISION_PROP, "48")
                .optional()
                .build()
        )
        .build();
    final Struct inputStruct = new Struct(inputSchema)
        .put("first", BigDecimal.ONE)
        .put("second", null)
        .put("third", BigDecimal.ONE);
    final Schema expectedSchema = SchemaBuilder.struct()
        .field(
            "first",
            Decimal.builder(5)
                .parameter(SetMaximumPrecision.CONNECT_AVRO_DECIMAL_PRECISION_PROP, "32")
                .build()
        )
        .field(
            "second",
            Decimal.builder(5)
                .parameter(SetMaximumPrecision.CONNECT_AVRO_DECIMAL_PRECISION_PROP, "16")
                .optional()
                .build()
        )
        .field(
            "third",
            Decimal.builder(5)
                .parameter(SetMaximumPrecision.CONNECT_AVRO_DECIMAL_PRECISION_PROP, "32")
                .optional()
                .build()
        )
        .build();
    final Struct expectedStruct = new Struct(expectedSchema)
        .put("first", BigDecimal.ONE)
        .put("second", null)
        .put("third", BigDecimal.ONE);


    ConnectRecord record = record(inputStruct);
    SetMaximumPrecision.Value transform = new SetMaximumPrecision.Value();
    KeyValue defaultKeyValue = new DefaultKeyValue();
    defaultKeyValue.put(SetMaximumPrecisionConfig.MAX_PRECISION_CONFIG, 32);

    transform.start(defaultKeyValue);
    ConnectRecord actual = transform.doTransform(record);
    assertNotNull(actual);
    assertStruct(expectedStruct, (Struct) actual.getData());
  }

}
