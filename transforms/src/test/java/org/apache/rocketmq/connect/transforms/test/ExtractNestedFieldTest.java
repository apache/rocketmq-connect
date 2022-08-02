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
import io.openmessaging.connector.api.component.Transform;
import io.openmessaging.connector.api.data.ConnectRecord;
import io.openmessaging.connector.api.data.RecordOffset;
import io.openmessaging.connector.api.data.RecordPartition;
import io.openmessaging.connector.api.data.Schema;
import io.openmessaging.connector.api.data.SchemaBuilder;
import io.openmessaging.connector.api.data.Struct;
import io.openmessaging.internal.DefaultKeyValue;
import org.apache.rocketmq.connect.transforms.ExtractNestedField;
import org.apache.rocketmq.connect.transforms.ExtractNestedFieldConfig;
import org.apache.rocketmq.connect.transforms.test.common.AssertStruct;
import org.apache.rocketmq.connect.transforms.util.SchemaUtil;
import org.junit.jupiter.api.Test;

import java.util.HashMap;

import static org.apache.rocketmq.connect.transforms.test.common.AssertSchema.assertSchema;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public abstract class ExtractNestedFieldTest extends TransformationTest {
  protected ExtractNestedFieldTest(boolean isKey) {
    super(isKey);
  }

  @Test
  public void test() {
    KeyValue defaultKeyValue = new DefaultKeyValue();
    defaultKeyValue.put(ExtractNestedFieldConfig.INNER_FIELD_NAME_CONF, "state");
    defaultKeyValue.put(ExtractNestedFieldConfig.OUTER_FIELD_NAME_CONF, "address");
    defaultKeyValue.put(ExtractNestedFieldConfig.OUTPUT_FIELD_NAME_CONF, "state");

    this.transformation.start(defaultKeyValue);

    final Schema innerSchema = SchemaBuilder.struct()
        .name("Address")
        .field("city", SchemaUtil.STRING_SCHEMA)
        .field("state", SchemaUtil.STRING_SCHEMA)
        .build();
    final Schema inputSchema = SchemaBuilder.struct()
        .field("first_name", SchemaUtil.STRING_SCHEMA)
        .field("last_name", SchemaUtil.STRING_SCHEMA)
        .field("address", innerSchema)
        .build();
    final Schema expectedSchema = SchemaBuilder.struct()
        .field("first_name", SchemaUtil.STRING_SCHEMA)
        .field("last_name", SchemaUtil.STRING_SCHEMA)
        .field("address", innerSchema)
        .field("state", SchemaUtil.STRING_SCHEMA)
        .build();
    final Struct innerStruct = new Struct(innerSchema)
        .put("city", "Austin")
        .put("state", "tx");
    final Struct inputStruct = new Struct(inputSchema)
        .put("first_name", "test")
        .put("last_name", "developer")
        .put("address", innerStruct);
    final Struct expectedStruct = new Struct(expectedSchema)
        .put("first_name", "test")
        .put("last_name", "developer")
        .put("address", innerStruct)
        .put("state", "tx");

    final ConnectRecord inputRecord = new ConnectRecord(
            new RecordPartition(new HashMap<>()),
            new RecordOffset(new HashMap<>()),
            System.currentTimeMillis(),
            null,
            null,
            inputSchema,
            inputStruct
    );
    for (int i = 0; i < 50; i++) {
      final ConnectRecord transformedRecord = this.transformation.doTransform(inputRecord);
      assertNotNull(transformedRecord, "transformedRecord should not be null.");
      assertSchema(expectedSchema, transformedRecord.getSchema());
      AssertStruct.assertStruct(expectedStruct, (Struct) transformedRecord.getData());
    }
  }


  public static class ValueTest extends ExtractNestedFieldTest {
    protected ValueTest() {
      super(false);
    }

    @Override
    protected Transform<ConnectRecord> create() {
      return new ExtractNestedField.Value();
    }
  }

}
