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

import com.google.common.base.CaseFormat;
import io.openmessaging.KeyValue;
import io.openmessaging.connector.api.component.Transform;
import io.openmessaging.connector.api.data.ConnectRecord;
import io.openmessaging.connector.api.data.RecordOffset;
import io.openmessaging.connector.api.data.RecordPartition;
import io.openmessaging.connector.api.data.Schema;
import io.openmessaging.connector.api.data.SchemaBuilder;
import io.openmessaging.connector.api.data.Struct;
import io.openmessaging.internal.DefaultKeyValue;
import org.apache.rocketmq.connect.transforms.ChangeCase;
import org.apache.rocketmq.connect.transforms.ChangeCaseConfig;
import org.apache.rocketmq.connect.transforms.util.SchemaUtil;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;

import java.util.HashMap;

public class ChangeCaseTest extends TransformationTest {
  public ChangeCaseTest() {
    super(false);
  }


  @Override
  protected Transform<ConnectRecord> create() {
    ChangeCase.Value value = new ChangeCase.Value();
    return value;
  }

  @Test
  public void test() {

    KeyValue defaultKeyValue = new DefaultKeyValue();
    defaultKeyValue.put(ChangeCaseConfig.FROM_CONFIG, CaseFormat.UPPER_UNDERSCORE.toString());
    defaultKeyValue.put(ChangeCaseConfig.TO_CONFIG, CaseFormat.LOWER_UNDERSCORE.toString());
    this.transformation.start(defaultKeyValue);
    final Schema inputSchema = SchemaBuilder.struct()
        .field("FIRST_NAME", SchemaUtil.STRING_SCHEMA)
        .field("LAST_NAME", SchemaUtil.STRING_SCHEMA)
        .build();
    final Schema expectedSchema = SchemaBuilder.struct()
        .field("first_name", SchemaUtil.STRING_SCHEMA)
        .field("last_name", SchemaUtil.STRING_SCHEMA)
        .build();
    final Struct inputStruct = new Struct(inputSchema)
        .put("FIRST_NAME", "test")
        .put("LAST_NAME", "user");
    final Struct expectedStruct = new Struct(expectedSchema)
        .put("first_name", "test")
        .put("last_name", "user");

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
      Assertions.assertNotNull(transformedRecord, "transformedRecord should not be null.");
    }
  }
}
