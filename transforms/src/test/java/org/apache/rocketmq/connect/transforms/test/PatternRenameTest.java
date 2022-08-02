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

import com.google.common.collect.ImmutableMap;
import io.openmessaging.KeyValue;
import io.openmessaging.connector.api.component.Transform;
import io.openmessaging.connector.api.data.ConnectRecord;
import io.openmessaging.connector.api.data.RecordOffset;
import io.openmessaging.connector.api.data.RecordPartition;
import io.openmessaging.connector.api.data.Schema;
import io.openmessaging.connector.api.data.SchemaBuilder;
import io.openmessaging.connector.api.data.Struct;
import io.openmessaging.internal.DefaultKeyValue;
import org.apache.rocketmq.connect.transforms.PatternRename;
import org.apache.rocketmq.connect.transforms.PatternRenameConfig;
import org.apache.rocketmq.connect.transforms.util.SchemaUtil;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.apache.rocketmq.connect.transforms.test.common.AssertSchema.assertSchema;
import static org.apache.rocketmq.connect.transforms.test.common.AssertStruct.assertStruct;
import static org.junit.Assert.assertNotNull;

public abstract class PatternRenameTest extends TransformationTest {
  final static String TOPIC = "test";
  protected PatternRenameTest(boolean isKey) {
    super(isKey);
  }

  @Test
  public void schemaLess() {
    KeyValue defaultKeyValue = new DefaultKeyValue();
    defaultKeyValue.put(PatternRenameConfig.FIELD_PATTERN_CONF, "\\.");
    defaultKeyValue.put(PatternRenameConfig.FIELD_REPLACEMENT_CONF, "_");
    this.transformation.start(defaultKeyValue);
    final Map<String, Object> input = ImmutableMap.of(
        "first.name", "example",
        "last.name", "user"
    );
    final Map<String, Object> expected = ImmutableMap.of(
        "first_name", "example",
        "last_name", "user"
    );

    final Object key = isKey ? input : null;
    final Object value = isKey ? null : input;
    final Schema keySchema = null;
    final Schema valueSchema = null;

    final ConnectRecord inputRecord = new ConnectRecord(
        new RecordPartition(new HashMap<>()),
        new RecordOffset(new HashMap<>()),
        System.currentTimeMillis(),
        keySchema,
        key,
        valueSchema,
        value
    );
    final ConnectRecord outputRecord = this.transformation.doTransform(inputRecord);
    assertNotNull(outputRecord);
  }

  @Test
  public void prefixed() {

    KeyValue defaultKeyValue = new DefaultKeyValue();
    defaultKeyValue.put(PatternRenameConfig.FIELD_PATTERN_CONF, "^prefixed");
    defaultKeyValue.put(PatternRenameConfig.FIELD_REPLACEMENT_CONF, "");
    this.transformation.start(defaultKeyValue);

    Schema inputSchema = SchemaBuilder.struct()
            .name("testing")
            .field("prefixedfirstname", SchemaUtil.STRING_SCHEMA)
            .field("prefixedlastname", SchemaUtil.STRING_SCHEMA)
            .build();
    Struct inputStruct = new Struct(inputSchema)
        .put("prefixedfirstname", "example")
        .put("prefixedlastname", "user");

    final Object key = isKey ? inputStruct : null;
    final Object value = isKey ? null : inputStruct;
    final Schema keySchema = isKey ? inputSchema : null;
    final Schema valueSchema = isKey ? null : inputSchema;

    final ConnectRecord inputRecord = new ConnectRecord(
            new RecordPartition(new HashMap<>()),
            new RecordOffset(new HashMap<>()),
            System.currentTimeMillis(),
        keySchema,
        key,
        valueSchema,
        value
    );
    final ConnectRecord outputRecord = this.transformation.doTransform(inputRecord);
    assertNotNull(outputRecord);

    final Schema actualSchema = isKey ? outputRecord.getKeySchema() : outputRecord.getSchema();
    final Struct actualStruct = (Struct) (isKey ? outputRecord.getKey() : outputRecord.getData());

    final Schema expectedSchema = SchemaBuilder.struct()
        .name("testing")
        .field("firstname", SchemaUtil.STRING_SCHEMA)
        .field("lastname", SchemaUtil.STRING_SCHEMA).build();
    Struct expectedStruct = new Struct(expectedSchema)
        .put("firstname", "example")
        .put("lastname", "user");

    assertSchema(expectedSchema, actualSchema);
    assertStruct(expectedStruct, actualStruct);
  }

  public static class KeyTest extends PatternRenameTest {
    public KeyTest() {
      super(true);
    }

    @Override
    protected Transform<ConnectRecord> create() {
      return new PatternRename.Key();
    }
  }

  public static class ValueTest extends PatternRenameTest {
    public ValueTest() {
      super(false);
    }

    @Override
    protected Transform<ConnectRecord> create() {
      return new PatternRename.Value();
    }
  }
}
