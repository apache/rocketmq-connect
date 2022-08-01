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
import io.openmessaging.connector.api.data.ConnectRecord;
import io.openmessaging.connector.api.data.RecordOffset;
import io.openmessaging.connector.api.data.RecordPartition;
import io.openmessaging.connector.api.data.Schema;
import io.openmessaging.connector.api.data.SchemaBuilder;
import io.openmessaging.connector.api.data.Struct;
import io.openmessaging.internal.DefaultKeyValue;
import org.apache.rocketmq.connect.transforms.PatternFilter;
import org.apache.rocketmq.connect.transforms.PatternFilterConfig;
import org.apache.rocketmq.connect.transforms.util.SchemaUtil;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

public class PatternFilterTest {
  public PatternFilter.Value transform;

  @BeforeEach
  public void before() {
    KeyValue defaultKeyValue = new DefaultKeyValue();
    defaultKeyValue.put(PatternFilterConfig.FIELD_CONFIG, "input");
    defaultKeyValue.put(PatternFilterConfig.PATTERN_CONFIG, "^filter$");

    this.transform = new PatternFilter.Value();
    this.transform.start(defaultKeyValue);
  }

  ConnectRecord map(String value) {
    return new ConnectRecord(
            new RecordPartition(new HashMap<>()),
            new RecordOffset(new HashMap<>()),
            System.currentTimeMillis(),
            null,
            null,
            null,
            ImmutableMap.of("input", value)
    );
  }

  ConnectRecord struct(String value) {
    Schema schema = SchemaBuilder.struct()
        .field("input", SchemaUtil.STRING_SCHEMA)
        .build();
    Struct struct = new Struct(schema)
        .put("input", value);
    return new ConnectRecord(
            new RecordPartition(new HashMap<>()),
            new RecordOffset(new HashMap<>()),
            System.currentTimeMillis(),
            null,
            null,
            schema,
            struct
    );
  }

  @Test
  public void filtered() {
    assertNull(this.transform.doTransform(struct("filter")));
    assertNull(this.transform.doTransform(map("filter")));
  }

  @Test
  public void notFiltered() {
    assertNotNull(this.transform.doTransform(struct("ok")));
    assertNotNull(this.transform.doTransform(map("ok")));
  }

}
