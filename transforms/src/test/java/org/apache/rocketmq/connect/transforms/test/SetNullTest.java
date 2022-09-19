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

import io.openmessaging.connector.api.data.ConnectRecord;
import io.openmessaging.connector.api.data.RecordOffset;
import io.openmessaging.connector.api.data.RecordPartition;
import org.apache.rocketmq.connect.transforms.SetNull;
import org.apache.rocketmq.connect.transforms.util.SchemaUtil;
import org.junit.jupiter.api.Test;

import java.util.HashMap;

import static org.junit.jupiter.api.Assertions.assertNull;

public class SetNullTest {

  @Test
  public void test() {
    final ConnectRecord input = new ConnectRecord(
            new RecordPartition(new HashMap<>()),
            new RecordOffset(new HashMap<>()),
            System.currentTimeMillis(),
            SchemaUtil.STRING_SCHEMA,
            "key",
            null,
            ""
    );
    SetNull transform = new SetNull.Key();
    final ConnectRecord actual = transform.doTransform(input);
    assertNull(actual.getKey(), "key should be null.");
    assertNull(actual.getKeySchema(), "keySchema should be null.");
  }

}
