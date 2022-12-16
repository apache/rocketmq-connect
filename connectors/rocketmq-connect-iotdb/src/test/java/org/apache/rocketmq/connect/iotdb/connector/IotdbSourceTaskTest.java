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

package org.apache.rocketmq.connect.iotdb.connector;

import io.openmessaging.KeyValue;
import io.openmessaging.connector.api.component.task.source.SourceTaskContext;
import io.openmessaging.connector.api.data.RecordOffset;
import io.openmessaging.connector.api.storage.OffsetStorageReader;
import io.openmessaging.internal.DefaultKeyValue;
import java.lang.reflect.Field;
import org.apache.rocketmq.connect.iotdb.config.IotdbConstant;
import org.assertj.core.api.Assertions;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class IotdbSourceTaskTest {

    @Mock
    SourceTaskContext sourceTaskContext;

    @Mock
    private RecordOffset recordOffset;

    @Mock
    private OffsetStorageReader offsetStorageReader;

    @Test
    public void startTest() throws IllegalAccessException, NoSuchFieldException {
        IotdbSourceTask task = new IotdbSourceTask();
        final Field field = task.getClass().getSuperclass().getDeclaredField("sourceTaskContext");
        field.setAccessible(true);
        field.set(task, sourceTaskContext);
        when(sourceTaskContext.offsetStorageReader()).thenReturn(offsetStorageReader);
        when(offsetStorageReader.readOffset(any())).thenReturn(recordOffset);
        KeyValue keyValue = new DefaultKeyValue();
        keyValue.put(IotdbConstant.IOTDB_HOST, "localhost");
        keyValue.put(IotdbConstant.IOTDB_PORT, 6667);
        keyValue.put(IotdbConstant.IOTDB_PATH, "root.ln.wf01.wt01");
        Assertions.assertThatCode(() -> task.start(keyValue)).doesNotThrowAnyException();
    }
}
