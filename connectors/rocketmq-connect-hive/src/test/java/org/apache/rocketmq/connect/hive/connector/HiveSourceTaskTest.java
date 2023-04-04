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

package org.apache.rocketmq.connect.hive.connector;

import io.openmessaging.KeyValue;
import io.openmessaging.connector.api.component.task.source.SourceTaskContext;
import io.openmessaging.connector.api.storage.OffsetStorageReader;
import io.openmessaging.internal.DefaultKeyValue;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import org.apache.rocketmq.connect.hive.config.HiveColumn;
import org.apache.rocketmq.connect.hive.config.HiveConfig;
import org.apache.rocketmq.connect.hive.config.HiveConstant;
import org.apache.rocketmq.connect.hive.config.HiveJdbcDriverManager;
import org.apache.rocketmq.connect.hive.config.HiveRecord;
import org.assertj.core.api.Assertions;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class HiveSourceTaskTest {

    private HiveSourceTask hiveSourceTask;

    private KeyValue keyValue;

    private HiveConfig hiveConfig;

    @Mock
    private SourceTaskContext sourceTaskContext;

    @Mock
    private OffsetStorageReader offsetStorageReader;

    @Before
    public void before() throws NoSuchFieldException, IllegalAccessException {
        hiveSourceTask = new HiveSourceTask();
        keyValue = new DefaultKeyValue();
        keyValue.put(HiveConstant.HOST, "127.0.0.1");
        keyValue.put(HiveConstant.PORT, 10000);
        keyValue.put(HiveConstant.TABLES, "{\n" +
            "\"invites\": {\n" +
            "\"bar\":1\n" +
            "}\n" +
            "}");
        keyValue.put(HiveConstant.INCREMENT_FIELD, "foo");
        keyValue.put(HiveConstant.INCREMENT_VALUE, 1);
        keyValue.put(HiveConstant.TABLE_NAME, "invites");
        keyValue.put(HiveConstant.DATABASE, "default");
        keyValue.put("invites", "foo");
        hiveConfig = new HiveConfig();
        hiveConfig.load(keyValue);
        final Field context = hiveSourceTask.getClass().getSuperclass().getDeclaredField("sourceTaskContext");
        context.setAccessible(true);
        context.set(hiveSourceTask, sourceTaskContext);
        when(sourceTaskContext.offsetStorageReader()).thenReturn(offsetStorageReader);
        HiveJdbcDriverManager.init(hiveConfig);
        hiveSourceTask.start(keyValue);
    }

    @After
    public void after() {
        hiveSourceTask.stop();
    }


    @Test
    public void hiveRecord2ConnectRecordTest() throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        HiveRecord record = new HiveRecord();
        record.setTableName("invites");
        List<HiveColumn> coumnList = new ArrayList<>();
        HiveColumn column = new HiveColumn("name", "oliver");
        coumnList.add(column);
        record.setRecord(coumnList);
        final Method hiveRecord2ConnectRecordMethod = hiveSourceTask.getClass().getDeclaredMethod("hiveRecord2ConnectRecord", HiveRecord.class);
        hiveRecord2ConnectRecordMethod.setAccessible(true);
        Assertions.assertThatCode(() ->hiveRecord2ConnectRecordMethod.invoke(hiveSourceTask, record)).doesNotThrowAnyException();
    }

}
