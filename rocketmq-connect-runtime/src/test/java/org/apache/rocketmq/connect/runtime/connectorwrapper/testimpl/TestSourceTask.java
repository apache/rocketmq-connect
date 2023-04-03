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
 *
 */

package org.apache.rocketmq.connect.runtime.connectorwrapper.testimpl;

import io.openmessaging.KeyValue;
import io.openmessaging.connector.api.component.task.source.SourceTask;
import io.openmessaging.connector.api.data.ConnectRecord;
import io.openmessaging.connector.api.data.RecordOffset;
import io.openmessaging.connector.api.data.RecordPartition;
import io.openmessaging.connector.api.data.SchemaBuilder;
import org.assertj.core.util.Maps;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

public class TestSourceTask extends SourceTask {

    @Override
    public List<ConnectRecord> poll() {
        List<ConnectRecord> sourceTasks = new ArrayList<>(8);
        Map<String, String> partition = Maps.newHashMap("file", "fileName1");
        RecordPartition recordPartition = new RecordPartition(partition);
        Map<String, String> offset = Maps.newHashMap("offset", "2");
        RecordOffset recordOffset = new RecordOffset(offset);
        ConnectRecord record = new ConnectRecord(recordPartition, recordOffset, new Date().getTime(), SchemaBuilder.string().build(), "test");
        sourceTasks.add(record);
        return sourceTasks;
    }

    @Override
    public void validate(KeyValue config) {

    }

    @Override
    public void start(KeyValue config) {

    }

    @Override
    public void stop() {

    }

}
