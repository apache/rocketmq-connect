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

package org.apache.rocketmq.connect.cassandra.connector;

import io.openmessaging.KeyValue;
import io.openmessaging.internal.DefaultKeyValue;
import org.apache.rocketmq.connect.cassandra.config.Config;
import org.assertj.core.api.Assertions;
import org.junit.Before;
import org.junit.Test;

public class CassandraSinkTaskTest {

    private KeyValue keyValue;

    private CassandraSinkTask cassandraSinkTask;

    @Before
    public void before() {
        keyValue = new DefaultKeyValue();
        keyValue.put(Config.CONN_DB_IP, "127.0.0.1");
        keyValue.put(Config.CONN_DB_PORT, "9042");
        keyValue.put(Config.CONN_DB_DATACENTER, "datacenter1");
        keyValue.put(Config.CONN_DB_MODE, "bulk");
        keyValue.put(Config.ROCKETMQ_TOPIC, "TEST_TOPIC");
        keyValue.put(Config.CONN_SOURCE_RMQ, "127.0.0.1:9876");

        cassandraSinkTask = new CassandraSinkTask();
    }

    @Test
    public void startTest() {
        Assertions.assertThatCode(() -> cassandraSinkTask.start(keyValue)).doesNotThrowAnyException();
    }
}
