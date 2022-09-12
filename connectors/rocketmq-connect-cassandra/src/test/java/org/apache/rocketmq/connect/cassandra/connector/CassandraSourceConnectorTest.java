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
import java.util.List;
import org.apache.rocketmq.connect.cassandra.config.Config;
import org.assertj.core.api.Assertions;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class CassandraSourceConnectorTest {

    private CassandraSourceConnector cassandraSourceConnector;

    private KeyValue keyValue;

    @Before
    public void before() {
        keyValue = new DefaultKeyValue();
        keyValue.put(Config.CONN_DB_IP, "127.0.0.1");
        keyValue.put(Config.CONN_DB_PORT, "9042");
        keyValue.put(Config.CONN_DB_DATACENTER, "datacenter1");
        keyValue.put(Config.CONN_DB_MODE, "bulk");
        keyValue.put(Config.ROCKETMQ_TOPIC, "TEST_TOPIC");
        keyValue.put(Config.CONN_SOURCE_RMQ, "127.0.0.1:9876");
        keyValue.put(Config.CONN_WHITE_LIST, "{\n" +
            "        \"hello_ca\": {\n" +
            "            \"test_user\": {\n" +
            "                \"id\": \"1\"\n" +
            "            }\n" +
            "        }\n" +
            "    }");
        cassandraSourceConnector = new CassandraSourceConnector();
    }

    @Test
    public void validateTest() {
        Assertions.assertThatCode(() -> cassandraSourceConnector.validate(keyValue)).doesNotThrowAnyException();
    }

    @Test
    public void taskConfigsTest() {
        cassandraSourceConnector.validate(keyValue);
        final List<KeyValue> list = cassandraSourceConnector.taskConfigs(2);
        final KeyValue keyValue1 = list.get(0);
        Assert.assertEquals("127.0.0.1", keyValue1.getString(Config.CONN_DB_IP));
        Assert.assertEquals("9042", keyValue1.getString(Config.CONN_DB_PORT));
        Assert.assertEquals("bulk", keyValue1.getString(Config.CONN_DB_MODE));
        Assert.assertEquals("datacenter1", keyValue1.getString(Config.CONN_DB_DATACENTER));
    }
}
