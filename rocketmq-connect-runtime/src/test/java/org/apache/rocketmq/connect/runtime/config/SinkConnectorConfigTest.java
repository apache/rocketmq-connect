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

package org.apache.rocketmq.connect.runtime.config;

import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.connect.runtime.common.ConnectKeyValue;
import org.junit.Assert;
import org.junit.Test;

import java.util.Set;

public class SinkConnectorConfigTest {

    private ConnectKeyValue connectKeyValue = new ConnectKeyValue();

    @Test
    public void parseTopicListTest() {
        connectKeyValue.put(SinkConnectorConfig.CONNECT_TOPICNAMES, "testTopic");
        connectKeyValue.put(SinkConnectorConfig.CONNECTOR_CLASS, "org.apache.rocketmq.connect.runtime.connectorwrapper.testimpl.TestConnector");
        final Set<String> result = new SinkConnectorConfig(connectKeyValue).parseTopicList();
        System.out.println(result.toString());
        Assert.assertTrue(!result.isEmpty());
        Assert.assertEquals("[testTopic]", result.toString());
    }

    @Test
    public void parseMessageQueueListTest() {
        final MessageQueue queue1 = SinkConnectorConfig.parseMessageQueueList("topic");
        assert queue1 == null;
        final MessageQueue queue2 = SinkConnectorConfig.parseMessageQueueList("topic;brokerName;1");
        assert queue2 != null;
    }
}
