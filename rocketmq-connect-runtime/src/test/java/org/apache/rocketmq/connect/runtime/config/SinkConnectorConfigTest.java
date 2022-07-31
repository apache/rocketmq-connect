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

import java.util.Set;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.connect.runtime.common.ConnectKeyValue;
import org.junit.Test;

public class SinkConnectorConfigTest {

    private ConnectKeyValue connectKeyValue = new ConnectKeyValue();

    @Test
    public void parseTopicListTest() {
        // connect-topicname is null
        final Set<String> result1 = SinkConnectorConfig.parseTopicList(connectKeyValue);
        assert result1 == null;

        // connect-topicname is not null
        connectKeyValue.put(RuntimeConfigDefine.CONNECT_TOPICNAME, "test");
        final Set<String> result2 = SinkConnectorConfig.parseTopicList(connectKeyValue);
        assert !result2.isEmpty();
    }

    @Test
    public void parseMessageQueueListTest() {
        final MessageQueue queue1 = SinkConnectorConfig.parseMessageQueueList("topic");
        assert queue1 == null;
        final MessageQueue queue2 = SinkConnectorConfig.parseMessageQueueList("topic;brokerName;1");
        assert queue2  != null;
    }
}
