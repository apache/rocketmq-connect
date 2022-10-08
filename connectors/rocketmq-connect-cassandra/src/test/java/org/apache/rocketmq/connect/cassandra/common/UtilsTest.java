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

package org.apache.rocketmq.connect.cassandra.common;

import java.util.ArrayList;
import java.util.List;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.protocol.route.BrokerData;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;
import org.junit.Assert;
import org.junit.Test;

public class UtilsTest {

    private static final String GROUP_PREFIX = "defaultGroup";

    private static final String GROUP_POSTFIX = "connect";

    private static final String TASK_PREFIX = "defaultTask";

    private static final String NAMESERVER_ADDRESS = "127.0.0.1:9876";

    private static final String TOPIC = "TEST_TOPIC";

    private static final String CLUSTER = "DefaultCluster";

    @Test
    public void createGroupNameTest() {
        final String groupName = Utils.createGroupName(GROUP_PREFIX);
        Assert.assertTrue(groupName.startsWith(GROUP_PREFIX));

        final String groupName1 = Utils.createGroupName(GROUP_PREFIX, GROUP_POSTFIX);
        Assert.assertTrue(groupName1.startsWith(GROUP_PREFIX) && groupName1.endsWith(GROUP_POSTFIX));
    }

    @Test
    public void createTaskIdTest() {
        final String id = Utils.createTaskId(TASK_PREFIX);
        Assert.assertTrue(id.startsWith(TASK_PREFIX));
    }

    @Test
    public void createInstanceNameTest() {
        final String hashCode = Utils.createInstanceName(NAMESERVER_ADDRESS);
        List<String> namesrvList = new ArrayList<>();
        namesrvList.add(NAMESERVER_ADDRESS);
        Assert.assertEquals(String.valueOf(namesrvList.toString().hashCode()), hashCode);
    }

    @Test
    public void examineBrokerDataTest() throws RemotingException, InterruptedException, MQClientException {
        // it needs to start nameServer and broker first
        DefaultMQAdminExt defaultMQAdminExt = new DefaultMQAdminExt();
        defaultMQAdminExt.setNamesrvAddr(NAMESERVER_ADDRESS);
        defaultMQAdminExt.start();
        final List<BrokerData> data = Utils.examineBrokerData(defaultMQAdminExt, TOPIC, CLUSTER);
        final BrokerData brokerData = data.get(0);
        Assert.assertEquals("broker-a", brokerData.getBrokerName());
        Assert.assertEquals("127.0.0.1:10911", brokerData.getBrokerAddrs().get(0L));
    }
}
