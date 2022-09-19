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

package org.apache.rocketmq.connect.runtime.config;

import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class ConnectConfigTest {

    @Test
    public void testConnectConfigAttribute() {
        WorkerConfig connectConfig = new WorkerConfig();
        connectConfig.setHttpPort(8081);
        assertThat(connectConfig.getHttpPort()).isEqualTo(8081);

        connectConfig.setWorkerId("testWorker");
        assertThat("testWorker".equals(connectConfig.getWorkerId()));

        connectConfig.setNamesrvAddr("127.0.0.1:9876");
        assertThat("127.0.0.1:9876".equals(connectConfig.getNamesrvAddr()));

        connectConfig.setRmqProducerGroup("producer_group");
        assertThat("producer_group".equals(connectConfig.getRmqProducerGroup()));

        connectConfig.setMaxMessageSize(100);
        assertThat(100 == connectConfig.getMaxMessageSize());

        connectConfig.setOperationTimeout(100);
        assertThat(100 == connectConfig.getOperationTimeout());

        connectConfig.setRmqConsumerGroup("consumer_group");
        assertThat("consumer_group".equals(connectConfig.getRmqConsumerGroup()));

        connectConfig.setRmqMaxRedeliveryTimes(100);
        assertThat(100 == connectConfig.getRmqMaxRedeliveryTimes());

        connectConfig.setRmqMessageConsumeTimeout(100);
        assertThat(100 == connectConfig.getRmqMessageConsumeTimeout());

        connectConfig.setRmqMaxConsumeThreadNums(100);
        assertThat(100 == connectConfig.getRmqMaxConsumeThreadNums());

        connectConfig.setRmqMinConsumeThreadNums(10);
        assertThat(10 == connectConfig.getRmqMinConsumeThreadNums());

        connectConfig.setStorePathRootDir("/a/b");
        assertThat("/a/b".equals(connectConfig.getStorePathRootDir()));

        connectConfig.setPositionPersistInterval(100);
        assertThat(100 == connectConfig.getPositionPersistInterval());

        connectConfig.setOffsetPersistInterval(100);
        assertThat(100 == connectConfig.getOffsetPersistInterval());

        connectConfig.setConfigPersistInterval(100);
        assertThat(100 == connectConfig.getConfigPersistInterval());

        connectConfig.setPluginPaths("/a/b");
        assertThat("/a/b".equals(connectConfig.getPluginPaths()));

        connectConfig.setClusterStoreTopic("cluster_topic");
        assertThat("cluster_topic".equals(connectConfig.getClusterStoreTopic()));

        connectConfig.setConfigStoreTopic("config_topic");
        assertThat("config_topic".equals(connectConfig.getConfigStoreTopic()));

        connectConfig.setPositionStoreTopic("position_topic");
        assertThat("position_topic".equals(connectConfig.getPositionStoreTopic()));

        connectConfig.setOffsetStoreTopic("offset_topic");
        assertThat("offset_topic".equals(connectConfig.getOffsetStoreTopic()));

        connectConfig.setConnectClusterId("cluster_1");
        assertThat("cluster_1".equals(connectConfig.getConnectClusterId()));

        connectConfig.setAllocTaskStrategy("strategy_1");
        assertThat("strategy_1".equals(connectConfig.getAllocTaskStrategy()));

        connectConfig.setAclEnable(true);
        assertThat(true == connectConfig.getAclEnable());

        connectConfig.setAccessKey("admin");
        assertThat("admin".equals(connectConfig.getAccessKey()));

        connectConfig.setSecretKey("12345678");
        assertThat("12345678".equals(connectConfig.getSecretKey()));

        connectConfig.setAutoCreateGroupEnable(true);
        assertThat(true == connectConfig.isAutoCreateGroupEnable());

        connectConfig.setAdminExtGroup("admin_group");
        assertThat("admin_group".equals(connectConfig.getAdminExtGroup()));

        connectConfig.setConnectHome("/usr/connect");
        assertThat("/usr/connect".equals(connectConfig.getConnectHome()));

        connectConfig.setOffsetCommitIntervalMsConfig(100);
        assertThat(100 == connectConfig.getOffsetCommitIntervalMsConfig());

        connectConfig.setMaxStartTimeoutMills(100);
        assertThat(100 == connectConfig.getMaxStartTimeoutMills());

        connectConfig.setMaxStopTimeoutMills(100);
        assertThat(100 == connectConfig.getMaxStopTimeoutMills());

        connectConfig.setOffsetCommitTimeoutMsConfig(100);
        assertThat(100 == connectConfig.getOffsetCommitTimeoutMsConfig());

    }
}