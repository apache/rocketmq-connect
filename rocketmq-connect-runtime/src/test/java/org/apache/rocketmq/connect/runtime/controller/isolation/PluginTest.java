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

package org.apache.rocketmq.connect.runtime.controller.isolation;

import org.assertj.core.api.Assertions;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class PluginTest {

    private Plugin plugin;

    @Before
    public void before() {
        List<String> pluginPaths = new ArrayList<>();
        pluginPaths.add("src/test/java/org/apache/rocketmq/connect/runtime");
        plugin = new Plugin(pluginPaths);
    }

    @Test
    public void initLoadersTest() {
        Assertions.assertThatCode(() -> plugin.initLoaders()).doesNotThrowAnyException();
    }


    @Test
    public void shouldLoadInIsolationTest() {
        Assert.assertTrue(PluginUtils.shouldLoadInIsolation("org.apache.rocketmq.replicator.ReplicatorSourceTask"));
        Assert.assertTrue(PluginUtils.shouldLoadInIsolation("org.apache.rocketmq.connect.jdbc.mysql.source.MysqlJdbcSourceConnector"));

        Assert.assertFalse(PluginUtils.shouldLoadInIsolation("org.apache.rocketmq.client.consumer.DefaultMQPushConsumer"));
    }

}
