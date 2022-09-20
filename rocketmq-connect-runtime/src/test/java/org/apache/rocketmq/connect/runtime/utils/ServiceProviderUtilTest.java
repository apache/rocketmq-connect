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

package org.apache.rocketmq.connect.runtime.utils;

import org.apache.rocketmq.connect.runtime.service.ClusterManagementService;
import org.apache.rocketmq.connect.runtime.service.ClusterManagementServiceImpl;
import org.apache.rocketmq.connect.runtime.service.ConfigManagementService;
import org.apache.rocketmq.connect.runtime.service.ConfigManagementServiceImpl;
import org.apache.rocketmq.connect.runtime.service.PositionManagementService;
import org.apache.rocketmq.connect.runtime.service.PositionManagementServiceImpl;
import org.apache.rocketmq.connect.runtime.service.StagingMode;
import org.apache.rocketmq.connect.runtime.service.memory.FilePositionManagementServiceImpl;
import org.apache.rocketmq.connect.runtime.service.memory.MemoryClusterManagementServiceImpl;
import org.apache.rocketmq.connect.runtime.service.memory.MemoryConfigManagementServiceImpl;
import org.junit.Assert;
import org.junit.Test;

public class ServiceProviderUtilTest {

    @Test
    public void getClusterManagementServicesTest() {
        final ClusterManagementService distributed = ServiceProviderUtil.getClusterManagementServices(StagingMode.DISTRIBUTED);
        Assert.assertTrue(distributed instanceof ClusterManagementServiceImpl);

        final ClusterManagementService standAlone = ServiceProviderUtil.getClusterManagementServices(StagingMode.STANDALONE);
        Assert.assertTrue(standAlone instanceof MemoryClusterManagementServiceImpl);
    }

    @Test
    public void getConfigManagementServicesTest(){
        final ConfigManagementService distributedService = ServiceProviderUtil.getConfigManagementServices(StagingMode.DISTRIBUTED);
        Assert.assertTrue(distributedService instanceof ConfigManagementServiceImpl);

        final ConfigManagementService standaloneService = ServiceProviderUtil.getConfigManagementServices(StagingMode.STANDALONE);
        Assert.assertTrue(standaloneService instanceof MemoryConfigManagementServiceImpl);
    }

    @Test
    public void getPositionManagementServicesTest() {
        final PositionManagementService distributedService = ServiceProviderUtil.getPositionManagementServices(StagingMode.DISTRIBUTED);
        Assert.assertTrue(distributedService instanceof PositionManagementServiceImpl);
        final PositionManagementService standaloneService = ServiceProviderUtil.getPositionManagementServices(StagingMode.STANDALONE);
        Assert.assertTrue(standaloneService instanceof FilePositionManagementServiceImpl);
    }
}
