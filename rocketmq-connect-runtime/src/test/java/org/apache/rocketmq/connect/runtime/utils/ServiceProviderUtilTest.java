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
import org.apache.rocketmq.connect.runtime.service.local.LocalConfigManagementServiceImpl;
import org.apache.rocketmq.connect.runtime.service.PositionManagementService;
import org.apache.rocketmq.connect.runtime.service.local.LocalPositionManagementServiceImpl;
import org.apache.rocketmq.connect.runtime.service.memory.FilePositionManagementServiceImpl;
import org.apache.rocketmq.connect.runtime.service.memory.MemoryClusterManagementServiceImpl;
import org.apache.rocketmq.connect.runtime.service.memory.MemoryConfigManagementServiceImpl;
import org.junit.Assert;
import org.junit.Test;

public class ServiceProviderUtilTest {

    @Test
    public void getClusterManagementServicesTest() {
        final ClusterManagementService distributed =
                ServiceProviderUtil.getClusterManagementService(ClusterManagementServiceImpl.class.getName());
        Assert.assertTrue(distributed instanceof ClusterManagementServiceImpl);

        final ClusterManagementService standAlone =
                ServiceProviderUtil.getClusterManagementService(MemoryClusterManagementServiceImpl.class.getName());
        Assert.assertTrue(standAlone instanceof MemoryClusterManagementServiceImpl);
    }

    @Test
    public void getConfigManagementServicesTest(){
        final ConfigManagementService distributedService =
                ServiceProviderUtil.getConfigManagementService(LocalConfigManagementServiceImpl.class.getName());
        Assert.assertTrue(distributedService instanceof LocalConfigManagementServiceImpl);

        final ConfigManagementService standaloneService =
                ServiceProviderUtil.getConfigManagementService(MemoryConfigManagementServiceImpl.class.getName());
        Assert.assertTrue(standaloneService instanceof MemoryConfigManagementServiceImpl);
    }

    @Test
    public void getPositionManagementServicesTest() {
        final PositionManagementService distributedService =
                ServiceProviderUtil.getPositionManagementService(LocalPositionManagementServiceImpl.class.getName());
        Assert.assertTrue(distributedService instanceof LocalPositionManagementServiceImpl);
        final PositionManagementService standaloneService =
                ServiceProviderUtil.getPositionManagementService(FilePositionManagementServiceImpl.class.getName());
        Assert.assertTrue(standaloneService instanceof FilePositionManagementServiceImpl);
    }
}
