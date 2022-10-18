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

package org.apache.rocketmq.connect.runtime.service.memory;

import io.openmessaging.connector.api.data.RecordConverter;
import java.util.Collection;
import java.util.Set;
import org.apache.rocketmq.connect.runtime.config.WorkerConfig;
import org.apache.rocketmq.connect.runtime.connectorwrapper.status.AbstractStatus;
import org.apache.rocketmq.connect.runtime.connectorwrapper.status.ConnectorStatus;
import org.apache.rocketmq.connect.runtime.connectorwrapper.status.TaskStatus;
import org.apache.rocketmq.connect.runtime.converter.record.json.JsonConverter;
import org.apache.rocketmq.connect.runtime.utils.ConnectorTaskId;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class MemoryStateManagementServiceImplTest {

    private MemoryStateManagementServiceImpl service;

    private WorkerConfig workerConfig;

    private RecordConverter recordConverter;

    @Before
    public void before() {
        service = new MemoryStateManagementServiceImpl();
        workerConfig = new WorkerConfig();
        recordConverter = new JsonConverter();
        service.initialize(workerConfig, recordConverter);
    }

    @Test
    public void TaskStatusTest() {
        ConnectorStatus connectorStatus = new ConnectorStatus("testConnector", AbstractStatus.State.RUNNING, "defaultWorkId", 1L);
        service.put(connectorStatus);

        ConnectorTaskId taskId = new ConnectorTaskId("testConnector", 2);
        TaskStatus taskStatus = new TaskStatus(taskId, AbstractStatus.State.RUNNING, "defaultWorkId", 1L);
        service.put(taskStatus);

        final TaskStatus resultTaskStatus = service.get(taskId);
        Assert.assertEquals(taskStatus.toString(), resultTaskStatus.toString());

        final ConnectorStatus resultConnectorStatus = service.get("testConnector");
        Assert.assertEquals(connectorStatus.toString(), resultConnectorStatus.toString());

        final Collection<TaskStatus> taskStatusList = service.getAll("testConnector");
        for (TaskStatus status : taskStatusList) {
            Assert.assertEquals(taskStatus.toString(), status.toString());
        }

        final Set<String> connectors = service.connectors();
        for (String connector : connectors) {
            Assert.assertEquals("testConnector", connector);
        }
    }
}
