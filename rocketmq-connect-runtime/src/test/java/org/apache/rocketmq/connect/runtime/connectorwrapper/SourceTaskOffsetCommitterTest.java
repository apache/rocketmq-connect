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

package org.apache.rocketmq.connect.runtime.connectorwrapper;

import org.apache.rocketmq.connect.runtime.config.WorkerConfig;
import org.apache.rocketmq.connect.runtime.utils.ConnectorTaskId;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

@RunWith(MockitoJUnitRunner.class)
public class SourceTaskOffsetCommitterTest {

    private SourceTaskOffsetCommitter sourceTaskOffsetCommitter;

    private WorkerConfig connectConfig = new WorkerConfig();

    private ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool(5);

    private ConcurrentMap<ConnectorTaskId, ScheduledFuture<?>> committers = new ConcurrentHashMap<>();

    @Mock
    private ConnectorTaskId connectorTaskId;

    @Mock
    private WorkerSourceTask workerSourceTask;

    @Before
    public void before() {
        connectConfig.setOffsetCommitIntervalMsConfig(100);
        sourceTaskOffsetCommitter = new SourceTaskOffsetCommitter(connectConfig);
        sourceTaskOffsetCommitter = new SourceTaskOffsetCommitter(connectConfig, scheduledExecutorService, committers);
        ScheduledFuture<?> commitFuture = scheduledExecutorService.scheduleWithFixedDelay(() -> {
            if (workerSourceTask.commitOffsets()) {
                return;
            }
        }, 100, 1000, TimeUnit.MILLISECONDS);

        committers.put(connectorTaskId, commitFuture);
    }

    @After
    public void after() {
        scheduledExecutorService.shutdown();
        sourceTaskOffsetCommitter.close(100);
    }

    @Test
    public void scheduleTest() throws InterruptedException {
        sourceTaskOffsetCommitter.schedule(connectorTaskId, workerSourceTask);
        TimeUnit.SECONDS.sleep(1);
    }

    @Test
    public void removeTest() {
        sourceTaskOffsetCommitter.remove(connectorTaskId);
    }


}
