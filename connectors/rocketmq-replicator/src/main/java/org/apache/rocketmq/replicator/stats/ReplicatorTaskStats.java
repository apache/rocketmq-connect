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
package org.apache.rocketmq.replicator.stats;


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.rocketmq.connect.runtime.stats.ConnectStatsManager;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * @author osgoo
 * @date 2022/6/22
 * depends on ConnectStatsManager
 */
public class ReplicatorTaskStats {
    // todo log 输出到 rocketmq_client.log
    private static final Log log = LogFactory.getLog(ReplicatorTaskStats.class);
    // additional item
    private static boolean additionalStatsEnable = false;
    // statsItems
    public static final String REPLICATOR_SOURCE_TASK_DELAY_NUMS = "REPLICATOR_SOURCE_TASK_DELAY_NUMS";
    public static final String REPLICATOR_SOURCE_TASK_DELAY_MS = "REPLICATOR_SOURCE_TASK_DELAY_MS";

    // todo p2p delay, src born timestamp --->  sink dest timestamp
    // todo rpo 计算规则
    public static final String REPLICATOR_HEARTBEAT_DELAY_MS = "REPLICATOR_HEARTBEAT_DELAY_MS";

    public static List<String> additionalItems = new ArrayList<>();
    static {
        additionalItems.add(REPLICATOR_SOURCE_TASK_DELAY_NUMS);
        additionalItems.add(REPLICATOR_SOURCE_TASK_DELAY_MS);
        additionalItems.add(REPLICATOR_HEARTBEAT_DELAY_MS);
    }
    private static ConnectStatsManager connectStatsManager;

    public static synchronized void init() {
        // init statsItem
        connectStatsManager = new ConnectStatsManager(UUID.randomUUID().toString());
        connectStatsManager.initAdditionalItems(ReplicatorTaskStats.additionalItems);
        additionalStatsEnable = true;
        log.info("Replicator added additional items.");
    }

    public static ConnectStatsManager getConnectStatsManager() {
        return connectStatsManager;
    }

    public static void incItemValue(String itemName, String key, int incValue, int incTimes) {
        if (!additionalStatsEnable || connectStatsManager == null || itemName == null || key == null) {
            log.warn("Replicator stats not enabled. connectStatsManager : " + connectStatsManager + ", itemName : " + itemName + ", key : " + key);
            return;
        }
        connectStatsManager.incAdditionalItem(itemName, key, incValue, incTimes);
    }

    public static void removeItem(String itemName, String key) {
        if (!additionalStatsEnable || connectStatsManager == null || itemName == null || key == null) {
            log.warn("Replicator stats not enabled. connectStatsManager : " + connectStatsManager + ", itemName : " + itemName + ", key : " + key);
            return;
        }
        connectStatsManager.removeAdditionalItem(itemName, key);
    }

}
