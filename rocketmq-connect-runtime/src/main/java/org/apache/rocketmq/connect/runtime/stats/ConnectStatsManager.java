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
 *
 */
package org.apache.rocketmq.connect.runtime.stats;

import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.stats.StatsItem;
import org.apache.rocketmq.common.stats.StatsItemSet;
import org.apache.rocketmq.common.utils.ThreadUtils;
import org.apache.rocketmq.connect.runtime.config.WorkerConfig;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;

import static org.apache.rocketmq.connect.runtime.common.LoggerName.ROCKETMQ_CONNECT_STATS;

public class ConnectStatsManager {

    public static final String SOURCE_RECORD_WRITE_NUMS = "SOURCE_RECORD_WRITE_NUMS";
    public static final String SOURCE_RECORD_WRITE_TOTAL_NUMS = "SOURCE_RECORD_WRITE_TOTAL_NUMS";

    public static final String SOURCE_RECORD_WRITE_FAIL_NUMS = "SOURCE_RECORD_WRITE_FAIL_NUMS";
    public static final String SOURCE_RECORD_WRITE_TOTAL_FAIL_NUMS = "SOURCE_RECORD_WRITE_TOTAL_FAIL_NUMS";

    public static final String SOURCE_RECORD_POLL_NUMS = "SOURCE_RECORD_POLL_NUMS";
    public static final String SOURCE_RECORD_POLL_TOTAL_NUMS = "SOURCE_RECORD_POLL_TOTAL_NUMS";

    public static final String SOURCE_RECORD_POLL_FAIL_NUMS = "SOURCE_RECORD_POLL_FAIL_NUMS";
    public static final String SOURCE_RECORD_POLL_FAIL_TOTAL_NUMS = "SOURCE_RECORD_POLL_FAIL_TOTAL_NUMS";

    public static final String SINK_RECORD_READ_NUMS = "SINK_RECORD_READ_NUMS";
    public static final String SINK_RECORD_READ_TOTAL_NUMS = "SINK_RECORD_READ_TOTAL_NUMS";

    public static final String SINK_RECORD_READ_RT = "SINK_RECORD_READ_RT";
    public static final String SINK_RECORD_READ_TOTAL_RT = "SINK_RECORD_READ_TOTAL_RT";

    public static final String SINK_RECORD_READ_FAIL_NUMS = "SINK_RECORD_READ_FAIL_NUMS";
    public static final String SINK_RECORD_READ_TOTAL_FAIL_NUMS = "SINK_RECORD_READ_TOTAL_FAIL_NUMS";

    public static final String SINK_RECORD_READ_FAIL_RT = "SINK_RECORD_READ_FAIL_RT";
    public static final String SINK_RECORD_READ_TOTAL_FAIL_RT = "SINK_RECORD_READ_TOTAL_FAIL_RT";

    public static final String SINK_RECORD_PUT_NUMS = "SINK_RECORD_PUT_NUMS";
    public static final String SINK_RECORD_PUT_TOTAL_NUMS = "SINK_RECORD_PUT_TOTAL_NUMS";

    public static final String SINK_RECORD_PUT_RT = "SINK_RECORD_PUT_RT";
    public static final String SINK_RECORD_PUT_TOTAL_RT = "SINK_RECORD_PUT_TOTAL_RT";

    public static final String SINK_RECORD_PUT_FAIL_NUMS = "SINK_RECORD_PUT_FAIL_NUMS";
    public static final String SINK_RECORD_PUT_TOTAL_FAIL_NUMS = "SINK_RECORD_PUT_TOTAL_FAIL_NUMS";

    public static final String SINK_RECORD_PUT_FAIL_RT = "SINK_RECORD_PUT_FAIL_RT";
    public static final String SINK_RECORD_PUT_TOTAL_FAIL_RT = "SINK_RECORD_PUT_TOTAL_FAIL_RT";

    public static final String SOURCE_RECORD_POLL_TOTAL_TIMES = "SOURCE_RECORD_POLL_TOTAL_TIMES";
    public static final String SINK_RECORD_READ_TOTAL_TIMES = "SINK_RECORD_READ_TOTAL_TIMES";

    /**
     * read disk follow stats
     */
    private static final InternalLogger log = InternalLoggerFactory.getLogger(ROCKETMQ_CONNECT_STATS);


    private static final InternalLogger COMMERCIAL_LOG = InternalLoggerFactory.getLogger(
            LoggerName.COMMERCIAL_LOGGER_NAME);
    private final ScheduledExecutorService scheduledExecutorService =
            ThreadUtils.newSingleThreadScheduledExecutor("ConnectStatsThread", true);
    private final ScheduledExecutorService commercialExecutor =
            ThreadUtils.newSingleThreadScheduledExecutor("CommercialStatsThread", true);
    private final ScheduledExecutorService accountExecutor =
            ThreadUtils.newSingleThreadScheduledExecutor("AccountStatsThread", true);

    private final HashMap<String, StatsItemSet> statsTable = new HashMap<String, StatsItemSet>();
    private final String worker;
    private WorkerConfig connectConfig;

    public ConnectStatsManager(WorkerConfig connectConfig) {
        this.connectConfig = connectConfig;
        this.worker = connectConfig.getWorkerId();
        init();
    }

    public ConnectStatsManager(String worker) {
        this.worker = worker;
    }

    public void init() {
        this.statsTable.put(SOURCE_RECORD_WRITE_NUMS, new StatsItemSet(SOURCE_RECORD_WRITE_NUMS, this.scheduledExecutorService, log));
        this.statsTable.put(SOURCE_RECORD_WRITE_TOTAL_NUMS, new StatsItemSet(SOURCE_RECORD_WRITE_TOTAL_NUMS, this.scheduledExecutorService, log));
        this.statsTable.put(SOURCE_RECORD_WRITE_FAIL_NUMS, new StatsItemSet(SOURCE_RECORD_WRITE_FAIL_NUMS, this.scheduledExecutorService, log));
        this.statsTable.put(SOURCE_RECORD_WRITE_TOTAL_FAIL_NUMS, new StatsItemSet(SOURCE_RECORD_WRITE_TOTAL_FAIL_NUMS, this.scheduledExecutorService, log));
        this.statsTable.put(SOURCE_RECORD_POLL_NUMS, new StatsItemSet(SOURCE_RECORD_POLL_NUMS, this.scheduledExecutorService, log));
        this.statsTable.put(SOURCE_RECORD_POLL_TOTAL_NUMS, new StatsItemSet(SOURCE_RECORD_POLL_TOTAL_NUMS, this.scheduledExecutorService, log));
        this.statsTable.put(SOURCE_RECORD_POLL_FAIL_NUMS, new StatsItemSet(SOURCE_RECORD_POLL_FAIL_NUMS, this.scheduledExecutorService, log));
        this.statsTable.put(SOURCE_RECORD_POLL_FAIL_TOTAL_NUMS, new StatsItemSet(SOURCE_RECORD_POLL_FAIL_TOTAL_NUMS, this.scheduledExecutorService, log));
        this.statsTable.put(SINK_RECORD_READ_NUMS, new StatsItemSet(SINK_RECORD_READ_NUMS, this.scheduledExecutorService, log));
        this.statsTable.put(SINK_RECORD_READ_TOTAL_NUMS, new StatsItemSet(SINK_RECORD_READ_TOTAL_NUMS, this.scheduledExecutorService, log));
        this.statsTable.put(SINK_RECORD_READ_FAIL_NUMS, new StatsItemSet(SINK_RECORD_READ_FAIL_NUMS, this.scheduledExecutorService, log));
        this.statsTable.put(SINK_RECORD_READ_TOTAL_FAIL_NUMS, new StatsItemSet(SINK_RECORD_READ_TOTAL_FAIL_NUMS, this.scheduledExecutorService, log));
        this.statsTable.put(SINK_RECORD_PUT_NUMS, new StatsItemSet(SINK_RECORD_PUT_NUMS, this.scheduledExecutorService, log));
        this.statsTable.put(SINK_RECORD_PUT_TOTAL_NUMS, new StatsItemSet(SINK_RECORD_PUT_TOTAL_NUMS, this.scheduledExecutorService, log));
        this.statsTable.put(SINK_RECORD_PUT_FAIL_NUMS, new StatsItemSet(SINK_RECORD_PUT_FAIL_NUMS, this.scheduledExecutorService, log));
        this.statsTable.put(SINK_RECORD_PUT_TOTAL_FAIL_NUMS, new StatsItemSet(SINK_RECORD_PUT_TOTAL_FAIL_NUMS, this.scheduledExecutorService, log));

        this.statsTable.put(SINK_RECORD_READ_RT, new StatsItemSet(SINK_RECORD_READ_RT, this.scheduledExecutorService, log));
        this.statsTable.put(SINK_RECORD_READ_TOTAL_RT, new StatsItemSet(SINK_RECORD_READ_TOTAL_RT, this.scheduledExecutorService, log));
        this.statsTable.put(SINK_RECORD_READ_FAIL_RT, new StatsItemSet(SINK_RECORD_READ_FAIL_RT, this.scheduledExecutorService, log));
        this.statsTable.put(SINK_RECORD_READ_TOTAL_FAIL_RT, new StatsItemSet(SINK_RECORD_READ_TOTAL_FAIL_RT, this.scheduledExecutorService, log));
        this.statsTable.put(SINK_RECORD_PUT_RT, new StatsItemSet(SINK_RECORD_PUT_RT, this.scheduledExecutorService, log));
        this.statsTable.put(SINK_RECORD_PUT_TOTAL_RT, new StatsItemSet(SINK_RECORD_PUT_TOTAL_RT, this.scheduledExecutorService, log));
        this.statsTable.put(SINK_RECORD_PUT_FAIL_RT, new StatsItemSet(SINK_RECORD_PUT_FAIL_RT, this.scheduledExecutorService, log));
        this.statsTable.put(SINK_RECORD_PUT_TOTAL_FAIL_RT, new StatsItemSet(SINK_RECORD_PUT_TOTAL_FAIL_RT, this.scheduledExecutorService, log));

        this.statsTable.put(SOURCE_RECORD_POLL_TOTAL_TIMES, new StatsItemSet(SOURCE_RECORD_POLL_TOTAL_TIMES, this.scheduledExecutorService, log));
        this.statsTable.put(SINK_RECORD_READ_TOTAL_TIMES, new StatsItemSet(SINK_RECORD_READ_TOTAL_TIMES, this.scheduledExecutorService, log));
    }

    public void start() {
    }

    public void shutdown() {
        this.scheduledExecutorService.shutdown();
        this.commercialExecutor.shutdown();
    }

    public StatsItem getStatsItem(final String statsName, final String statsKey) {
        try {
            return this.statsTable.get(statsName).getStatsItem(statsKey);
        } catch (Exception e) {
        }

        return null;
    }

    public void incSourceRecordPollTotalNums(int incValue) {
        this.statsTable.get(SOURCE_RECORD_POLL_TOTAL_NUMS).addValue(worker, incValue, 1);

    }

    public void incSourceRecordPollNums(String taskId, int incValue) {
        if (StringUtils.isBlank(taskId)) {
            return;
        }
        this.statsTable.get(SOURCE_RECORD_POLL_NUMS).addValue(taskId, incValue, 1);
    }

    public void incSourceRecordPollTotalFailNums() {
        this.statsTable.get(SOURCE_RECORD_POLL_FAIL_TOTAL_NUMS).addValue(worker, 1, 1);

    }

    public void incSourceRecordPollFailNums(String taskId) {
        if (StringUtils.isBlank(taskId)) {
            return;
        }
        this.statsTable.get(SOURCE_RECORD_POLL_FAIL_NUMS).addValue(taskId, 1, 1);
    }

    public void incSourceRecordWriteTotalNums() {
        this.statsTable.get(SOURCE_RECORD_WRITE_TOTAL_NUMS).addValue(worker, 1, 1);
    }

    public void incSourceRecordWriteNums(String taskId) {
        if (StringUtils.isBlank(taskId)) {
            return;
        }
        this.statsTable.get(SOURCE_RECORD_WRITE_NUMS).addValue(taskId, 1, 1);
    }

    public void incSourceRecordWriteTotalFailNums() {
        this.statsTable.get(SOURCE_RECORD_WRITE_TOTAL_FAIL_NUMS).addValue(worker, 1, 1);
    }

    public void incSourceRecordWriteFailNums(String taskId) {
        if (StringUtils.isBlank(taskId)) {
            return;
        }
        this.statsTable.get(SOURCE_RECORD_WRITE_FAIL_NUMS).addValue(taskId, 1, 1);
    }

    public void incSinkRecordPutTotalFailNums() {
        this.statsTable.get(SINK_RECORD_PUT_TOTAL_FAIL_NUMS).addValue(worker, 1, 1);
    }

    public void incSinkRecordPutFailNums(String taskId) {
        if (StringUtils.isBlank(taskId)) {
            return;
        }
        this.statsTable.get(SINK_RECORD_PUT_FAIL_NUMS).addValue(taskId, 1, 1);
    }

    public void incSinkRecordReadTotalFailNums() {
        this.statsTable.get(SINK_RECORD_READ_TOTAL_FAIL_NUMS).addValue(worker, 1, 1);
    }

    public void incSinkRecordReadFailNums(String taskId) {
        if (StringUtils.isBlank(taskId)) {
            return;
        }
        this.statsTable.get(SINK_RECORD_READ_FAIL_NUMS).addValue(taskId, 1, 1);
    }

    public void incSinkRecordReadTotalNums(int incValue) {
        this.statsTable.get(SINK_RECORD_READ_TOTAL_NUMS).addValue(worker, incValue, 1);
    }

    public void incSinkRecordReadNums(String taskId) {
        if (StringUtils.isBlank(taskId)) {
            return;
        }
        this.statsTable.get(SINK_RECORD_READ_NUMS).addValue(taskId, 1, 1);
    }

    public void incSinkRecordReadNums(String taskId, int incValue) {
        if (StringUtils.isBlank(taskId)) {
            return;
        }
        this.statsTable.get(SINK_RECORD_READ_NUMS).addValue(taskId, incValue, 1);
    }

    public void incSinkRecordPutTotalNums() {
        this.statsTable.get(SINK_RECORD_PUT_TOTAL_NUMS).addValue(worker, 1, 1);
    }

    public void incSinkRecordPutNums(String taskId) {
        if (StringUtils.isBlank(taskId)) {
            return;
        }
        this.statsTable.get(SINK_RECORD_PUT_NUMS).addValue(taskId, 1, 1);
    }


    public void incSinkRecordPutTotalFailRT(final long rt) {
        this.statsTable.get(SINK_RECORD_PUT_TOTAL_FAIL_RT).addValue(worker, (int) rt, 1);
    }

    public void incSinkRecordPutFailRT(String taskId, final long rt) {
        if (StringUtils.isBlank(taskId)) {
            return;
        }
        this.statsTable.get(SINK_RECORD_PUT_FAIL_RT).addValue(taskId, (int) rt, 1);
    }

    public void incSinkRecordReadTotalFailRT(final long rt) {
        this.statsTable.get(SINK_RECORD_READ_TOTAL_FAIL_RT).addValue(worker, (int) rt, 1);
    }

    public void incSinkRecordReadFailRT(String taskId, final long rt) {
        if (StringUtils.isBlank(taskId)) {
            return;
        }
        this.statsTable.get(SINK_RECORD_READ_FAIL_RT).addValue(taskId, (int) rt, 1);
    }

    public void incSinkRecordReadTotalRT(final long rt) {
        this.statsTable.get(SINK_RECORD_READ_TOTAL_RT).addValue(worker, (int) rt, 1);
    }

    public void incSinkRecordReadRT(String taskId, final long rt) {
        if (StringUtils.isBlank(taskId)) {
            return;
        }
        this.statsTable.get(SINK_RECORD_READ_RT).addValue(taskId, (int) rt, 1);
    }

    public void incSinkRecordPutTotalRT(final long rt) {
        this.statsTable.get(SINK_RECORD_PUT_TOTAL_RT).addValue(worker, (int) rt, 1);
    }

    public void incSinkRecordPutRT(String taskId, final long rt) {
        if (StringUtils.isBlank(taskId)) {
            return;
        }
        this.statsTable.get(SINK_RECORD_PUT_RT).addValue(taskId, (int) rt, 1);
    }

    public void incSourceRecordPollTotalTimes() {
        this.statsTable.get(SOURCE_RECORD_POLL_TOTAL_TIMES).addValue(worker, 1, 1);
    }

    public void incSinkRecordReadTotalTimes() {
        this.statsTable.get(SINK_RECORD_READ_TOTAL_TIMES).addValue(worker, 1, 1);
    }

    public void initAdditionalItems(List<String> additionalItems) {
        for (String additionalItem : additionalItems) {
            if (this.statsTable.containsKey(additionalItem)) {
                log.warn("Already exists statsItem : " + additionalItem + ", just skip");
                continue;
            }
            this.statsTable.put(additionalItem, new StatsItemSet(additionalItem, scheduledExecutorService, log));
        }
    }

    public void incAdditionalItem(String additionalItem, String key, int incValue, int incTimes) {
        StatsItemSet statsItemSet = this.statsTable.get(additionalItem);
        if (statsItemSet != null) {
            statsItemSet.addValue(key, incValue, incTimes);
        }
    }

    public void removeAdditionalItem(String additionalItem, String key) {
        StatsItemSet statsItemSet = this.statsTable.get(additionalItem);
        if (statsItemSet != null) {
            statsItemSet.delValue(key);
        }
    }
}
