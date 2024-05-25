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

package org.apache.rocketmq.connect.hologres.config;

import io.openmessaging.KeyValue;

import static org.apache.rocketmq.connect.hologres.config.HologresConstant.SOURCE_BATCH_SIZE;
import static org.apache.rocketmq.connect.hologres.config.HologresConstant.SOURCE_BATCH_SIZE_DEFAULT;
import static org.apache.rocketmq.connect.hologres.config.HologresConstant.SOURCE_COMMIT_TIME_INTERVAL;
import static org.apache.rocketmq.connect.hologres.config.HologresConstant.SOURCE_COMMIT_TIME_INTERVAL_DEFAULT;
import static org.apache.rocketmq.connect.hologres.config.HologresConstant.SOURCE_HEARTBEAT_INTERVAL;
import static org.apache.rocketmq.connect.hologres.config.HologresConstant.SOURCE_HEARTBEAT_INTERVAL_DEFAULT;
import static org.apache.rocketmq.connect.hologres.config.HologresConstant.SOURCE_IGNORE_BEFORE_UPDATE;
import static org.apache.rocketmq.connect.hologres.config.HologresConstant.SOURCE_IGNORE_BEFORE_UPDATE_DEFAULT;
import static org.apache.rocketmq.connect.hologres.config.HologresConstant.SOURCE_IGNORE_DELETE;
import static org.apache.rocketmq.connect.hologres.config.HologresConstant.SOURCE_IGNORE_DELETE_DEFAULT;
import static org.apache.rocketmq.connect.hologres.config.HologresConstant.SOURCE_RETRY_COUNT;
import static org.apache.rocketmq.connect.hologres.config.HologresConstant.SOURCE_RETRY_COUNT_DEFAULT;

public class HologresSourceConfig extends AbstractHologresConfig {

    private int binlogReadBatchSize;
    private int binlogHeartBeatIntervalMs;
    private boolean binlogIgnoreDelete;
    private boolean binlogIgnoreBeforeUpdate;
    private int retryCount;
    private int binlogCommitTimeIntervalMs;

    private String slotName;
    // format: YYYY-mm-dd HH:mm:ss
    private String startTime;

    public HologresSourceConfig(KeyValue keyValue) {
        super(keyValue);

        this.binlogReadBatchSize = keyValue.getInt(SOURCE_BATCH_SIZE, SOURCE_BATCH_SIZE_DEFAULT);
        this.binlogHeartBeatIntervalMs = keyValue.getInt(SOURCE_HEARTBEAT_INTERVAL, SOURCE_HEARTBEAT_INTERVAL_DEFAULT);
        this.binlogIgnoreDelete = Boolean.parseBoolean(keyValue.getString(SOURCE_IGNORE_DELETE, SOURCE_IGNORE_DELETE_DEFAULT));
        this.binlogIgnoreBeforeUpdate = Boolean.parseBoolean(keyValue.getString(SOURCE_IGNORE_BEFORE_UPDATE, SOURCE_IGNORE_BEFORE_UPDATE_DEFAULT));
        this.retryCount = keyValue.getInt(SOURCE_RETRY_COUNT, SOURCE_RETRY_COUNT_DEFAULT);
        this.binlogCommitTimeIntervalMs = keyValue.getInt(SOURCE_COMMIT_TIME_INTERVAL, SOURCE_COMMIT_TIME_INTERVAL_DEFAULT);
    }

    public int getBinlogReadBatchSize() {
        return binlogReadBatchSize;
    }

    public void setBinlogReadBatchSize(int binlogReadBatchSize) {
        this.binlogReadBatchSize = binlogReadBatchSize;
    }

    public int getBinlogHeartBeatIntervalMs() {
        return binlogHeartBeatIntervalMs;
    }

    public void setBinlogHeartBeatIntervalMs(int binlogHeartBeatIntervalMs) {
        this.binlogHeartBeatIntervalMs = binlogHeartBeatIntervalMs;
    }

    public boolean isBinlogIgnoreDelete() {
        return binlogIgnoreDelete;
    }

    public void setBinlogIgnoreDelete(boolean binlogIgnoreDelete) {
        this.binlogIgnoreDelete = binlogIgnoreDelete;
    }

    public boolean isBinlogIgnoreBeforeUpdate() {
        return binlogIgnoreBeforeUpdate;
    }

    public void setBinlogIgnoreBeforeUpdate(boolean binlogIgnoreBeforeUpdate) {
        this.binlogIgnoreBeforeUpdate = binlogIgnoreBeforeUpdate;
    }

    public int getRetryCount() {
        return retryCount;
    }

    public void setRetryCount(int retryCount) {
        this.retryCount = retryCount;
    }

    public String getSlotName() {
        return slotName;
    }

    public void setSlotName(String slotName) {
        this.slotName = slotName;
    }

    public String getStartTime() {
        return startTime;
    }

    public void setStartTime(String startTime) {
        this.startTime = startTime;
    }

    public int getBinlogCommitTimeIntervalMs() {
        return binlogCommitTimeIntervalMs;
    }

    public void setBinlogCommitTimeIntervalMs(int binlogCommitTimeIntervalMs) {
        this.binlogCommitTimeIntervalMs = binlogCommitTimeIntervalMs;
    }
}
