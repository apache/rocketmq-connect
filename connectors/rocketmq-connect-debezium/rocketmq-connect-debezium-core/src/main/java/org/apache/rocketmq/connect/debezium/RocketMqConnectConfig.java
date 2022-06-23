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

package org.apache.rocketmq.connect.debezium;


import io.debezium.config.Configuration;

/**
 * rocketmq connect config
 */
public class RocketMqConnectConfig {

    private String dbHistoryName;

    private String namesrvAddr;

    private String rmqProducerGroup;

    private int operationTimeout = 3000;

    private String rmqConsumerGroup;

    private int rmqMaxRedeliveryTimes;

    private int rmqMessageConsumeTimeout = 3000;

    private int rmqMaxConsumeThreadNums = 32;

    private int rmqMinConsumeThreadNums = 1;

    private String adminExtGroup;

    // set acl config
    private boolean aclEnable;
    private String accessKey;
    private String secretKey;


    public RocketMqConnectConfig() {
    }

    public RocketMqConnectConfig(Configuration config, String dbHistoryName) {
        this.dbHistoryName = dbHistoryName;
        // init config
        this.rmqProducerGroup = this.dbHistoryName.concat("-producer-group");
        this.rmqConsumerGroup = this.dbHistoryName.concat("-consumer-group");
        this.adminExtGroup = this.dbHistoryName.concat("-admin-group");

        // init rocketmq connection
        this.namesrvAddr = config.getString(RocketMqDatabaseHistory.NAME_SRV_ADDR);
        this.aclEnable = config.getBoolean(RocketMqDatabaseHistory.ROCKETMQ_ACL_ENABLE);
        this.accessKey = config.getString(RocketMqDatabaseHistory.ROCKETMQ_ACCESS_KEY);
        this.secretKey = config.getString(RocketMqDatabaseHistory.ROCKETMQ_SECRET_KEY);
    }


    public String getDbHistoryName() {
        return dbHistoryName;
    }

    public void setDbHistoryName(String dbHistoryName) {
        this.dbHistoryName = dbHistoryName;
    }

    public String getNamesrvAddr() {
        return namesrvAddr;
    }

    public void setNamesrvAddr(String namesrvAddr) {
        this.namesrvAddr = namesrvAddr;
    }

    public String getRmqProducerGroup() {
        return rmqProducerGroup;
    }

    public void setRmqProducerGroup(String rmqProducerGroup) {
        this.rmqProducerGroup = rmqProducerGroup;
    }

    public int getOperationTimeout() {
        return operationTimeout;
    }

    public void setOperationTimeout(int operationTimeout) {
        this.operationTimeout = operationTimeout;
    }

    public String getRmqConsumerGroup() {
        return rmqConsumerGroup;
    }

    public void setRmqConsumerGroup(String rmqConsumerGroup) {
        this.rmqConsumerGroup = rmqConsumerGroup;
    }

    public int getRmqMaxRedeliveryTimes() {
        return rmqMaxRedeliveryTimes;
    }

    public void setRmqMaxRedeliveryTimes(int rmqMaxRedeliveryTimes) {
        this.rmqMaxRedeliveryTimes = rmqMaxRedeliveryTimes;
    }

    public int getRmqMessageConsumeTimeout() {
        return rmqMessageConsumeTimeout;
    }

    public void setRmqMessageConsumeTimeout(int rmqMessageConsumeTimeout) {
        this.rmqMessageConsumeTimeout = rmqMessageConsumeTimeout;
    }

    public int getRmqMaxConsumeThreadNums() {
        return rmqMaxConsumeThreadNums;
    }

    public void setRmqMaxConsumeThreadNums(int rmqMaxConsumeThreadNums) {
        this.rmqMaxConsumeThreadNums = rmqMaxConsumeThreadNums;
    }

    public int getRmqMinConsumeThreadNums() {
        return rmqMinConsumeThreadNums;
    }

    public void setRmqMinConsumeThreadNums(int rmqMinConsumeThreadNums) {
        this.rmqMinConsumeThreadNums = rmqMinConsumeThreadNums;
    }

    public boolean isAclEnable() {
        return aclEnable;
    }

    public void setAclEnable(boolean aclEnable) {
        this.aclEnable = aclEnable;
    }

    public String getAccessKey() {
        return accessKey;
    }

    public void setAccessKey(String accessKey) {
        this.accessKey = accessKey;
    }

    public String getSecretKey() {
        return secretKey;
    }

    public void setSecretKey(String secretKey) {
        this.secretKey = secretKey;
    }

    public String getAdminExtGroup() {
        return adminExtGroup;
    }

    public void setAdminExtGroup(String adminExtGroup) {
        this.adminExtGroup = adminExtGroup;
    }

    @Override
    public String toString() {
        return "RocketMqConnectConfig{" +
                "dbHistoryName='" + dbHistoryName + '\'' +
                ", namesrvAddr='" + namesrvAddr + '\'' +
                ", rmqProducerGroup='" + rmqProducerGroup + '\'' +
                ", operationTimeout=" + operationTimeout +
                ", rmqConsumerGroup='" + rmqConsumerGroup + '\'' +
                ", rmqMaxRedeliveryTimes=" + rmqMaxRedeliveryTimes +
                ", rmqMessageConsumeTimeout=" + rmqMessageConsumeTimeout +
                ", rmqMaxConsumeThreadNums=" + rmqMaxConsumeThreadNums +
                ", rmqMinConsumeThreadNums=" + rmqMinConsumeThreadNums +
                ", adminExtGroup='" + adminExtGroup + '\'' +
                ", aclEnable=" + aclEnable +
                ", accessKey='" + accessKey + '\'' +
                ", secretKey='" + secretKey + '\'' +
                '}';
    }
}
