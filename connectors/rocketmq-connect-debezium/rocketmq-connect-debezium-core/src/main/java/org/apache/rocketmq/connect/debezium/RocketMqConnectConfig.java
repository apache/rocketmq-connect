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

import java.util.Objects;

/**
 * rocketmq connect config
 */
public class RocketMqConnectConfig {
    private String namesrvAddr;

    private int operationTimeout = 3000;

    private String rmqConsumerGroup;

    private int rmqMaxRedeliveryTimes;

    private int rmqMessageConsumeTimeout = 3000;

    private int rmqMaxConsumeThreadNums = 32;

    private int rmqMinConsumeThreadNums = 1;


    /** set acl config **/
    private boolean aclEnable;
    private String accessKey;
    private String secretKey;


    public static Builder newBuilder(){
        return new Builder();
    }

    public RocketMqConnectConfig() {}

    public RocketMqConnectConfig(String rmqConsumerGroup, String namesrvAddr, boolean aclEnable, String accessKey, String secretKey) {
        this.rmqConsumerGroup = rmqConsumerGroup;
        this.namesrvAddr = namesrvAddr;
        this.aclEnable = aclEnable;
        this.accessKey = accessKey;
        this.secretKey = secretKey;
    }

    public RocketMqConnectConfig(Configuration config, String prefixGroupName) {
        this.rmqConsumerGroup = prefixGroupName.concat("-group");
        // init rocketmq connection
        this.namesrvAddr = config.getString(RocketMqDatabaseHistory.NAME_SRV_ADDR);
        this.aclEnable = config.getBoolean(RocketMqDatabaseHistory.ROCKETMQ_ACL_ENABLE);
        this.accessKey = config.getString(RocketMqDatabaseHistory.ROCKETMQ_ACCESS_KEY);
        this.secretKey = config.getString(RocketMqDatabaseHistory.ROCKETMQ_SECRET_KEY);
    }

    public String getNamesrvAddr() {
        return namesrvAddr;
    }

    public void setNamesrvAddr(String namesrvAddr) {
        this.namesrvAddr = namesrvAddr;
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

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RocketMqConnectConfig that = (RocketMqConnectConfig) o;
        return operationTimeout == that.operationTimeout && rmqMaxRedeliveryTimes == that.rmqMaxRedeliveryTimes && rmqMessageConsumeTimeout == that.rmqMessageConsumeTimeout && rmqMaxConsumeThreadNums == that.rmqMaxConsumeThreadNums && rmqMinConsumeThreadNums == that.rmqMinConsumeThreadNums && aclEnable == that.aclEnable && Objects.equals(namesrvAddr, that.namesrvAddr) && Objects.equals(rmqConsumerGroup, that.rmqConsumerGroup) && Objects.equals(accessKey, that.accessKey) && Objects.equals(secretKey, that.secretKey);
    }

    @Override
    public int hashCode() {
        return Objects.hash(namesrvAddr, operationTimeout, rmqConsumerGroup, rmqMaxRedeliveryTimes, rmqMessageConsumeTimeout, rmqMaxConsumeThreadNums, rmqMinConsumeThreadNums, aclEnable, accessKey, secretKey);
    }

    @Override
    public String toString() {
        return "RocketMqConnectConfig{" +
                "namesrvAddr='" + namesrvAddr + '\'' +
                ", operationTimeout=" + operationTimeout +
                ", rmqConsumerGroup='" + rmqConsumerGroup + '\'' +
                ", rmqMaxRedeliveryTimes=" + rmqMaxRedeliveryTimes +
                ", rmqMessageConsumeTimeout=" + rmqMessageConsumeTimeout +
                ", rmqMaxConsumeThreadNums=" + rmqMaxConsumeThreadNums +
                ", rmqMinConsumeThreadNums=" + rmqMinConsumeThreadNums +
                ", aclEnable=" + aclEnable +
                ", accessKey='" + accessKey + '\'' +
                ", secretKey='" + secretKey + '\'' +
                '}';
    }

    public static class Builder{
        private String namesrvAddr;
        private String rmqConsumerGroup;
        /** set acl config **/
        private boolean aclEnable;
        private String accessKey;
        private String secretKey;

        public Builder namesrvAddr(String namesrvAddr){
            this.namesrvAddr = namesrvAddr;
            return this;
        }

        public Builder rmqConsumerGroup(String rmqConsumerGroup){
            this.rmqConsumerGroup = rmqConsumerGroup;
            return this;
        }

        public Builder aclEnable(boolean aclEnable){
            this.aclEnable = aclEnable;
            return this;
        }

        public Builder accessKey(String accessKey){
            this.accessKey = accessKey;
            return this;
        }
        public Builder secretKey(String secretKey){
            this.secretKey = secretKey;
            return this;
        }
        public  RocketMqConnectConfig build(){
            return new RocketMqConnectConfig(rmqConsumerGroup, namesrvAddr, aclEnable, accessKey, secretKey);
        }
    }
}
