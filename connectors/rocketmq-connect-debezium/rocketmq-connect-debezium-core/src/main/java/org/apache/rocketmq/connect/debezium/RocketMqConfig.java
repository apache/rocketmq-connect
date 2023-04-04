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

import java.util.Objects;

/**
 * Configuration for connecting RocketMq
 */
public class RocketMqConfig {
    private String namesrvAddr;

    private String groupId;

    /**
     * set acl config
     **/
    private boolean aclEnable;
    private String accessKey;
    private String secretKey;

    public static Builder newBuilder() {
        return new Builder();
    }

    private RocketMqConfig(String rmqConsumerGroup, String namesrvAddr, boolean aclEnable, String accessKey,
                           String secretKey) {
        this.groupId = rmqConsumerGroup;
        this.namesrvAddr = namesrvAddr;
        this.aclEnable = aclEnable;
        this.accessKey = accessKey;
        this.secretKey = secretKey;
    }

    public String getNamesrvAddr() {
        return namesrvAddr;
    }

    public String getGroupId() {
        return groupId;
    }

    public boolean isAclEnable() {
        return aclEnable;
    }

    public String getAccessKey() {
        return accessKey;
    }

    public String getSecretKey() {
        return secretKey;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RocketMqConfig that = (RocketMqConfig) o;
        return aclEnable == that.aclEnable && Objects.equals(namesrvAddr, that.namesrvAddr) && Objects.equals(groupId, that.groupId) && Objects.equals(accessKey, that.accessKey) && Objects.equals(secretKey, that.secretKey);
    }

    @Override
    public int hashCode() {
        return Objects.hash(namesrvAddr, groupId, aclEnable, accessKey, secretKey);
    }

    @Override
    public String toString() {
        return "RocketMqConfig{" +
                "namesrvAddr='" + namesrvAddr + '\'' +
                ", groupId='" + groupId + '\'' +
                ", aclEnable=" + aclEnable +
                ", accessKey='" + accessKey + '\'' +
                ", secretKey='" + secretKey + '\'' +
                '}';
    }

    public static class Builder {
        private String namesrvAddr;
        private String groupId;
        /**
         * set acl config
         **/
        private boolean aclEnable;
        private String accessKey;
        private String secretKey;

        public Builder namesrvAddr(String namesrvAddr) {
            this.namesrvAddr = namesrvAddr;
            return this;
        }

        public Builder groupId(String groupId) {
            this.groupId = groupId;
            return this;
        }

        public Builder aclEnable(boolean aclEnable) {
            this.aclEnable = aclEnable;
            return this;
        }

        public Builder accessKey(String accessKey) {
            this.accessKey = accessKey;
            return this;
        }

        public Builder secretKey(String secretKey) {
            this.secretKey = secretKey;
            return this;
        }

        public RocketMqConfig build() {
            return new RocketMqConfig(groupId, namesrvAddr, aclEnable, accessKey, secretKey);
        }
    }
}
