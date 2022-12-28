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

package org.apache.rocketmq.connect.runtime.config;

import org.apache.rocketmq.common.MixAll;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import static org.apache.rocketmq.connect.runtime.common.LoggerName.ROCKETMQ_RUNTIME;

/**
 * Configurations for runtime.
 */
public class WorkerConfig {
    public static final String METRIC_CLASS = "metrics.reporter";
    public static final String CONNECT_HOME_PROPERTY = "connect.home.dir";
    public static final String CONNECT_HOME_ENV = "CONNECT_HOME";
    private static final Logger log = LoggerFactory.getLogger(ROCKETMQ_RUNTIME);
    private String connectHome = System.getProperty(CONNECT_HOME_PROPERTY, System.getenv(CONNECT_HOME_ENV));

    /**
     * The unique ID of each worker instance in the cluster
     */
    private String workerId = "DefaultWorker";

    /**
     * Group listening for cluster changes
     */
    private String connectClusterId = "DefaultConnectCluster";

    /**
     * config example:
     * namesrvAddr = localhost:9876
     */
    private String namesrvAddr = System.getProperty(MixAll.NAMESRV_ADDR_PROPERTY, System.getenv(MixAll.NAMESRV_ADDR_ENV));

    /**
     * Http port for REST API.
     */
    private int httpPort = 8082;

    /**
     * plugin paths config;
     * Multiple use ',' split
     * config example:
     * pluginPaths = /tmp/plugins/,/app/plugins/
     */
    private String pluginPaths;

    /**
     * rocketmq access control config;
     * config example:
     * aclEnable = true
     * accessKey = 12345
     * secretKey = 11111
     */
    private boolean aclEnable = false;
    private String accessKey;
    private String secretKey;

    /**
     * auto create group enable config:
     * config example:
     * autoCreateGroupEnable = false
     */
    private boolean autoCreateGroupEnable = false;

    /**
     * Configure cluster converter
     */
    private String keyConverter = "org.apache.rocketmq.connect.runtime.converter.record.StringConverter";
    private String valueConverter = "org.apache.rocketmq.connect.runtime.converter.record.StringConverter";

    /**
     * Storage directory for file store.
     * config example:
     * storePathRootDir = /tmp/storeRoot
     */
    private String storePathRootDir = System.getProperty("user.home") + File.separator + "connectorStore";

    private int maxMessageSize;

    private int operationTimeout = 3000;


    /**
     * Default topic to send/consume online or offline message.
     */
    private String clusterStoreTopic = "connector-cluster-topic";
    /**
     * Task allocated strategy
     * config example with default:
     * allocTaskStrategy
     */
    private String allocTaskStrategy = "org.apache.rocketmq.connect.runtime.service.strategy.DefaultAllocateConnAndTaskStrategy";


    /**
     * Default topic to send/consume config change message.
     */
    private String configStoreTopic = "connector-config-topic";

    /**
     * Connector configuration persistence interval.
     */
    private int configPersistInterval = 20 * 1000;

    /**
     * Default topic to send/consume position change message.
     */
    private String positionStoreTopic = "connector-position-topic";
    /**
     * Source task position persistence interval.
     */
    private int positionPersistInterval = 20 * 1000;

    /**
     * Default topic to send/consume offset change message.
     */
    private String offsetStoreTopic = "connector-offset-topic";

    /**
     * Sink task offset persistence interval.
     */
    private int offsetPersistInterval = 20 * 1000;

    /**
     * Default topic to send/consume state change message.
     */
    private String connectStatusTopic = "connector-status-topic";

    /**
     * Connector state persistence interval.
     */
    private int statePersistInterval = 20 * 1000;


    /**
     * RocketMq store topic producer group
     * config example with default:
     * rmqProducerGroup = connector-producer-group;
     */
    private String rmqProducerGroup = "connector-producer-group";
    /**
     * RocketMq store topic consumer group
     * config example with default:
     * rmqConsumerGroup = connector-consumer-group;
     */
    private String rmqConsumerGroup = "connector-consumer-group";

    /**
     * RocketMq admin group
     * config example with default:
     * adminExtGroup = connector-admin-group;
     */
    private String adminExtGroup = "connector-admin-group";

    private int rmqMaxRedeliveryTimes;

    private int rmqMessageConsumeTimeout = 3000;
    private int rmqMaxConsumeThreadNums = 32;
    private int rmqMinConsumeThreadNums = 1;
    private int brokerSuspendMaxTimeMillis = 300;


    /**
     * Task start timeout mills, default 3 minute
     * config example by default:
     * maxStartTimeoutMills = 1000 * 60 * 3
     */
    private long maxStartTimeoutMills = 1000 * 60 * 3;

    /**
     * task stop timeout mills, default 1 minute
     * config example by default:
     * maxStopTimeoutMills = 1000 * 60
     */
    private long maxStopTimeoutMills = 1000 * 60;


    /**
     * offset commit timeout(ms)
     * config example with default
     * offsetCommitTimeoutMsConfig = 5000L
     */
    private long offsetCommitTimeoutMsConfig = 5000L;

    /**
     * offset commit interval ms
     * config example with default
     * offsetCommitIntervalMsConfig = 10000L
     */
    private long offsetCommitIntervalMsConfig = 30000L;

    /**
     * metrics config
     */
    private boolean openLogMetricReporter = false;
    private String metricsConfigPath;
    private Map<String, Map<String, String>> metricsConfig = new HashMap<>();

    /**
     * Management service
     */
    private String clusterManagementService;
    private String configManagementService;
    private String positionManagementService;
    private String stateManagementService;

    public String getWorkerId() {
        return workerId;
    }

    public void setWorkerId(String workerId) {
        this.workerId = workerId;
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

    public int getMaxMessageSize() {
        return maxMessageSize;
    }

    public void setMaxMessageSize(int maxMessageSize) {
        this.maxMessageSize = maxMessageSize;
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

    public String getStorePathRootDir() {
        return storePathRootDir;
    }

    public void setStorePathRootDir(String storePathRootDir) {
        this.storePathRootDir = storePathRootDir;
    }

    public int getHttpPort() {
        return httpPort;
    }

    public void setHttpPort(int httpPort) {
        this.httpPort = httpPort;
    }

    public int getPositionPersistInterval() {
        return positionPersistInterval;
    }

    public void setPositionPersistInterval(int positionPersistInterval) {
        this.positionPersistInterval = positionPersistInterval;
    }

    public int getOffsetPersistInterval() {
        return offsetPersistInterval;
    }

    public void setOffsetPersistInterval(int offsetPersistInterval) {
        this.offsetPersistInterval = offsetPersistInterval;
    }

    public int getConfigPersistInterval() {
        return configPersistInterval;
    }

    public void setConfigPersistInterval(int configPersistInterval) {
        this.configPersistInterval = configPersistInterval;
    }

    public String getPluginPaths() {
        return pluginPaths;
    }

    public void setPluginPaths(String pluginPaths) {
        this.pluginPaths = pluginPaths;
    }

    public String getClusterStoreTopic() {
        return clusterStoreTopic;
    }

    public void setClusterStoreTopic(String clusterStoreTopic) {
        this.clusterStoreTopic = clusterStoreTopic;
    }

    public String getConfigStoreTopic() {
        return configStoreTopic;
    }

    public void setConfigStoreTopic(String configStoreTopic) {
        this.configStoreTopic = configStoreTopic;
    }

    public String getPositionStoreTopic() {
        return positionStoreTopic;
    }

    public void setPositionStoreTopic(String positionStoreTopic) {
        this.positionStoreTopic = positionStoreTopic;
    }

    public String getOffsetStoreTopic() {
        return offsetStoreTopic;
    }

    public void setOffsetStoreTopic(String offsetStoreTopic) {
        this.offsetStoreTopic = offsetStoreTopic;
    }

    public String getConnectStatusTopic() {
        return connectStatusTopic;
    }

    public void setConnectStatusTopic(String connectStatusTopic) {
        this.connectStatusTopic = connectStatusTopic;
    }

    public String getConnectClusterId() {
        return connectClusterId;
    }

    public void setConnectClusterId(String connectClusterId) {
        this.connectClusterId = connectClusterId;
    }

    public String getAllocTaskStrategy() {
        return this.allocTaskStrategy;
    }

    public void setAllocTaskStrategy(String allocTaskStrategy) {
        this.allocTaskStrategy = allocTaskStrategy;
    }

    public boolean getAclEnable() {
        return aclEnable;
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

    public boolean isAutoCreateGroupEnable() {
        return autoCreateGroupEnable;
    }

    public void setAutoCreateGroupEnable(boolean autoCreateGroupEnable) {
        this.autoCreateGroupEnable = autoCreateGroupEnable;
    }

    public String getAdminExtGroup() {
        return adminExtGroup;
    }

    public void setAdminExtGroup(String adminExtGroup) {
        this.adminExtGroup = adminExtGroup;
    }

    public String getConnectHome() {
        return connectHome;
    }

    public void setConnectHome(String connectHome) {
        this.connectHome = connectHome;
    }

    public long getOffsetCommitIntervalMsConfig() {
        return offsetCommitIntervalMsConfig;
    }

    public void setOffsetCommitIntervalMsConfig(long offsetCommitIntervalMsConfig) {
        this.offsetCommitIntervalMsConfig = offsetCommitIntervalMsConfig;
    }

    public long getMaxStartTimeoutMills() {
        return maxStartTimeoutMills;
    }

    public void setMaxStartTimeoutMills(long maxStartTimeoutMills) {
        this.maxStartTimeoutMills = maxStartTimeoutMills;
    }

    public long getMaxStopTimeoutMills() {
        return maxStopTimeoutMills;
    }

    public void setMaxStopTimeoutMills(long maxStopTimeoutMills) {
        this.maxStopTimeoutMills = maxStopTimeoutMills;
    }

    public long getOffsetCommitTimeoutMsConfig() {
        return offsetCommitTimeoutMsConfig;
    }

    public void setOffsetCommitTimeoutMsConfig(long offsetCommitTimeoutMsConfig) {
        this.offsetCommitTimeoutMsConfig = offsetCommitTimeoutMsConfig;
    }

    public int getBrokerSuspendMaxTimeMillis() {
        return brokerSuspendMaxTimeMillis;
    }

    public void setBrokerSuspendMaxTimeMillis(int brokerSuspendMaxTimeMillis) {
        this.brokerSuspendMaxTimeMillis = brokerSuspendMaxTimeMillis;
    }

    public boolean isAclEnable() {
        return aclEnable;
    }

    public void setAclEnable(boolean aclEnable) {
        this.aclEnable = aclEnable;
    }

    public String getKeyConverter() {
        return keyConverter;
    }

    public void setKeyConverter(String keyConverter) {
        this.keyConverter = keyConverter;
    }

    public String getValueConverter() {
        return valueConverter;
    }

    public void setValueConverter(String valueConverter) {
        this.valueConverter = valueConverter;
    }

    public int getStatePersistInterval() {
        return statePersistInterval;
    }

    public void setStatePersistInterval(int statePersistInterval) {
        this.statePersistInterval = statePersistInterval;
    }

    public String getMetricsConfigPath() {
        return metricsConfigPath;
    }

    public void setMetricsConfigPath(String metricsConfigPath) {
        this.metricsConfigPath = metricsConfigPath;
    }

    public Map<String, Map<String, String>> getMetricsConfig() {
        return metricsConfig;
    }

    public void setMetricsConfig(Map<String, Map<String, String>> metricsConfig) {
        this.metricsConfig = metricsConfig;
    }

    public boolean isOpenLogMetricReporter() {
        return openLogMetricReporter;
    }

    public void setOpenLogMetricReporter(boolean openLogMetricReporter) {
        this.openLogMetricReporter = openLogMetricReporter;
    }

    public String getClusterManagementService() {
        return clusterManagementService;
    }

    public void setClusterManagementService(String clusterManagementService) {
        this.clusterManagementService = clusterManagementService;
    }

    public String getConfigManagementService() {
        return configManagementService;
    }

    public void setConfigManagementService(String configManagementService) {
        this.configManagementService = configManagementService;
    }

    public String getPositionManagementService() {
        return positionManagementService;
    }

    public void setPositionManagementService(String positionManagementService) {
        this.positionManagementService = positionManagementService;
    }

    public String getStateManagementService() {
        return stateManagementService;
    }

    public void setStateManagementService(String stateManagementService) {
        this.stateManagementService = stateManagementService;
    }

    @Override
    public String toString() {
        return "WorkerConfig{" +
                "connectHome='" + connectHome + '\'' +
                ", workerId='" + workerId + '\'' +
                ", connectClusterId='" + connectClusterId + '\'' +
                ", namesrvAddr='" + namesrvAddr + '\'' +
                ", httpPort=" + httpPort +
                ", pluginPaths='" + pluginPaths + '\'' +
                ", aclEnable=" + aclEnable +
                ", accessKey='" + accessKey + '\'' +
                ", secretKey='" + secretKey + '\'' +
                ", autoCreateGroupEnable=" + autoCreateGroupEnable +
                ", keyConverter='" + keyConverter + '\'' +
                ", valueConverter='" + valueConverter + '\'' +
                ", storePathRootDir='" + storePathRootDir + '\'' +
                ", maxMessageSize=" + maxMessageSize +
                ", operationTimeout=" + operationTimeout +
                ", clusterStoreTopic='" + clusterStoreTopic + '\'' +
                ", allocTaskStrategy='" + allocTaskStrategy + '\'' +
                ", configStoreTopic='" + configStoreTopic + '\'' +
                ", configPersistInterval=" + configPersistInterval +
                ", positionStoreTopic='" + positionStoreTopic + '\'' +
                ", positionPersistInterval=" + positionPersistInterval +
                ", offsetStoreTopic='" + offsetStoreTopic + '\'' +
                ", offsetPersistInterval=" + offsetPersistInterval +
                ", connectStatusTopic='" + connectStatusTopic + '\'' +
                ", statePersistInterval=" + statePersistInterval +
                ", rmqProducerGroup='" + rmqProducerGroup + '\'' +
                ", rmqConsumerGroup='" + rmqConsumerGroup + '\'' +
                ", adminExtGroup='" + adminExtGroup + '\'' +
                ", rmqMaxRedeliveryTimes=" + rmqMaxRedeliveryTimes +
                ", rmqMessageConsumeTimeout=" + rmqMessageConsumeTimeout +
                ", rmqMaxConsumeThreadNums=" + rmqMaxConsumeThreadNums +
                ", rmqMinConsumeThreadNums=" + rmqMinConsumeThreadNums +
                ", brokerSuspendMaxTimeMillis=" + brokerSuspendMaxTimeMillis +
                ", maxStartTimeoutMills=" + maxStartTimeoutMills +
                ", maxStopTimeoutMills=" + maxStopTimeoutMills +
                ", offsetCommitTimeoutMsConfig=" + offsetCommitTimeoutMsConfig +
                ", offsetCommitIntervalMsConfig=" + offsetCommitIntervalMsConfig +
                ", openLogMetricReporter=" + openLogMetricReporter +
                ", metricsConfigPath='" + metricsConfigPath + '\'' +
                ", metricsConfig=" + metricsConfig +
                ", clusterManagementService='" + clusterManagementService + '\'' +
                ", configManagementService='" + configManagementService + '\'' +
                ", positionManagementService='" + positionManagementService + '\'' +
                ", stateManagementService='" + stateManagementService + '\'' +
                '}';
    }
}
