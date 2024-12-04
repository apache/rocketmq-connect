/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.rocketmq.connect.doris.cfg;

import io.openmessaging.KeyValue;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.stream.Collectors;
import org.apache.rocketmq.connect.doris.converter.ConverterMode;
import org.apache.rocketmq.connect.doris.converter.schema.SchemaEvolutionMode;
import org.apache.rocketmq.connect.doris.exception.DorisException;
import org.apache.rocketmq.connect.doris.utils.ConfigCheckUtils;
import org.apache.rocketmq.connect.doris.writer.DeliveryGuarantee;
import org.apache.rocketmq.connect.doris.writer.load.LoadModel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DorisOptions {
    private static final Logger LOG = LoggerFactory.getLogger(DorisOptions.class);
    private final String name;
    private final String urls;
    private final int queryPort;
    private final int httpPort;
    private final String user;
    private final String password;
    private final String database;
    private final Map<String, String> topicMap;
    private final int fileSize;
    private final int recordNum;
    private final long flushTime;
    private final boolean enableCustomJMX;
    private final int taskId;
    private final boolean enableDelete;
    private final boolean enable2PC;
    private boolean autoRedirect = true;
    private int requestReadTimeoutMs;
    private int requestConnectTimeoutMs;
    private final boolean enableGroupCommit;
    private boolean customCluster;
    private ProxyConfig proxyConfig;
    /**
     * Properties for the StreamLoad.
     */
    private final Properties streamLoadProp;
    private final String databaseTimeZone;
    private final LoadModel loadModel;
    private final DeliveryGuarantee deliveryGuarantee;
    private final ConverterMode converterMode;
    private final SchemaEvolutionMode schemaEvolutionMode;

    public DorisOptions(KeyValue config) {
        this.name = config.getString(DorisSinkConnectorConfig.NAME);
        this.urls = config.getString(DorisSinkConnectorConfig.DORIS_URLS);
        this.queryPort = Integer.parseInt(config.getString(DorisSinkConnectorConfig.DORIS_QUERY_PORT));
        this.httpPort = Integer.parseInt(config.getString(DorisSinkConnectorConfig.DORIS_HTTP_PORT));
        this.user = config.getString(DorisSinkConnectorConfig.DORIS_USER);
        this.password = config.getString(DorisSinkConnectorConfig.DORIS_PASSWORD);
        this.database = config.getString(DorisSinkConnectorConfig.DORIS_DATABASE);
        this.taskId = Integer.parseInt(config.getString(ConfigCheckUtils.TASK_ID));
        this.databaseTimeZone = config.getString(DorisSinkConnectorConfig.DATABASE_TIME_ZONE);
        this.loadModel = LoadModel.of(config.getString(DorisSinkConnectorConfig.LOAD_MODEL));
        this.deliveryGuarantee =
            DeliveryGuarantee.of(config.getString(DorisSinkConnectorConfig.DELIVERY_GUARANTEE));
        this.converterMode = ConverterMode.of(config.getString(DorisSinkConnectorConfig.CONVERTER_MODE));
        this.schemaEvolutionMode =
            SchemaEvolutionMode.of(
                config.getString(DorisSinkConnectorConfig.DEBEZIUM_SCHEMA_EVOLUTION));
        this.fileSize = Integer.parseInt(config.getString(DorisSinkConnectorConfig.BUFFER_SIZE_BYTES));
        this.recordNum =
            Integer.parseInt(config.getString(DorisSinkConnectorConfig.BUFFER_COUNT_RECORDS));

        this.flushTime = Long.parseLong(config.getString(DorisSinkConnectorConfig.BUFFER_FLUSH_TIME_SEC));
        this.topicMap = getTopicToTableMap(config);

        this.enable2PC = Boolean.parseBoolean(config.getString(DorisSinkConnectorConfig.ENABLE_2PC));
        this.enableCustomJMX = Boolean.parseBoolean(config.getString(DorisSinkConnectorConfig.JMX_OPT));
        this.enableDelete =
            Boolean.parseBoolean(config.getString(DorisSinkConnectorConfig.ENABLE_DELETE));
        this.requestConnectTimeoutMs =
            DorisSinkConnectorConfig.DORIS_REQUEST_CONNECT_TIMEOUT_MS_DEFAULT;
        this.requestReadTimeoutMs = DorisSinkConnectorConfig.DORIS_REQUEST_READ_TIMEOUT_MS_DEFAULT;
        if (config.containsKey(DorisSinkConnectorConfig.AUTO_REDIRECT)) {
            this.autoRedirect =
                Boolean.parseBoolean(config.getString(DorisSinkConnectorConfig.AUTO_REDIRECT));
        }
        if (config.containsKey(DorisSinkConnectorConfig.REQUEST_CONNECT_TIMEOUT_MS)) {
            this.requestConnectTimeoutMs =
                Integer.parseInt(
                    config.getString(DorisSinkConnectorConfig.REQUEST_CONNECT_TIMEOUT_MS));
        }
        if (config.containsKey(DorisSinkConnectorConfig.REQUEST_READ_TIMEOUT_MS)) {
            this.requestReadTimeoutMs =
                Integer.parseInt(config.getString(DorisSinkConnectorConfig.REQUEST_READ_TIMEOUT_MS));
        }
        if (config.containsKey(DorisSinkConnectorConfig.DORIS_CUSTOM_CLUSTER)) {
            this.customCluster = Boolean.parseBoolean(config.getString(DorisSinkConnectorConfig.DORIS_CUSTOM_CLUSTER));
            parseClusterProxyConfig(config);
        }
        this.streamLoadProp = getStreamLoadPropFromConfig(config);
        this.enableGroupCommit =
            ConfigCheckUtils.validateGroupCommitMode(getStreamLoadProp(), enable2PC());
    }

    private void parseClusterProxyConfig(KeyValue config) {
        if (customCluster) {
            String socks5Endpoint = config.getString(DorisSinkConnectorConfig.SOCKS5_ENDPOINT);
            String socks5UserName = config.getString(DorisSinkConnectorConfig.SOCKS5_USERNAME);
            String socks5Password = config.getString(DorisSinkConnectorConfig.SOCKET5_PASSWORD);
            if (socks5Endpoint == null || socks5UserName == null || socks5Password == null) {
                throw new DorisException(
                    "Currently it is doris custom cluster mode, and socks5Endpoint, socks5UserName, socks5Password need to be provided.");
            }
            this.proxyConfig = new ProxyConfig(socks5Endpoint, socks5UserName, socks5Password);
        }
    }

    private Properties getStreamLoadPropFromConfig(KeyValue config) {
        Properties properties = new Properties();
        properties.putAll(getStreamLoadDefaultValues());
        for (String key : config.keySet()) {
            if (key.startsWith(DorisSinkConnectorConfig.STREAM_LOAD_PROP_PREFIX)) {
                String subKey = key.substring(DorisSinkConnectorConfig.STREAM_LOAD_PROP_PREFIX.length());
                properties.put(subKey, config.getString(key));
            }
        }
        return properties;
    }

    private Properties getStreamLoadDefaultValues() {
        Properties properties = new Properties();
        properties.setProperty("format", "json");
        properties.setProperty("read_json_by_line", "true");
        return properties;
    }

    public String getName() {
        return name;
    }

    public String getUrls() {
        return urls;
    }

    public int getQueryPort() {
        return queryPort;
    }

    public int getHttpPort() {
        return httpPort;
    }

    public String getUser() {
        return user;
    }

    public String getPassword() {
        return password;
    }

    public String getDatabase() {
        return database;
    }

    public int getFileSize() {
        return fileSize;
    }

    public int getRecordNum() {
        return recordNum;
    }

    public long getFlushTime() {
        return flushTime;
    }

    public String getTopicMapTable(String topic) {
        return topicMap.get(topic);
    }

    public boolean enableGroupCommit() {
        return enableGroupCommit;
    }

    public boolean enable2PC() {
        return enable2PC;
    }

    public Map<String, String> getTopicMap() {
        return topicMap;
    }

    public String getQueryUrl() {
        List<String> queryUrls = getQueryUrls();
        return queryUrls.get(0);
    }

    public String getHttpUrl() {
        List<String> httpUrls = getHttpUrls();
        return httpUrls.get(0);
    }

    public List<String> getQueryUrls() {
        List<String> queryUrls = new ArrayList<>();
        if (urls.contains(",")) {
            queryUrls =
                Arrays.stream(urls.split(","))
                    .map(
                        url -> {
                            return url.trim() + ":" + queryPort;
                        })
                    .collect(Collectors.toList());
            Collections.shuffle(queryUrls);
            return queryUrls;
        }
        queryUrls.add(urls + ":" + queryPort);
        return queryUrls;
    }

    public List<String> getHttpUrls() {
        List<String> httpUrls = new ArrayList<>();
        if (urls.contains(",")) {
            httpUrls =
                Arrays.stream(urls.split(","))
                    .map(
                        url -> {
                            return url.trim() + ":" + httpPort;
                        })
                    .collect(Collectors.toList());
            Collections.shuffle(httpUrls);
            return httpUrls;
        }
        httpUrls.add(urls + ":" + httpPort);
        return httpUrls;
    }

    public Integer getRequestReadTimeoutMs() {
        return this.requestReadTimeoutMs;
    }

    public Integer getRequestConnectTimeoutMs() {
        return this.requestConnectTimeoutMs;
    }

    public Properties getStreamLoadProp() {
        return streamLoadProp;
    }

    public boolean isEnableCustomJMX() {
        return enableCustomJMX;
    }

    public int getTaskId() {
        return taskId;
    }

    public LoadModel getLoadModel() {
        return loadModel;
    }

    public DeliveryGuarantee getDeliveryGuarantee() {
        return this.deliveryGuarantee;
    }

    public ConverterMode getConverterMode() {
        return this.converterMode;
    }

    public SchemaEvolutionMode getSchemaEvolutionMode() {
        return this.schemaEvolutionMode;
    }

    public boolean isAutoRedirect() {
        return autoRedirect;
    }

    public String getDatabaseTimeZone() {
        return databaseTimeZone;
    }

    public boolean isEnableDelete() {
        return enableDelete;
    }

    public boolean customCluster() {
        return customCluster;
    }

    public Optional<ProxyConfig> getProxyConfig() {
        return Optional.ofNullable(proxyConfig);
    }

    /**
     * parse topic to table map
     *
     * @param config connector config file
     * @return result map
     */
    private Map<String, String> getTopicToTableMap(KeyValue config) {
        if (config.containsKey(DorisSinkConnectorConfig.TOPICS_TABLES_MAP)) {
            Map<String, String> result =
                ConfigCheckUtils.parseTopicToTableMap(
                    config.getString(DorisSinkConnectorConfig.TOPICS_TABLES_MAP));
            if (result != null) {
                return result;
            }
            LOG.error("Invalid Input, Topic2Table Map disabled");
        }
        return new HashMap<>();
    }

    public class ProxyConfig {
        private final String socks5Endpoint;
        private final String socks5UserName;
        private final String socks5Password;
        private final String socks5Host;
        private final Integer socks5Port;

        public ProxyConfig(String socks5Endpoint, String socks5UserName, String socks5Password) {
            this.socks5Endpoint = socks5Endpoint;
            this.socks5UserName = socks5UserName;
            this.socks5Password = socks5Password;
            String[] splitEndpoint = socks5Endpoint.split(":");
            socks5Host = splitEndpoint[0];
            socks5Port = Integer.parseInt(splitEndpoint[1]);
            assert socks5Host != null;
        }

        public String getSocks5Endpoint() {
            return socks5Endpoint;
        }

        public String getSocks5Host() {
            return socks5Host;
        }

        public String getSocks5Password() {
            return socks5Password;
        }

        public Integer getSocks5Port() {
            return socks5Port;
        }

        public String getSocks5UserName() {
            return socks5UserName;
        }
    }

}
