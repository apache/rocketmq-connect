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
import io.openmessaging.internal.DefaultKeyValue;
import java.util.Locale;
import org.apache.rocketmq.connect.doris.DorisSinkConnector;
import org.apache.rocketmq.connect.doris.converter.ConverterMode;
import org.apache.rocketmq.connect.doris.converter.schema.SchemaEvolutionMode;
import org.apache.rocketmq.connect.doris.writer.DeliveryGuarantee;
import org.apache.rocketmq.connect.doris.writer.load.LoadModel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Doris SinkConnectorConfig
 */
public class DorisSinkConnectorConfig {
    private static final Logger LOG = LoggerFactory.getLogger(DorisSinkConnector.class);

    public static final String NAME = "name";
    public static final String TOPICS = "topics";
    public static final String TOPICS_REGEX = "topics.regex";

    // Connector config
    public static final String BUFFER_COUNT_RECORDS = "buffer.count.records";
    public static final long BUFFER_COUNT_RECORDS_DEFAULT = 10000;
    public static final String BUFFER_SIZE_BYTES = "buffer.size.bytes";
    public static final long BUFFER_SIZE_BYTES_DEFAULT = 5000000;
    public static final long BUFFER_SIZE_BYTES_MIN = 1;
    public static final String TOPICS_TABLES_MAP = "doris.topic2table.map";
    public static final String LABEL_PREFIX = "label.prefix";

    // Time in seconds
    public static final long BUFFER_FLUSH_TIME_SEC_MIN = 10;
    public static final long BUFFER_FLUSH_TIME_SEC_DEFAULT = 120;
    public static final String BUFFER_FLUSH_TIME_SEC = "buffer.flush.time";

    private static final String DORIS_INFO = "Doris Info";

    // doris config
    public static final String DORIS_URLS = "doris.urls";
    public static final String DORIS_QUERY_PORT = "doris.query.port";
    public static final String DORIS_HTTP_PORT = "doris.http.port";
    public static final String DORIS_USER = "doris.user";
    public static final String DORIS_PASSWORD = "doris.password";
    public static final String DORIS_DATABASE = "doris.database";
    public static final String REQUEST_READ_TIMEOUT_MS = "request.read.timeout.ms";
    public static final String REQUEST_CONNECT_TIMEOUT_MS = "request.connect.timeout.ms";
    public static final Integer DORIS_REQUEST_READ_TIMEOUT_MS_DEFAULT = 30 * 1000;
    public static final Integer DORIS_REQUEST_CONNECT_TIMEOUT_MS_DEFAULT = 30 * 1000;
    public static final String DATABASE_TIME_ZONE = "database.time_zone";
    public static final String DATABASE_TIME_ZONE_DEFAULT = "UTC";
    public static final String LOAD_MODEL = "load.model";
    public static final String LOAD_MODEL_DEFAULT = LoadModel.STREAM_LOAD.name();
    public static final String AUTO_REDIRECT = "auto.redirect";
    public static final String DELIVERY_GUARANTEE = "delivery.guarantee";
    public static final String DELIVERY_GUARANTEE_DEFAULT = DeliveryGuarantee.AT_LEAST_ONCE.name();
    public static final String CONVERTER_MODE = "converter.mode";
    public static final String CONVERT_MODE_DEFAULT = ConverterMode.NORMAL.getName();

    // Prefix for Doris StreamLoad specific properties.
    public static final String STREAM_LOAD_PROP_PREFIX = "sink.properties.";
    public static final String DEBEZIUM_SCHEMA_EVOLUTION = "debezium.schema.evolution";
    public static final String DEBEZIUM_SCHEMA_EVOLUTION_DEFAULT =
        SchemaEvolutionMode.NONE.getName();

    // custom cluster config
    public static final String DORIS_CUSTOM_CLUSTER = "doris.custom.cluster";
    public static final String DORIS_CUSTOM_CLUSTER_DEFAULT = "false";
    public static final String SOCKS5_ENDPOINT = "socks5Endpoint";
    public static final String SOCKS5_USERNAME = "socks5UserName";
    public static final String SOCKET5_PASSWORD = "socks5Password";

    // metrics
    public static final String JMX_OPT = "jmx";
    public static final boolean JMX_OPT_DEFAULT = true;

    public static final String ENABLE_DELETE = "enable.delete";
    public static final boolean ENABLE_DELETE_DEFAULT = false;
    public static final String ENABLE_2PC = "enable.2pc";
    public static final boolean ENABLE_2PC_DEFAULT = true;

    public static void setDefaultValues(KeyValue config) {
        setFieldToDefaultValues(
            config, BUFFER_COUNT_RECORDS, String.valueOf(BUFFER_COUNT_RECORDS_DEFAULT));
        setFieldToDefaultValues(
            config, BUFFER_SIZE_BYTES, String.valueOf(BUFFER_SIZE_BYTES_DEFAULT));
        setFieldToDefaultValues(
            config, BUFFER_FLUSH_TIME_SEC, String.valueOf(BUFFER_FLUSH_TIME_SEC_DEFAULT));
        setFieldToDefaultValues(config, DATABASE_TIME_ZONE, DATABASE_TIME_ZONE_DEFAULT);
        setFieldToDefaultValues(config, LOAD_MODEL, LOAD_MODEL_DEFAULT);
        setFieldToDefaultValues(config, DELIVERY_GUARANTEE, DELIVERY_GUARANTEE_DEFAULT);
        setFieldToDefaultValues(config, CONVERTER_MODE, CONVERT_MODE_DEFAULT);
        setFieldToDefaultValues(
            config, DEBEZIUM_SCHEMA_EVOLUTION, DEBEZIUM_SCHEMA_EVOLUTION_DEFAULT);
        setFieldToDefaultValues(config, JMX_OPT, String.valueOf(JMX_OPT_DEFAULT));
        setFieldToDefaultValues(config, DORIS_CUSTOM_CLUSTER, DORIS_CUSTOM_CLUSTER_DEFAULT);
    }

    private static void setFieldToDefaultValues(KeyValue config, String field, String value) {
        if (!config.containsKey(field)) {
            config.put(field, value);
            LOG.info("Set the default value of {} to {}", field, value);
        }
    }

    public static KeyValue convertToLowercase(KeyValue config) {
        KeyValue newConfig = new DefaultKeyValue();
        for (String key : config.keySet()) {
            String value = config.getString(key);
            newConfig.put(key.toLowerCase(Locale.ROOT), value);
        }
        return newConfig;
    }
}
