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

package org.apache.rocketmq.connect.doris.utils;

import io.openmessaging.KeyValue;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Pattern;
import org.apache.rocketmq.connect.doris.cfg.DorisSinkConnectorConfig;
import org.apache.rocketmq.connect.doris.converter.ConverterMode;
import org.apache.rocketmq.connect.doris.converter.schema.SchemaEvolutionMode;
import org.apache.rocketmq.connect.doris.exception.ArgumentsException;
import org.apache.rocketmq.connect.doris.exception.DorisException;
import org.apache.rocketmq.connect.doris.writer.DeliveryGuarantee;
import org.apache.rocketmq.connect.doris.writer.load.LoadModel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConfigCheckUtils {
    private static final Logger LOG = LoggerFactory.getLogger(ConfigCheckUtils.class);

    // task id
    public static final String TASK_ID = "task_id";

    /**
     * Validate input configuration
     *
     * @param config configuration Map
     * @return connector name
     */
    public static String validateConfig(KeyValue config) {
        LOG.info("start validate connector config");
        boolean configIsValid = true; // verify all config

        // unique name of this connector instance
        String connectorName = config.getString(DorisSinkConnectorConfig.NAME);
        if (Objects.isNull(connectorName) || connectorName.isEmpty() || !isValidDorisApplicationName(connectorName)) {
            LOG.error(
                "{} is empty or invalid. It should match doris object identifier syntax. Please see "
                    + "the documentation.",
                DorisSinkConnectorConfig.NAME);
            configIsValid = false;
        }

        String topics = config.getString(DorisSinkConnectorConfig.TOPICS);
        String topicsRegex = config.getString(DorisSinkConnectorConfig.TOPICS_REGEX);
        if (topics.isEmpty() && topicsRegex.isEmpty()) {
            LOG.error(
                "{} or {} cannot be empty.",
                DorisSinkConnectorConfig.TOPICS,
                DorisSinkConnectorConfig.TOPICS_REGEX);
            configIsValid = false;
        }

        if (!topics.isEmpty() && !topicsRegex.isEmpty()) {
            LOG.error(
                "{} and {} cannot be set at the same time.",
                DorisSinkConnectorConfig.TOPICS,
                DorisSinkConnectorConfig.TOPICS_REGEX);
            configIsValid = false;
        }

        if (config.containsKey(DorisSinkConnectorConfig.TOPICS_TABLES_MAP)
            && parseTopicToTableMap(config.getString(DorisSinkConnectorConfig.TOPICS_TABLES_MAP))
            == null) {
            LOG.error("{} is empty or invalid.", DorisSinkConnectorConfig.TOPICS_TABLES_MAP);
            configIsValid = false;
        }

        String dorisUrls = config.getString(DorisSinkConnectorConfig.DORIS_URLS);
        if (dorisUrls.isEmpty()) {
            LOG.error("{} cannot be empty.", DorisSinkConnectorConfig.DORIS_URLS);
            configIsValid = false;
        }

        String queryPort = config.getString(DorisSinkConnectorConfig.DORIS_QUERY_PORT);
        if (queryPort.isEmpty()) {
            LOG.error("{} cannot be empty.", DorisSinkConnectorConfig.DORIS_QUERY_PORT);
            configIsValid = false;
        }

        String httpPort = config.getString(DorisSinkConnectorConfig.DORIS_HTTP_PORT);
        if (httpPort.isEmpty()) {
            LOG.error("{} cannot be empty.", DorisSinkConnectorConfig.DORIS_HTTP_PORT);
            configIsValid = false;
        }

        String dorisUser = config.getString(DorisSinkConnectorConfig.DORIS_USER);
        if (dorisUser.isEmpty()) {
            LOG.error("{} cannot be empty.", DorisSinkConnectorConfig.DORIS_USER);
            configIsValid = false;
        }

        String autoDirect = config.getString(DorisSinkConnectorConfig.AUTO_REDIRECT);
        if (!autoDirect.isEmpty()
            && !("true".equalsIgnoreCase(autoDirect) || "false".equalsIgnoreCase(autoDirect))) {
            LOG.error("autoDirect non-boolean type, {}", autoDirect);
            configIsValid = false;
        }

        String bufferCountRecords = config.getString(DorisSinkConnectorConfig.BUFFER_COUNT_RECORDS);
        if (!isNumeric(bufferCountRecords)) {
            LOG.error(
                "{} cannot be empty or not a number.",
                DorisSinkConnectorConfig.BUFFER_COUNT_RECORDS);
            configIsValid = false;
        }

        String bufferSizeBytes = config.getString(DorisSinkConnectorConfig.BUFFER_SIZE_BYTES);
        if (!isNumeric(bufferSizeBytes)
            || isIllegalRange(
            bufferSizeBytes, DorisSinkConnectorConfig.BUFFER_SIZE_BYTES_MIN)) {
            LOG.error(
                "{} cannot be empty or not a number or less than 1.",
                DorisSinkConnectorConfig.BUFFER_SIZE_BYTES);
            configIsValid = false;
        }

        String bufferFlushTime = config.getString(DorisSinkConnectorConfig.BUFFER_FLUSH_TIME_SEC);
        if (!isNumeric(bufferFlushTime)
            || isIllegalRange(
            bufferFlushTime, DorisSinkConnectorConfig.BUFFER_FLUSH_TIME_SEC_MIN)) {
            LOG.error(
                "{} cannot be empty or not a number or less than 10.",
                DorisSinkConnectorConfig.BUFFER_FLUSH_TIME_SEC);
            configIsValid = false;
        }

        String loadModel = config.getString(DorisSinkConnectorConfig.LOAD_MODEL);
        if (!validateEnumInstances(loadModel, LoadModel.instances())) {
            LOG.error(
                "The value of {} is an illegal parameter of {}.",
                loadModel,
                DorisSinkConnectorConfig.LOAD_MODEL);
            configIsValid = false;
        }

        String deliveryGuarantee = config.getString(DorisSinkConnectorConfig.DELIVERY_GUARANTEE);
        if (!validateEnumInstances(deliveryGuarantee, DeliveryGuarantee.instances())) {
            LOG.error(
                "The value of {} is an illegal parameter of {}.",
                loadModel,
                DorisSinkConnectorConfig.DELIVERY_GUARANTEE);
            configIsValid = false;
        }

        String converterMode = config.getString(DorisSinkConnectorConfig.CONVERTER_MODE);
        if (!validateEnumInstances(converterMode, ConverterMode.instances())) {
            LOG.error(
                "The value of {} is an illegal parameter of {}.",
                loadModel,
                DorisSinkConnectorConfig.CONVERTER_MODE);
            configIsValid = false;
        }

        String schemaEvolutionMode = config.getString(DorisSinkConnectorConfig.DEBEZIUM_SCHEMA_EVOLUTION);
        if (!validateEnumInstances(schemaEvolutionMode, SchemaEvolutionMode.instances())) {
            LOG.error(
                "The value of {} is an illegal parameter of {}.",
                loadModel,
                DorisSinkConnectorConfig.DEBEZIUM_SCHEMA_EVOLUTION);
            configIsValid = false;
        }

        if (!configIsValid) {
            throw new DorisException(
                "input kafka connector configuration is null, missing required values, or wrong input value");
        }

        return connectorName;
    }

    /**
     * validates that given name is a valid doris application name, support '-'
     *
     * @param appName doris application name
     * @return true if given application name is valid
     */
    public static boolean isValidDorisApplicationName(String appName) {
        return appName.matches("([a-zA-Z0-9_\\-]+)");
    }

    /**
     * verify topic name, and generate valid table name
     *
     * @param topic       input topic name
     * @param topic2table topic to table map
     * @return valid table name
     */
    public static String tableName(String topic, Map<String, String> topic2table) {
        return generateValidName(topic, topic2table);
    }

    /**
     * verify topic name, and generate valid table/application name
     *
     * @param topic       input topic name
     * @param topic2table topic to table map
     * @return valid table/application name
     */
    public static String generateValidName(String topic, Map<String, String> topic2table) {
        if (topic == null || topic.isEmpty()) {
            throw new DorisException("Topic name is empty String or null");
        }
        if (topic2table.containsKey(topic)) {
            return topic2table.get(topic);
        }
        if (isValidTableIdentifier(topic)) {
            return topic;
        }
        // debezium topic default regex name.db.tbl
        if (topic.contains(".")) {
            String[] split = topic.split("\\.");
            return split[split.length - 1];
        }

        throw new ArgumentsException("Failed get table name from topic");
    }

    public static Map<String, String> parseTopicToTableMap(String input) {
        Map<String, String> topic2Table = new HashMap<>();
        boolean isInvalid = false;
        for (String str : input.split(",")) {
            String[] tt = str.split(":");

            if (tt.length != 2 || tt[0].trim().isEmpty() || tt[1].trim().isEmpty()) {
                LOG.error(
                    "Invalid {} config format: {}",
                    DorisSinkConnectorConfig.TOPICS_TABLES_MAP,
                    input);
                return null;
            }

            String topic = tt[0].trim();
            String table = tt[1].trim();

            if (table.isEmpty()) {
                LOG.error("tableName is empty");
                isInvalid = true;
            }

            if (topic2Table.containsKey(topic)) {
                LOG.error("topic name {} is duplicated", topic);
                isInvalid = true;
            }

            topic2Table.put(tt[0].trim(), tt[1].trim());
        }
        if (isInvalid) {
            throw new DorisException("Failed to parse topic2table map");
        }
        return topic2Table;
    }

    private static boolean isNumeric(String str) {
        if (str != null && !str.isEmpty()) {
            Pattern pattern = Pattern.compile("[0-9]*");
            return pattern.matcher(str).matches();
        }
        return false;
    }

    private static boolean isIllegalRange(String flushTime, long minValue) {
        long time = Long.parseLong(flushTime);
        return time < minValue;
    }

    /**
     * validates that table name is a valid table identifier
     */
    private static boolean isValidTableIdentifier(String tblName) {
        return tblName.matches("^[a-zA-Z][a-zA-Z0-9_]*$");
    }

    private static boolean validateEnumInstances(String value, String[] instances) {
        for (String instance : instances) {
            if (instance.equalsIgnoreCase(value)) {
                return true;
            }
        }
        return false;
    }
}
