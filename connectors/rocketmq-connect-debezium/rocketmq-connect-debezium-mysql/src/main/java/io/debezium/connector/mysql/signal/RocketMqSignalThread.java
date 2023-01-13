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

package io.debezium.connector.mysql.signal;

import com.google.common.collect.Lists;
import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.connector.mysql.MySqlReadOnlyIncrementalSnapshotChangeEventSource;
import io.debezium.document.Document;
import io.debezium.document.DocumentReader;
import io.debezium.pipeline.signal.ExecuteSnapshot;
import io.debezium.schema.DataCollectionId;
import io.debezium.util.Threads;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.source.SourceConnector;
import org.apache.rocketmq.client.consumer.DefaultLitePullConsumer;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.connect.debezium.RocketMqAdminUtil;
import org.apache.rocketmq.connect.debezium.RocketMqConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;

/**
 * The class responsible for processing of signals delivered to Debezium via a dedicated Kafka topic.
 * The signal message must have the following structure:
 * <ul>
 * <li>{@code id STRING} - the unique identifier of the signal sent, usually UUID, can be used for deduplication</li>
 * <li>{@code type STRING} - the unique logical name of the code executing the signal</li>
 * <li>{@code data STRING} - the data in JSON format that are passed to the signal code
 * </ul>
 */
public class RocketMqSignalThread<T extends DataCollectionId> {

    public static final String CONFIGURATION_FIELD_PREFIX_STRING = "signal.";
    public static final Field SIGNAL_TOPIC = Field.create(CONFIGURATION_FIELD_PREFIX_STRING + "rocketmq.topic")
            .withDisplayName("Signal topic name")
            .withType(ConfigDef.Type.STRING)
            .withWidth(ConfigDef.Width.LONG)
            .withImportance(ConfigDef.Importance.HIGH)
            .withDescription("The name of the topic for the signals to the connector")
            .withValidation(Field::isRequired);
    public static final Field NAME_SRV_ADDR = Field.create(CONFIGURATION_FIELD_PREFIX_STRING + "name.srv.addr")
            .withDisplayName("Name server addresses")
            .withType(ConfigDef.Type.STRING)
            .withWidth(ConfigDef.Width.LONG)
            .withImportance(ConfigDef.Importance.HIGH)
            .withDescription("Rocketmq name server addresses.")
            .withValidation(Field::isRequired);
    public static final Field ROCKETMQ_ACL_ENABLE = Field.create(CONFIGURATION_FIELD_PREFIX_STRING + "acl.enabled")
            .withDisplayName("Rocketmq acl enabled")
            .withType(ConfigDef.Type.BOOLEAN)
            .withDefault(false)
            .withWidth(ConfigDef.Width.LONG)
            .withImportance(ConfigDef.Importance.HIGH)
            .withDescription("Rocketmq acl enabled");
    public static final Field ROCKETMQ_ACCESS_KEY = Field.create(CONFIGURATION_FIELD_PREFIX_STRING + "access.key")
            .withDisplayName("Rocketmq acl access key")
            .withType(ConfigDef.Type.STRING)
            .withWidth(ConfigDef.Width.LONG)
            .withImportance(ConfigDef.Importance.HIGH)
            .withDescription("Rocketmq acl access key");
    public static final Field ROCKETMQ_SECRET_KEY = Field.create(CONFIGURATION_FIELD_PREFIX_STRING + "secret.key")
            .withDisplayName("Rocketmq acl secret key")
            .withType(ConfigDef.Type.STRING)
            .withWidth(ConfigDef.Width.LONG)
            .withImportance(ConfigDef.Importance.HIGH)
            .withDescription("Rocketmq acl secret key");
    private static final Logger LOGGER = LoggerFactory.getLogger(RocketMqSignalThread.class);
    private final ExecutorService signalTopicListenerExecutor;
    private final String topicName;
    private final String nameSrvAddrs;
    private final boolean aclEnabled;
    private final MessageQueue messageQueue;
    private final String accessKey;
    private final String secretKey;
    private final String connectorName;
    private final RocketMqConfig rocketMqConfig;
    private final MySqlReadOnlyIncrementalSnapshotChangeEventSource<T> eventSource;
    private final DefaultLitePullConsumer signalsConsumer;

    public RocketMqSignalThread(Class<? extends SourceConnector> connectorType, CommonConnectorConfig connectorConfig,
                                MySqlReadOnlyIncrementalSnapshotChangeEventSource<T> eventSource) {
        String signalName = "rocketmq-signal";
        connectorName = connectorConfig.getLogicalName();
        signalTopicListenerExecutor = Threads.newSingleThreadExecutor(connectorType, connectorName, signalName, true);
        Configuration signalConfig = connectorConfig.getConfig().subset(CONFIGURATION_FIELD_PREFIX_STRING, false)
                .edit()
                .withDefault(RocketMqSignalThread.SIGNAL_TOPIC, connectorName + "-signal")
                .build();
        this.eventSource = eventSource;
        this.topicName = signalConfig.getString(SIGNAL_TOPIC);
        this.aclEnabled = signalConfig.getBoolean(ROCKETMQ_ACL_ENABLE);
        this.accessKey = signalConfig.getString(ROCKETMQ_ACCESS_KEY);
        this.secretKey = signalConfig.getString(ROCKETMQ_SECRET_KEY);
        this.nameSrvAddrs = signalConfig.getString(NAME_SRV_ADDR);
        this.rocketMqConfig = RocketMqConfig.newBuilder()
                .groupId(connectorName.concat("-signal-group"))
                .namesrvAddr(this.nameSrvAddrs)
                .aclEnable(this.aclEnabled)
                .accessKey(this.accessKey)
                .secretKey(this.secretKey)
                .build();

        try {
            // init and start
            this.signalsConsumer = initDefaultLitePullConsumer();
            this.signalsConsumer.start();
            List<MessageQueue> messageQueues = new ArrayList<>(this.signalsConsumer.fetchMessageQueues(topicName));
            // only get first message queue
            this.messageQueue = messageQueues.stream().findFirst().get();
            this.signalsConsumer.assign(Lists.newArrayList(this.messageQueue));
            LOGGER.info("Subscribing to signals topic '{}'", topicName);
        } catch (MQClientException e) {
            LOGGER.error("Initialization and assigin failed, {}", e);
            throw new RuntimeException(e);
        }
    }


    private DefaultLitePullConsumer initDefaultLitePullConsumer() throws MQClientException {
        // create topic
        if (!RocketMqAdminUtil.topicExist(this.rocketMqConfig, this.topicName)) {
            // read queue 1, write queue 1, prem 1
            RocketMqAdminUtil.createTopic(this.rocketMqConfig, new TopicConfig(this.topicName, 1, 1, 6));
            LOGGER.info("Create rocketmq signal topic {}", this.topicName);
        }
        String groupName = connectorName.concat("-signal-group");
        Set<String> groupSet = RocketMqAdminUtil.fetchAllConsumerGroup(this.rocketMqConfig);
        if (!groupSet.contains(groupName)) {
            // create consumer group
            RocketMqAdminUtil.createSubGroup(this.rocketMqConfig, rocketMqConfig.getGroupId());
        }
        return RocketMqAdminUtil.initDefaultLitePullConsumer(rocketMqConfig, false);

    }


    public void start() {
        signalTopicListenerExecutor.submit(this::monitorSignals);
    }

    private void monitorSignals() {
        while (true) {
            List<MessageExt> recoveredRecords = signalsConsumer.poll(5000);
            for (MessageExt record : recoveredRecords) {
                try {
                    processSignal(record);
                } catch (final InterruptedException e) {
                    LOGGER.error("Signals processing was interrupted", e);
                    signalsConsumer.shutdown();
                    return;
                } catch (final Exception e) {
                    LOGGER.error("Skipped signal due to an error '{}'", record, e);
                }
            }
        }
    }

    private void processSignal(MessageExt record) throws IOException, InterruptedException {
        if (!connectorName.equals(record.getKeys())) {
            LOGGER.info("Signal key '{}' doesn't match the connector's name '{}'", record.getKeys(), connectorName);
            return;
        }
        // Get value
        String value = new String(record.getBody(), StandardCharsets.UTF_8);
        LOGGER.trace("Processing signal: {}", value);
        final Document jsonData = (value == null || value.isEmpty()) ? Document.create()
                : DocumentReader.defaultReader().read(value);
        String type = jsonData.getString("type");
        Document data = jsonData.getDocument("data");
        if (ExecuteSnapshot.NAME.equals(type)) {
            executeSnapshot(data, record.getQueueOffset());
        } else {
            LOGGER.warn("Unknown signal type {}", type);
        }
    }

    private void executeSnapshot(Document data, long signalOffset) {
        final List<String> dataCollections = ExecuteSnapshot.getDataCollections(data);
        if (dataCollections != null) {
            ExecuteSnapshot.SnapshotType snapshotType = ExecuteSnapshot.getSnapshotType(data);
            LOGGER.info("Requested '{}' snapshot of data collections '{}'", snapshotType, dataCollections);
            if (snapshotType == ExecuteSnapshot.SnapshotType.INCREMENTAL) {
                eventSource.enqueueDataCollectionNamesToSnapshot(dataCollections, signalOffset);
            }
        }
    }

    public void seek(long signalOffset) {
        try {
            signalsConsumer.seek(this.messageQueue, signalOffset + 1);
        } catch (MQClientException e) {
            LOGGER.error("Signals consumer seek failure , error {}", e);
        }
    }
}