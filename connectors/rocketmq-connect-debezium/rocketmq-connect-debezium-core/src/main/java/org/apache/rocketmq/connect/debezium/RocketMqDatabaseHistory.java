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
import io.debezium.config.Field;
import io.debezium.document.DocumentReader;
import io.debezium.relational.history.AbstractDatabaseHistory;
import io.debezium.relational.history.DatabaseHistory;
import io.debezium.relational.history.DatabaseHistoryException;
import io.debezium.relational.history.DatabaseHistoryListener;
import io.debezium.relational.history.HistoryRecord;
import io.debezium.relational.history.HistoryRecordComparator;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.rocketmq.client.consumer.DefaultLitePullConsumer;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.admin.TopicOffset;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.connect.kafka.connect.adaptor.task.AbstractKafkaConnectSource;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.Consumer;

/**
 * rocketmq database history
 */
public final class RocketMqDatabaseHistory extends AbstractDatabaseHistory {

    public static final Field TOPIC = Field.create(CONFIGURATION_FIELD_PREFIX_STRING + "rocketmq.topic")
            .withDisplayName("Database history topic name")
            .withType(ConfigDef.Type.STRING)
            .withWidth(ConfigDef.Width.LONG)
            .withImportance(ConfigDef.Importance.HIGH)
            .withDescription("The name of the topic for the database schema history")
            .withValidation(Field::isRequired);
    /**
     * rocketmq name srv addr
     */
    public static final Field NAME_SRV_ADDR = Field.create(CONFIGURATION_FIELD_PREFIX_STRING + "name.srv.addr")
            .withDisplayName("Rocketmq name srv addr")
            .withType(ConfigDef.Type.STRING)
            .withWidth(ConfigDef.Width.LONG)
            .withImportance(ConfigDef.Importance.HIGH)
            .withDescription("Rocketmq name srv addr")
            .withValidation(Field::isRequired);
    public static final Field ROCKETMQ_ACL_ENABLE = Field.create(CONFIGURATION_FIELD_PREFIX_STRING + "acl.enabled")
            .withDisplayName("Rocketmq acl enabled")
            .withType(ConfigDef.Type.BOOLEAN)
            .withDefault(false)
            .withWidth(ConfigDef.Width.LONG)
            .withImportance(ConfigDef.Importance.HIGH)
            .withDescription("Rocketmq acl enabled");
    public static final Field ROCKETMQ_ACCESS_KEY = Field.create(CONFIGURATION_FIELD_PREFIX_STRING + "access.key")
            .withDisplayName("Rocketmq access key")
            .withType(ConfigDef.Type.STRING)
            .withWidth(ConfigDef.Width.LONG)
            .withImportance(ConfigDef.Importance.HIGH)
            .withDescription("Rocketmq access key");
    public static final Field ROCKETMQ_SECRET_KEY = Field.create(CONFIGURATION_FIELD_PREFIX_STRING + "secret.key")
            .withDisplayName("Rocketmq secret key")
            .withType(ConfigDef.Type.STRING)
            .withWidth(ConfigDef.Width.LONG)
            .withImportance(ConfigDef.Importance.HIGH)
            .withDescription("Rocketmq secret key");

    public static final Field RECOVERY_POLL_ATTEMPTS = Field.create(CONFIGURATION_FIELD_PREFIX_STRING + "recovery.attempts")
            .withDisplayName("Max attempts to recovery database schema history")
            .withType(ConfigDef.Type.INT)
            .withGroup(Field.createGroupEntry(Field.Group.ADVANCED, 0))
            .withWidth(ConfigDef.Width.SHORT)
            .withImportance(ConfigDef.Importance.LOW)
            .withDescription("The number of attempts in a row that no data are returned from RocketMQ before recover " +
                    "completes. "
                    + "The maximum amount of time to wait after receiving no data is (recovery.attempts) x (recovery.poll.interval.ms).")
            .withDefault(60)
            .withValidation(Field::isInteger);

    public static final Field RECOVERY_POLL_INTERVAL_MS = Field.create(CONFIGURATION_FIELD_PREFIX_STRING
                    + "recovery.poll.interval.ms")
            .withDisplayName("Poll interval during database schema history recovery (ms)")
            .withType(ConfigDef.Type.INT)
            .withGroup(Field.createGroupEntry(Field.Group.ADVANCED, 1))
            .withWidth(ConfigDef.Width.SHORT)
            .withImportance(ConfigDef.Importance.LOW)
            .withDescription("The number of milliseconds to wait while polling for persisted data during recovery.")
            .withDefault(1000)
            .withValidation(Field::isLong);

    private static final Logger log = LoggerFactory.getLogger(AbstractKafkaConnectSource.class);
    private static final int MESSAGE_QUEUE = 0;
    private static final int UNLIMITED_VALUE = -1;

    private final DocumentReader reader = DocumentReader.defaultReader();
    private String topicName;
    private String dbHistoryName;
    private RocketMqConfig rocketMqConfig;
    private DefaultMQProducer producer;
    private int maxRecoveryAttempts;
    private Long pollInterval;

    @Override
    public void configure(
            Configuration config,
            HistoryRecordComparator comparator,
            DatabaseHistoryListener listener,
            boolean useCatalogBeforeSchema) {
        super.configure(config, comparator, listener, useCatalogBeforeSchema);
        this.topicName = config.getString(TOPIC);
        this.dbHistoryName = config.getString(DatabaseHistory.NAME, UUID.randomUUID().toString());
        this.maxRecoveryAttempts = config.getInteger(RECOVERY_POLL_ATTEMPTS);
        this.pollInterval = config.getLong(RECOVERY_POLL_INTERVAL_MS);
        log.info("Configure to store the debezium database history {} to rocketmq topic {}",
                dbHistoryName, topicName);
        // build config
        this.rocketMqConfig = RocketMqConfig.newBuilder()
                .aclEnable(config.getBoolean(RocketMqDatabaseHistory.ROCKETMQ_ACL_ENABLE))
                .accessKey(config.getString(RocketMqDatabaseHistory.ROCKETMQ_ACCESS_KEY))
                .secretKey(config.getString(RocketMqDatabaseHistory.ROCKETMQ_SECRET_KEY))
                .namesrvAddr(config.getString(RocketMqDatabaseHistory.NAME_SRV_ADDR))
                .groupId(dbHistoryName)
                .build();
    }

    @Override
    public void initializeStorage() {
        super.initializeStorage();
        log.info("try to create history topic: {}!", this.topicName);
        TopicConfig topicConfig = new TopicConfig(this.topicName, 1, 1, 6);
        RocketMqAdminUtil.createTopic(rocketMqConfig, topicConfig);
    }

    @Override
    public void start() {
        super.start();
        try {
            // Check and create group
            Set<String> consumerGroupSet = RocketMqAdminUtil.fetchAllConsumerGroup(rocketMqConfig);
            if (!consumerGroupSet.contains(rocketMqConfig.getGroupId())) {
                RocketMqAdminUtil.createSubGroup(rocketMqConfig, rocketMqConfig.getGroupId());
            }
            // Start rocketmq producer
            this.producer = RocketMqAdminUtil.initDefaultMQProducer(rocketMqConfig);
            this.producer.start();
        } catch (MQClientException e) {
            throw new DatabaseHistoryException(e);
        }
    }

    @Override
    public void stop() {
        try {
            if (this.producer != null) {
                producer.shutdown();
                this.producer = null;
            }
        } catch (Exception pe) {
            log.warn("Failed to closing rocketmq client", pe);
        }
    }

    @Override
    protected void storeRecord(HistoryRecord record) throws DatabaseHistoryException {
        if (this.producer == null) {
            throw new IllegalStateException("No producer is available. Ensure that 'initializeStorage()'"
                    + " is called before storing database schema history records.");
        }

        log.trace("Storing record into database schema history: {}", record);
        try {
            Message message = new Message(this.topicName, record.toString().getBytes());
            producer.send(message, new ZeroMessageQueueSelector(), null, new SendCallback() {
                @Override
                public void onSuccess(SendResult sendResult) {
                    log.debug("Stored record in topic '{}' partition {} at offset {} ",
                            message.getTopic(), sendResult.getMessageQueue(), sendResult.getMessageQueue());
                }

                @Override
                public void onException(Throwable e) {
                    log.error("Store record into database schema history failed : {}", e);
                }
            });
        } catch (InterruptedException e) {
            log.error("Interrupted before record was written into database schema history: {}", record);
            Thread.currentThread().interrupt();
            throw new DatabaseHistoryException(e);
        } catch (MQClientException | RemotingException e) {
            throw new DatabaseHistoryException(e);
        }
    }

    /**
     * Recover records
     *
     * @param records
     */
    @Override
    protected void recoverRecords(Consumer<HistoryRecord> records) {
        DefaultLitePullConsumer consumer = null;
        try {
            consumer = RocketMqAdminUtil.initDefaultLitePullConsumer(rocketMqConfig, false);
            consumer.start();

            // Select message queue
            MessageQueue messageQueue = new ZeroMessageQueueSelector().select(new ArrayList<>(consumer.fetchMessageQueues(topicName)), null, null);
            consumer.assign(Collections.singleton(messageQueue));
            consumer.seekToBegin(messageQueue);
            // Read all messages in the topic ...
            long lastProcessedOffset = UNLIMITED_VALUE;
            Long maxOffset = null;
            int recoveryAttempts = 0;

            do {
                if (recoveryAttempts > maxRecoveryAttempts) {
                    throw new IllegalStateException(
                            "The database schema history couldn't be recovered.");
                }
                // Get db schema history topic end offset
                maxOffset = getMaxOffsetOfSchemaHistoryTopic(maxOffset, messageQueue);
                log.debug("End offset of database schema history topic is {}", maxOffset);

                // Poll record from db schema history topic
                List<MessageExt> recoveredRecords = consumer.poll(pollInterval);
                int numRecordsProcessed = 0;

                for (MessageExt message : recoveredRecords) {
                    if (message.getQueueOffset() > lastProcessedOffset) {
                        HistoryRecord recordObj = new HistoryRecord(reader.read(message.getBody()));
                        log.trace("Recovering database history: {}", recordObj);
                        if (recordObj == null || !recordObj.isValid()) {
                            log.warn("Skipping invalid database history record '{}'. " +
                                            "This is often not an issue, but if it happens repeatedly please check the '{}' topic.",
                                    recordObj, topicName);
                        } else {
                            records.accept(recordObj);
                            log.trace("Recovered database history: {}", recordObj);
                        }
                        lastProcessedOffset = message.getQueueOffset();
                        ++numRecordsProcessed;
                    }
                }
                if (numRecordsProcessed == 0) {
                    log.debug("No new records found in the database schema history; will retry");
                    recoveryAttempts++;
                } else {
                    log.debug("Processed {} records from database schema history", numRecordsProcessed);
                }

            } while (lastProcessedOffset < maxOffset - 1);

        } catch (MQClientException | MQBrokerException | IOException | RemotingException | InterruptedException e) {
            throw new DatabaseHistoryException(e);
        } finally {
            if (consumer != null) {
                consumer.shutdown();
            }
        }
    }

    private Long getMaxOffsetOfSchemaHistoryTopic(Long previousEndOffset, MessageQueue messageQueue) throws MQBrokerException, RemotingException, InterruptedException, MQClientException {
        Map<MessageQueue, TopicOffset> minAndMaxOffsets = RocketMqAdminUtil.offsets(this.rocketMqConfig, topicName);
        Long maxOffset = minAndMaxOffsets.get(messageQueue).getMaxOffset();
        if (previousEndOffset != null && !previousEndOffset.equals(maxOffset)) {
            log.warn("Detected changed end offset of database schema history topic (previous: "
                    + previousEndOffset + ", current: " + maxOffset
                    + "). Make sure that the same history topic isn't shared by multiple connector instances.");
        }
        return maxOffset;
    }

    @Override
    public boolean exists() {
        boolean exists = false;
        if (this.storageExists()) {
            Map<MessageQueue, TopicOffset> minAndMaxOffset = RocketMqAdminUtil.offsets(this.rocketMqConfig,
                    topicName);
            for (MessageQueue messageQueue : minAndMaxOffset.keySet()) {
                if (MESSAGE_QUEUE == messageQueue.getQueueId()) {
                    exists =
                            minAndMaxOffset.get(messageQueue).getMaxOffset() > minAndMaxOffset.get(messageQueue).getMinOffset();
                }
            }
        }
        return exists;
    }

    @Override
    public boolean storageExists() {
        // Check whether topic exists
        return RocketMqAdminUtil.topicExist(rocketMqConfig, this.topicName);
    }

    @Override
    public String toString() {
        if (topicName != null) {
            return "Rocketmq topic (" + topicName + ")";
        }
        return "Rocketmq topic";
    }
}
