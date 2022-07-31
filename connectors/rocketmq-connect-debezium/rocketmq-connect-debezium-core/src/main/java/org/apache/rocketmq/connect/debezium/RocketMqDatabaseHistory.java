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
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.connect.kafka.connect.adaptor.task.AbstractKafkaConnectSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
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
    private static final Logger log = LoggerFactory.getLogger(AbstractKafkaConnectSource.class);
    private final DocumentReader reader = DocumentReader.defaultReader();
    private String topicName;
    private String dbHistoryName;
    private RocketMqConnectConfig connectConfig;
    private DefaultMQProducer producer;



    @Override
    public void configure(
            Configuration config,
            HistoryRecordComparator comparator,
            DatabaseHistoryListener listener,
            boolean useCatalogBeforeSchema) {
        super.configure(config, comparator, listener, useCatalogBeforeSchema);
        this.topicName = config.getString(TOPIC);
        this.dbHistoryName = config.getString(DatabaseHistory.NAME, UUID.randomUUID().toString());
        log.info("Configure to store the debezium database history {} to rocketmq topic {}",
                dbHistoryName, topicName);
        // init config
        connectConfig = new RocketMqConnectConfig(config, dbHistoryName);
    }

    @Override
    public void initializeStorage() {
        super.initializeStorage();
        log.info("try to create history topic: {}!", this.topicName);
        TopicConfig topicConfig = new TopicConfig(this.topicName);
        RocketMQConnectUtil.createTopic(connectConfig, topicConfig);
    }

    @Override
    public void start() {
        super.start();
        try {
            Set<String> consumerGroupSet = RocketMQConnectUtil.fetchAllConsumerGroup(connectConfig);
            if (!consumerGroupSet.contains(connectConfig.getRmqConsumerGroup())) {
                RocketMQConnectUtil.createSubGroup(connectConfig, connectConfig.getRmqConsumerGroup());
            }
            this.producer = RocketMQConnectUtil.initDefaultMQProducer(connectConfig);
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

    /**
     * recover record
     *
     * @param records
     */
    @Override
    protected void recoverRecords(Consumer<HistoryRecord> records) {
        DefaultLitePullConsumer consumer = null;
        try {
            consumer = RocketMQConnectUtil.initDefaultLitePullConsumer(connectConfig, topicName, false);
            consumer.start();
            while (true) {
                List<MessageExt> result = consumer.poll(10000);
                if (result == null || result.isEmpty()) {
                    break;
                }
                for (MessageExt message : result) {
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
                }
            }
        } catch (MQClientException ce) {
            throw new DatabaseHistoryException(ce);
        } catch (IOException e) {
            throw new DatabaseHistoryException(e);
        } finally {
            if (consumer != null) {
                consumer.shutdown();
            }
        }
    }

    @Override
    protected void storeRecord(HistoryRecord record) throws DatabaseHistoryException {
        log.info(record.toString());
        if (this.producer == null) {
            throw new IllegalStateException("No producer is available. Ensure that 'start()'"
                    + " is called before storing database history records.");
        }
        if (log.isTraceEnabled()) {
            log.trace("Storing record into database history: {}", record);
        }
        try {
            Message sourceMessage = new Message();
            sourceMessage.setTopic(this.topicName);
            final byte[] messageBody = record.toString().getBytes();
            sourceMessage.setBody(messageBody);
            producer.send(sourceMessage);
        } catch (Exception e) {
            throw new DatabaseHistoryException(e);
        }
    }


    @Override
    public boolean exists() {
        return this.storageExists();
    }

    @Override
    public boolean storageExists() {
        // check topic is exist
        return RocketMQConnectUtil.topicExist(connectConfig, this.topicName);
    }

    @Override
    public String toString() {
        if (topicName != null) {
            return "Rocketmq topic (" + topicName + ")";
        }
        return "Rocketmq topic";
    }
}
