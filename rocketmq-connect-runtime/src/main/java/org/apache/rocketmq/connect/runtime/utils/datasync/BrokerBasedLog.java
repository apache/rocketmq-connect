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

package org.apache.rocketmq.connect.runtime.utils.datasync;

import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import io.openmessaging.connector.api.errors.ConnectException;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.client.consumer.DefaultLitePullConsumer;
import org.apache.rocketmq.client.consumer.store.ReadOffsetType;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.selector.SelectMessageQueueByHash;
import org.apache.rocketmq.common.admin.TopicOffset;
import org.apache.rocketmq.common.admin.TopicStatsTable;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.connect.runtime.common.LoggerName;
import org.apache.rocketmq.connect.runtime.config.WorkerConfig;
import org.apache.rocketmq.connect.runtime.serialization.Serde;
import org.apache.rocketmq.connect.runtime.utils.Base64Util;
import org.apache.rocketmq.connect.runtime.utils.Callback;
import org.apache.rocketmq.connect.runtime.utils.ConnectUtil;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import static org.apache.rocketmq.connect.runtime.config.ConnectorConfig.MAX_MESSAGE_SIZE;

/**
 * A Broker base data synchronizer, synchronize data between workers.
 *
 * @param <K>
 * @param <V>
 */
public class BrokerBasedLog<K, V> implements DataSynchronizer<K, V> {

    private static final Logger log = LoggerFactory.getLogger(LoggerName.ROCKETMQ_RUNTIME);

    /**
     * A callback to receive data from other workers.
     */
    private DataSynchronizerCallback<K, V> dataSynchronizerCallback;

    /**
     * Producer to send data to broker.
     */
    private DefaultMQProducer producer;

    /**
     * Consumer to receive synchronize data from broker.
     */
    private DefaultLitePullConsumer consumer;

    /**
     * A queue to send or consume message.
     */
    private String topicName;
    /**
     * serializer and deserializer
     */
    private Serde keySerde;
    private Serde valueSerde;

    private WorkerConfig workerConfig;

    private boolean stopRequested;

    private Thread thread;

    private boolean enabledCompactTopic = false;

    private String groupName;

    public BrokerBasedLog(WorkerConfig workerConfig,
                          String topicName,
                          String groupName,
                          DataSynchronizerCallback<K, V> dataSynchronizerCallback,
                          Serde keySerde,
                          Serde valueSerde,
                          boolean enabledCompactTopic) {
        this(workerConfig, topicName, groupName, dataSynchronizerCallback, keySerde, valueSerde);
        this.enabledCompactTopic = enabledCompactTopic;

    }

    public BrokerBasedLog(WorkerConfig workerConfig,
                          String topicName,
                          String groupName,
                          DataSynchronizerCallback<K, V> dataSynchronizerCallback,
                          Serde keySerde,
                          Serde valueSerde) {

        this.topicName = topicName;
        this.keySerde = keySerde;
        this.valueSerde = valueSerde;
        this.workerConfig = workerConfig;
        this.stopRequested = false;
        this.groupName = groupName;

        this.dataSynchronizerCallback = dataSynchronizerCallback;
        // Init producer
        this.producer = ConnectUtil.initDefaultMQProducer(workerConfig);
        this.producer.setProducerGroup(groupName);
        // Init consumer
        this.consumer = ConnectUtil.initDefaultLitePullConsumer(workerConfig, true);
        this.consumer.setConsumerGroup(groupName);
        // prepare config
        this.prepare();
    }

    /**
     * Preparation before startup
     */
    private void prepare() {
        if (workerConfig.isAutoCreateGroupEnable()) {
            ConnectUtil.createSubGroup(workerConfig, consumer.getConsumerGroup());
        }
    }

    @Override
    public void start() {
        try {
            // start producer
            producer.start();
            // start consumer
            consumer.start();
            // Fetch message queues
            Collection<MessageQueue> messageQueues = consumer.fetchMessageQueues(topicName);
            this.consumer.assign(messageQueues);

            DefaultMQAdminExt adminClient = ConnectUtil.startMQAdminTool(workerConfig);
            TopicStatsTable topicStatsTable = adminClient.examineTopicStats(topicName);
            HashMap<MessageQueue, TopicOffset> minAndMaxOffsets = topicStatsTable.getOffsetTable();
            for (MessageQueue messageQueue : messageQueues){
                if (enabledCompactTopic){
                    consumer.seekToBegin(messageQueue);
                    // update message queue first
                    consumer.getOffsetStore().updateOffset(messageQueue,
                            minAndMaxOffsets.get(messageQueue).getMinOffset(), false);
                } else {
                    consumer.seekToEnd(messageQueue);
                }
            }
            // read to log end
            if (enabledCompactTopic) {
                readToLogEnd();
            }
            // start worker thread
            this.thread = new WorkThread();
            this.thread.start();
        } catch (MQClientException | MQBrokerException | RemotingException | InterruptedException e) {
            log.error("Start error.", e);
        }
    }

    /**
     * read to log end
     */
    private void readToLogEnd() throws MQClientException, MQBrokerException, RemotingException, InterruptedException {
        DefaultMQAdminExt adminClient = ConnectUtil.startMQAdminTool(workerConfig);
        TopicStatsTable topicStatsTable = adminClient.examineTopicStats(topicName);
        HashMap<MessageQueue, TopicOffset> minAndMaxOffsets = topicStatsTable.getOffsetTable();
        while (!minAndMaxOffsets.isEmpty()) {
            Iterator<Map.Entry<MessageQueue, TopicOffset>> it = minAndMaxOffsets.entrySet().iterator();
            while (it.hasNext()) {
                Map.Entry<MessageQueue, TopicOffset> offsetEntry = it.next();
                long lastConsumedOffset = this.consumer.getOffsetStore().readOffset(offsetEntry.getKey(),
                        ReadOffsetType.READ_FROM_MEMORY);
                if ((lastConsumedOffset + 1) >= offsetEntry.getValue().getMaxOffset()) {
                    log.trace("Read to end offset {} for {}", offsetEntry.getValue().getMaxOffset(),
                            offsetEntry.getKey().getQueueId());
                    it.remove();
                } else {
                    log.trace("Behind end offset {} for {}; last-read offset is {}",
                            offsetEntry.getValue().getMaxOffset(), offsetEntry.getKey().getQueueId(), lastConsumedOffset);
                    poll(5000);
                    break;
                }
            }
        }
    }

    private void poll(long timeoutMs) {
        List<MessageExt> records = consumer.poll(timeoutMs);
        for (MessageExt message : records) {
            log.info("Received one message: {}, topic is {}", message.getMsgId() + "\n", topicName);
            try {
                String key = message.getKeys();
                Map.Entry<K, V> entry = decode(StringUtils.isEmpty(key) ? null : Base64Util.base64Decode(key), message.getBody());
                dataSynchronizerCallback.onCompletion(null, entry.getKey(), entry.getValue());
                MessageQueue messageQueue = new MessageQueue(message.getTopic(), message.getBrokerName(),message.getQueueId());
                this.consumer.getOffsetStore().updateOffset(messageQueue, message.getQueueOffset(), false);
            } catch (Exception e) {
                log.error("Decode message data error. message: {}, error info: {}", message, e);
            }
        }
    }

    @Override
    public void stop() {
        synchronized (this) {
            stopRequested = true;
        }
        try {
            if (thread != null) {
                thread.join();
            }
        } catch (InterruptedException e) {
            throw new ConnectException("Failed to stop BrokerBasedLog. Exiting without cleanly shutting " +
                    "down it's producer and consumer.", e);
        }
        // shut down
        if (producer != null) {
            producer.shutdown();
        }
        if (consumer != null) {
            consumer.shutdown();
        }
    }

    @Override
    public void send(K key, V value) {
        try {
            Map.Entry<byte[], byte[]> encode = encode(key, value);
            byte[] body = encode.getValue();
            if (body.length > MAX_MESSAGE_SIZE) {
                log.error("Message size is greater than {} bytes, key: {}, value {}", MAX_MESSAGE_SIZE, key, value);
                return;
            }
            String encodeKey = Base64Util.base64Encode(encode.getKey());
            Message message = new Message(topicName, null, encodeKey, body);
            producer.send(message, new SelectMessageQueueByHash(), encodeKey, new SendCallback() {
                @Override
                public void onSuccess(org.apache.rocketmq.client.producer.SendResult result) {
                    log.info("Send async message OK, msgId: {},topic:{}", result.getMsgId(), topicName);
                }

                @Override
                public void onException(Throwable throwable) {
                    if (null != throwable) {
                        log.error("Send async message Failed, error: {}", throwable);
                        // Keep sending until success
                        send(key, value);
                    }
                }
            });
        } catch (Exception e) {
            log.error("BrokerBaseLog send async message Failed.", e);
        }
    }

    /**
     * send data to all workers
     *
     * @param key
     * @param value
     * @param callback
     */
    @Override
    public void send(K key, V value, Callback callback) {
        try {
            Map.Entry<byte[], byte[]> encode = encode(key, value);
            byte[] body = encode.getValue();
            if (body.length > MAX_MESSAGE_SIZE) {
                log.error("Message size is greater than {} bytes, key: {}, value {}", MAX_MESSAGE_SIZE, key, value);
                return;
            }
            String encodeKey = Base64Util.base64Encode(encode.getKey());
            Message message = new Message(topicName, null, encodeKey, body);
            producer.send(message, new SelectMessageQueueByHash(), encodeKey, new SendCallback() {
                @Override
                public void onSuccess(org.apache.rocketmq.client.producer.SendResult result) {
                    log.info("Send async message OK, msgId: {},topic:{}", result.getMsgId(), topicName);
                    callback.onCompletion(null, value);
                }

                @Override
                public void onException(Throwable throwable) {
                    if (null != throwable) {
                        log.error("Send async message Failed, error: {}", throwable);
                        // Keep sending until success
                        send(key, value, callback);
                    }
                }
            });
        } catch (Exception e) {
            log.error("BrokerBaseLog send async message Failed.", e);
        }
    }

    private Map.Entry<byte[], byte[]> encode(K key, V value) {
        byte[] keySer = keySerde.serializer().serialize(topicName, key);
        byte[] valueSer = valueSerde.serializer().serialize(topicName, value);
        return new Map.Entry<byte[], byte[]>() {
            @Override
            public byte[] getKey() {
                return keySer;
            }

            @Override
            public byte[] getValue() {
                return valueSer;
            }

            @Override
            public byte[] setValue(byte[] value) {
                throw new UnsupportedOperationException();
            }
        };
    }

    private Map.Entry<K, V> decode(byte[] key, byte[] value) {
        K deKey = (K) keySerde.deserializer().deserialize(topicName, key);
        V deValue = (V) valueSerde.deserializer().deserialize(topicName, value);
        return new Map.Entry<K, V>() {
            @Override
            public K getKey() {
                return deKey;
            }

            @Override
            public V getValue() {
                return deValue;
            }

            @Override
            public V setValue(V value) {
                throw new UnsupportedOperationException();
            }
        };
    }

    private class WorkThread extends Thread {
        public WorkThread() {
            super("BrokerBasedLog Work Thread - " + topicName);
        }

        @Override
        public void run() {
            try {
                log.trace("{} started execution", this);
                while (true) {
                    synchronized (BrokerBasedLog.this) {
                        if (stopRequested)
                            break;
                    }
                    poll(Integer.MAX_VALUE);
                }
            } catch (Throwable t) {
                log.error("Unexpected exception in {}", this, t);
            }
        }
    }
}
