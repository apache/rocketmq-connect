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

import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.connect.runtime.common.LoggerName;
import org.apache.rocketmq.connect.runtime.config.WorkerConfig;
import org.apache.rocketmq.connect.runtime.serialization.Serde;
import org.apache.rocketmq.connect.runtime.utils.Base64Util;
import org.apache.rocketmq.connect.runtime.utils.Callback;
import org.apache.rocketmq.connect.runtime.utils.ConnectUtil;
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
    private DefaultMQPushConsumer consumer;

    /**
     * A queue to send or consume message.
     */
    private String topicName;
    /**
     * serializer and deserializer
     */
    private Serde keySerde;
    private Serde valueSerde;

    public BrokerBasedLog(WorkerConfig connectConfig,
        String topicName,
        String workId,
        DataSynchronizerCallback<K, V> dataSynchronizerCallback,
        Serde keySerde,
        Serde valueSerde) {

        this.topicName = topicName;
        this.keySerde = keySerde;
        this.valueSerde = valueSerde;

        this.dataSynchronizerCallback = dataSynchronizerCallback;
        this.producer = ConnectUtil.initDefaultMQProducer(connectConfig);
        this.producer.setProducerGroup(workId);
        this.consumer = ConnectUtil.initDefaultMQPushConsumer(connectConfig);
        this.consumer.setConsumerGroup(workId);
        this.prepare(connectConfig);
    }

    /**
     * Preparation before startup
     *
     * @param connectConfig
     */
    private void prepare(WorkerConfig connectConfig) {
        if (connectConfig.isAutoCreateGroupEnable()) {
            ConnectUtil.createSubGroup(connectConfig, consumer.getConsumerGroup());
        }
    }

    @Override
    public void start() {
        try {
            producer.start();
            consumer.subscribe(topicName, "*");
            consumer.registerMessageListener(new MessageListenerImpl());
            consumer.start();
        } catch (MQClientException e) {
            log.error("Start error.", e);
        }
    }

    @Override
    public void stop() {
        producer.shutdown();
        consumer.shutdown();
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
            Message message = new Message(topicName, body);
            message.setKeys(Base64Util.base64Encode(encode.getKey()));
            producer.send(message, new SendCallback() {

                @Override
                public void onSuccess(org.apache.rocketmq.client.producer.SendResult result) {
                    log.info("Send async message OK, msgId: {},topic:{}", result.getMsgId(), topicName);
                }

                @Override
                public void onException(Throwable throwable) {
                    if (null != throwable) {
                        log.error("Send async message Failed, error: {}", throwable);
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
            Message message = new Message(topicName, body);
            message.setKeys(Base64Util.base64Encode(encode.getKey()));
            producer.send(message, new SendCallback() {
                @Override
                public void onSuccess(org.apache.rocketmq.client.producer.SendResult result) {
                    log.info("Send async message OK, msgId: {},topic:{}", result.getMsgId(), topicName);
                    callback.onCompletion(null, value);
                }

                @Override
                public void onException(Throwable throwable) {
                    if (null != throwable) {
                        log.error("Send async message Failed, error: {}", throwable);
                        callback.onCompletion(throwable, value);
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


    class MessageListenerImpl implements MessageListenerConcurrently {
        @Override
        public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> rmqMsgList, ConsumeConcurrentlyContext context) {
            for (MessageExt messageExt : rmqMsgList) {
                log.info("Received one message: {}, topic is {}", messageExt.getMsgId() + "\n", topicName);
                try {
                    String key = messageExt.getKeys();
                    Map.Entry<K, V> entry = decode(StringUtils.isEmpty(key) ? null : Base64Util.base64Decode(key), messageExt.getBody());
                    dataSynchronizerCallback.onCompletion(null, entry.getKey(), entry.getValue());
                } catch (Exception e) {
                    log.error("Decode message data error. message: {}, error info: {}", messageExt, e);
                    return ConsumeConcurrentlyStatus.RECONSUME_LATER;
                }
            }
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        }
    }

}
