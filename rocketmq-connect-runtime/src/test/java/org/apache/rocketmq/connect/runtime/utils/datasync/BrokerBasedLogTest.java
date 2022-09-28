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


import java.lang.reflect.Field;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.connect.runtime.config.WorkerConfig;
import org.apache.rocketmq.connect.runtime.serialization.Serde;
import org.apache.rocketmq.connect.runtime.serialization.Serializer;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class BrokerBasedLogTest {

    @Mock
    private DefaultMQProducer producer;

    @Mock
    private DefaultMQPushConsumer consumer;

    private String topicName;

    private String consumerGroup;

    private String producerGroup;

    private BrokerBasedLog brokerBasedLog;

    @Mock
    private DataSynchronizerCallback dataSynchronizerCallback;

    @Mock
    private Serde serde;

    private WorkerConfig connectConfig;

    @Mock
    private Serializer serializer;

    @Before
    public void init() throws IllegalAccessException, NoSuchFieldException {
        topicName = "testTopicName";
        consumerGroup = "testConsumerGroup1";
        producerGroup = "testProducerGroup1";
        connectConfig = new WorkerConfig();
        connectConfig.setRmqConsumerGroup(consumerGroup);
        connectConfig.setRmqProducerGroup(producerGroup);
        connectConfig.setNamesrvAddr("127.0.0.1:9876");
        connectConfig.setRmqMinConsumeThreadNums(1);
        connectConfig.setRmqMaxConsumeThreadNums(32);
        connectConfig.setRmqMessageConsumeTimeout(3 * 1000);

        brokerBasedLog = new BrokerBasedLog(connectConfig, topicName, consumerGroup, dataSynchronizerCallback, serde, serde);

        final Field producerField = BrokerBasedLog.class.getDeclaredField("producer");
        producerField.setAccessible(true);
        producerField.set(brokerBasedLog, producer);

        final Field consumerField = BrokerBasedLog.class.getDeclaredField("consumer");
        consumerField.setAccessible(true);
        consumerField.set(brokerBasedLog, consumer);

    }

    @Test
    public void testStart() throws MQClientException {
        brokerBasedLog.start();
        verify(producer, times(1)).start();
        verify(consumer, times(1)).subscribe(anyString(), anyString());
        verify(consumer, times(1)).start();
    }

    @Test
    public void testStop() {
        brokerBasedLog.stop();
        verify(producer, times(1)).shutdown();
        verify(consumer, times(1)).shutdown();
    }

    @Test
    public void testSend() throws RemotingException, MQClientException, InterruptedException {
        doReturn(serializer).when(serde).serializer();
        when(serializer.serialize(anyString(), any())).thenReturn(new byte[0]);
        brokerBasedLog.send(new Object(), new Object());
        verify(producer, times(1)).send(any(Message.class), any(SendCallback.class));
    }

}