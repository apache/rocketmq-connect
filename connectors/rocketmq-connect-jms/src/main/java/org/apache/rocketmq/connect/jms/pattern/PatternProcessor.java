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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.rocketmq.connect.jms.pattern;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.Session;

import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.connect.jms.Config;
import org.apache.rocketmq.connect.jms.Replicator;

public abstract class PatternProcessor {

    private Replicator replicator;

    protected Config config;

    private Connection connection;

    private Session session;

    private MessageConsumer consumer;

    public PatternProcessor(Replicator replicator) {
        this.replicator = replicator;
        this.config = replicator.getConfig();
    }

    public abstract ConnectionFactory connectionFactory();

    public void start() throws Exception {
        if (!StringUtils.equals("topic", config.getDestinationType())
            && !StringUtils.equals("queue", config.getDestinationType())) {
            // RuntimeException is caught by DataConnectException
            throw new RuntimeException("destination type is incorrectness");
        }

        ConnectionFactory connectionFactory = connectionFactory();

        if (StringUtils.isNotBlank(config.getUsername())
            && StringUtils.isNotBlank(config.getPassword())) {
            connection = connectionFactory.createConnection(config.getUsername(), config.getPassword());
        } else {
            connection = connectionFactory.createConnection();
        }
        connection.start();
        Session session = connection.createSession(config.getSessionTransacted(), config.getSessionAcknowledgeMode());
        Destination destination = null;
        if (StringUtils.equals("topic", config.getDestinationType())) {
            destination = session.createTopic(config.getDestinationName());
        } else if (StringUtils.equals("queue", config.getDestinationType())) {
            destination = session.createQueue(config.getDestinationName());
        }
        consumer = session.createConsumer(destination, config.getMessageSelector());
        consumer.setMessageListener(new MessageListener() {
            @Override
            public void onMessage(Message message) {
                replicator.commit(message, true);
            }
        });

    }

    public void stop() throws Exception {
        consumer.close();
        session.close();
        connection.close();
    }

}
