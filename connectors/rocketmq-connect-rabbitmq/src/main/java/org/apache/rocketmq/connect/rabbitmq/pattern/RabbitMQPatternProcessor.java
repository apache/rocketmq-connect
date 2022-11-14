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

package org.apache.rocketmq.connect.rabbitmq.pattern;

import com.rabbitmq.jms.admin.RMQConnectionFactory;
import javax.jms.ConnectionFactory;
import org.apache.rocketmq.connect.jms.Replicator;
import org.apache.rocketmq.connect.jms.pattern.PatternProcessor;

public class RabbitMQPatternProcessor extends PatternProcessor {


    public RabbitMQPatternProcessor(Replicator replicator) {
        super(replicator);
    }

    public ConnectionFactory connectionFactory() {
        RMQConnectionFactory connectionFactory = new RMQConnectionFactory();
        connectionFactory.setUsername(this.config.getUsername());
        connectionFactory.setPassword(this.config.getPassword());
        connectionFactory.setHost(this.config.getHost());
        connectionFactory.setPort(this.config.getPort());

        return connectionFactory;
    }

    public void start(long offset) throws Exception {
        // Sets the context class loader to a custom class loader to prevent ClassNotFoundException
        Thread.currentThread().setContextClassLoader(this.getClass().getClassLoader());
        super.start(offset);
    }

}
