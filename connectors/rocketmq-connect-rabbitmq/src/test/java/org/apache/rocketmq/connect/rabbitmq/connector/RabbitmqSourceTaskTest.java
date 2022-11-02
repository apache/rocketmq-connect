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

package org.apache.rocketmq.connect.rabbitmq.connector;

import com.alibaba.fastjson.JSON;
import com.rabbitmq.jms.admin.RMQConnectionFactory;
import com.rabbitmq.jms.client.message.RMQBytesMessage;
import com.rabbitmq.jms.client.message.RMQMapMessage;
import com.rabbitmq.jms.client.message.RMQObjectMessage;
import com.rabbitmq.jms.client.message.RMQStreamMessage;
import com.rabbitmq.jms.client.message.RMQTextMessage;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import javax.jms.BytesMessage;
import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MapMessage;
import javax.jms.MessageProducer;
import javax.jms.ObjectMessage;
import javax.jms.Session;
import javax.jms.StreamMessage;
import javax.jms.TextMessage;
import org.apache.rocketmq.connect.rabbitmq.RabbitmqConfig;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class RabbitmqSourceTaskTest {

	@Before
	public void before() throws JMSException {
		RMQConnectionFactory connectionFactory = new RMQConnectionFactory();
		connectionFactory.setHost("localhost");
		connectionFactory.setPort(5672);
		connectionFactory.setUsername("guest");
		connectionFactory.setPassword("guest");
		Connection connection = connectionFactory.createConnection("guest", "guest");

		connection.start();
		Session session = connection.createSession(true, Session.AUTO_ACKNOWLEDGE);
		Destination destination = session.createTopic("testTopic");

		MessageProducer producer = session.createProducer(destination);

		producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
		for (int i = 0; i < 20; i++) {
			TextMessage message = session.createTextMessage("hello 我是消息：" + i);
			producer.send(message);
		}

		session.commit();
		session.close();
		connection.close();
	}

	@Test
	public void getMessageContentTest() throws JMSException {
		String value = "hello rocketmq";
		RabbitmqSourceTask task = new RabbitmqSourceTask();
		RMQTextMessage textMessage = new RMQTextMessage();
		textMessage.setText(value);
		String content = task.getMessageContent(textMessage);
		Assert.assertEquals(content, textMessage.getText());

		ObjectMessage objectMessage = new RMQObjectMessage();
		objectMessage.setObject(value);
		content = task.getMessageContent(objectMessage);
		Assert.assertEquals(content, "\"" + objectMessage.getObject().toString() + "\"");

		MapMessage mapMessage = new RMQMapMessage();
		mapMessage.setString("hello", "rocketmq");
		content = task.getMessageContent(mapMessage);
		Map<String, String> map = JSON.parseObject(content, Map.class);
		Assert.assertEquals(map.get("hello"), "rocketmq");
		Assert.assertEquals(map.size(), 1);

	}
	
	@Test(expected=Exception.class)
	public void getMessageContentException() throws JMSException {
		RabbitmqSourceTask task = new RabbitmqSourceTask();
		task.getMessageContent(null);
		
	}

	@Test
	public void getConfig() {
		RabbitmqSourceTask task = new RabbitmqSourceTask();
		assertEquals(task.getConfig().getClass() , RabbitmqConfig.class);
	}
}
