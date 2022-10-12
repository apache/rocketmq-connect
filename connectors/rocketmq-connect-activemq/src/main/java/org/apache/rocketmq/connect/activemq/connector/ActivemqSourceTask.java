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

package org.apache.rocketmq.connect.activemq.connector;

import com.alibaba.fastjson.JSON;
import io.openmessaging.KeyValue;
import io.openmessaging.connector.api.component.task.source.SourceTask;
import io.openmessaging.connector.api.data.ConnectRecord;
import io.openmessaging.connector.api.data.Field;
import io.openmessaging.connector.api.data.RecordOffset;
import io.openmessaging.connector.api.data.RecordPartition;
import io.openmessaging.connector.api.data.Schema;
import io.openmessaging.connector.api.data.SchemaBuilder;
import io.openmessaging.connector.api.data.Struct;
import io.openmessaging.internal.DefaultKeyValue;
import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import javax.jms.BytesMessage;
import javax.jms.JMSException;
import javax.jms.MapMessage;
import javax.jms.Message;
import javax.jms.ObjectMessage;
import javax.jms.StreamMessage;
import javax.jms.TextMessage;
import org.apache.rocketmq.connect.activemq.Config;
import org.apache.rocketmq.connect.activemq.Replicator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ActivemqSourceTask extends SourceTask {

    private static final Logger log = LoggerFactory.getLogger(ActivemqSourceTask.class);

    private Replicator replicator;

    private Config config;

    @Override
    public List<ConnectRecord> poll() {
        List<ConnectRecord> res = new ArrayList<>();
        try {
            Message message = replicator.getQueue().poll(1000, TimeUnit.MILLISECONDS);
            if (message != null) {
                res.add(message2ConnectRecord(message));
            }
        } catch (Exception e) {
            log.error("activemq task poll error, current config:" + JSON.toJSONString(config), e);
        }
        return res;
    }

    @Override
    public void start(KeyValue props) {
        try {
            this.config = new Config();
            this.config.load(props);
            this.replicator = new Replicator(config);
            final RecordOffset recordOffset = this.sourceTaskContext.offsetStorageReader().readOffset(buildRecordPartition());
            long offset = 0L;
            if (recordOffset != null) {
                final Object position = recordOffset.getOffset().get(Config.POSITION);
                if (position != null) {
                    offset = Long.valueOf(position.toString());
                }
            }
            this.replicator.start(offset);
        } catch (Exception e) {
            log.error("activemq task start failed.", e);
        }
    }

    @Override
    public void stop() {
        try {
            replicator.stop();
        } catch (Exception e) {
            log.error("activemq task stop failed.", e);
        }
    }



    @SuppressWarnings("unchecked")
    public String getMessageContent(Message message) throws JMSException {
        String data = null;
        if (message instanceof TextMessage) {
            data = ((TextMessage) message).getText();
        } else if (message instanceof ObjectMessage) {
            data = JSON.toJSONString(((ObjectMessage) message).getObject());
        } else if (message instanceof BytesMessage) {
            BytesMessage bytesMessage = (BytesMessage) message;
            data = bytesMessage.toString();
        } else if (message instanceof MapMessage) {
            MapMessage mapMessage = (MapMessage) message;
            Map<String, Object> map = new HashMap<>();
            Enumeration<Object> names = mapMessage.getMapNames();
            while (names.hasMoreElements()) {
                String name = names.nextElement().toString();
                map.put(name, mapMessage.getObject(name));
            }
            data = JSON.toJSONString(map);
        } else if (message instanceof StreamMessage) {
            StreamMessage streamMessage = (StreamMessage) message;
            ByteArrayOutputStream bis = new ByteArrayOutputStream();
            byte[] by = new byte[1024];
            int i = 0;
            while ((i = streamMessage.readBytes(by)) != -1) {
                bis.write(by, 0, i);
            }
            data = bis.toString();
        } else {
            // The exception is printed and does not need to be written as a DataConnectException
            throw new RuntimeException("message type exception");
        }
        return data;
    }

    private ConnectRecord message2ConnectRecord(Message message) throws JMSException {
        Schema schema = SchemaBuilder.struct().name("activemq").build();
        final List<Field> fields = buildFields();
        schema.setFields(fields);
        final ConnectRecord connectRecord = new ConnectRecord(buildRecordPartition(),
            buildRecordOffset(message),
            System.currentTimeMillis(),
            schema,
            buildPayLoad(fields, message, schema));
        connectRecord.setExtensions(buildExtendFiled());
        return connectRecord;
    }

    private RecordOffset buildRecordOffset(Message message) throws JMSException {
        Map<String, Long> offsetMap = new HashMap<>();
        offsetMap.put(Config.POSITION, Long.parseLong(message.getJMSMessageID().split(":")[5]));
        RecordOffset recordOffset = new RecordOffset(offsetMap);
        return recordOffset;
    }

    private RecordPartition buildRecordPartition() {
        Map<String, String> partitionMap = new HashMap<>();
        partitionMap.put("partition", "defaultPartition");
        RecordPartition  recordPartition = new RecordPartition(partitionMap);
        return recordPartition;
    }

    private List<Field> buildFields() {
        final Schema stringSchema = SchemaBuilder.string().build();
        List<Field> fields = new ArrayList<>();
        fields.add(new Field(0, Config.MESSAGE, stringSchema));
        return fields;
    }

    private Struct buildPayLoad(List<Field> fields, Message message, Schema schema) throws JMSException {
        Struct payLoad = new Struct(schema);
        payLoad.put(fields.get(0), getMessageContent(message));
        return payLoad;
    }

    private KeyValue buildExtendFiled() {
        KeyValue keyValue = new DefaultKeyValue();
        keyValue.put(Config.DESTINATION_NAME,  config.getDestinationName());
        keyValue.put(Config.DESTINATION_TYPE, config.getDestinationType());
        return keyValue;
    }
}
