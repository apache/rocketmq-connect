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

package org.apache.rocketmq.connect.jms.connector;

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

import org.apache.rocketmq.connect.jms.Config;
import org.apache.rocketmq.connect.jms.Replicator;
import org.apache.rocketmq.connect.jms.pattern.PatternProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSON;

import io.openmessaging.KeyValue;


public abstract class BaseJmsSourceTask extends SourceTask {

    private static final Logger log = LoggerFactory.getLogger(BaseJmsSourceTask.class);

    protected Replicator replicator;

    protected Config config;

    @Override
    public List<ConnectRecord> poll() {
        List<ConnectRecord> res = new ArrayList<>();
        try {
            Message message = replicator.getQueue().poll(1000, TimeUnit.MILLISECONDS);
            if (message != null) {
                final ConnectRecord connectRecord = this.message2ConnectRecord(message);
                connectRecord.setExtensions(this.buildExtendFiled());
                res.add(connectRecord);
            }
        } catch (Exception e) {
            log.error("jms task poll error, current config:" + JSON.toJSONString(config), e);
        }
        return res;
    }

    @Override
    public void start(KeyValue props) {
        try {
            this.config = new Config();
            this.config.load(props);
            replicator = new Replicator(this.config, this);
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
            log.error("jms task start failed.", e);
            throw new RuntimeException(e.getMessage());
        }
    }

    @Override
    public void stop() {
        try {
            replicator.stop();
        } catch (Exception e) {
            log.error("jms task stop failed.", e);
            throw new RuntimeException(e.getMessage());
        }
    }

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
            throw new RuntimeException("message type exception");
        }
        return data;
    }

    protected ConnectRecord message2ConnectRecord(Message message) throws JMSException {
        Schema schema = SchemaBuilder.struct().name("jms").build();
        final List<Field> fields = buildFields();
        schema.setFields(fields);
        return new ConnectRecord(buildRecordPartition(),
            buildRecordOffset(message),
            System.currentTimeMillis(),
            schema,
            buildPayLoad(fields, message, schema));
    }

    protected RecordOffset buildRecordOffset(Message message) throws JMSException {
        Map<String, Long> offsetMap = new HashMap<>();
        offsetMap.put(Config.POSITION, message.getJMSTimestamp());
        RecordOffset recordOffset = new RecordOffset(offsetMap);
        return recordOffset;
    }

    protected RecordPartition buildRecordPartition() {
        Map<String, String> partitionMap = new HashMap<>();
        partitionMap.put("partition", config.getHost() + ":" + config.getPort());
        RecordPartition  recordPartition = new RecordPartition(partitionMap);
        return recordPartition;
    }

    protected List<Field> buildFields() {
        final Schema stringSchema = SchemaBuilder.string().build();
        List<Field> fields = new ArrayList<>();
        fields.add(new Field(0, Config.MESSAGE, stringSchema));
        return fields;
    }

    protected Struct buildPayLoad(List<Field> fields, Message message, Schema schema) throws JMSException {
        Struct payLoad = new Struct(schema);
        payLoad.put(fields.get(0), getMessageContent(message));
        return payLoad;
    }

    protected KeyValue buildExtendFiled() {
        KeyValue keyValue = new DefaultKeyValue();
        keyValue.put(Config.DESTINATION_NAME,  config.getDestinationName());
        keyValue.put(Config.DESTINATION_TYPE, config.getDestinationType());
        return keyValue;
    }

    public abstract Config getConfig();
    
    public abstract PatternProcessor getPatternProcessor(Replicator replicator);
}
