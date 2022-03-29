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

package org.apache.rocketmq.connect.redis.connector;

import com.alibaba.fastjson.JSONObject;
import io.openmessaging.KeyValue;
import io.openmessaging.connector.api.common.QueueMetaData;
import io.openmessaging.connector.api.data.Field;
import io.openmessaging.connector.api.data.Schema;
import io.openmessaging.connector.api.data.SinkDataEntry;
import io.openmessaging.connector.api.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * author doubleDimple
 */
public class RedisSinkTask extends SinkTask {

    private static final Logger LOGGER = LoggerFactory.getLogger(RedisSinkTask.class);

    @Override
    public void put(Collection<SinkDataEntry> sinkDataEntries) {
             //save data from MQ to redis
            for (SinkDataEntry record : sinkDataEntries) {
                Map<Field, Object[]> fieldMap = new HashMap<>();
                Object[] payloads = record.getPayload();
                Schema schema = record.getSchema();
                //判断schema的datasource,是不是redis,如果是redis,按照redis命令存储,
                //如果不是redis,则直接将数据存储到redis的集合中
                List<Field> fields = schema.getFields();
                Boolean parseError = false;
                if (!fields.isEmpty()) {
                    for (Field field : fields) {
                        Object fieldValue = payloads[field.getIndex()];
                        Object[] value = JSONObject.parseArray((String)fieldValue).toArray();
                        if (value.length == 2) {
                            fieldMap.put(field, value);
                        } else {
                            LOGGER.error("parseArray error, fieldValue:{}", fieldValue);
                            parseError = true;
                        }
                    }
                }
                if (!parseError) {

                }
            }
    }

    @Override
    public void commit(Map<QueueMetaData, Long> offsets) {

    }

    @Override
    public void start(KeyValue config) {

    }

    @Override
    public void stop() {

    }

    @Override
    public void pause() {

    }

    @Override
    public void resume() {

    }
}
