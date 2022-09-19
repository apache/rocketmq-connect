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
package org.apache.rocketmq.connect.kafka.connect.adaptor.schema;

import io.openmessaging.connector.api.errors.ConnectException;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * convert rocketmq connect record to kafka sink record
 */
public class KafkaSinkValueConverter {

    private static Logger logger = LoggerFactory.getLogger(KafkaSinkValueConverter.class);

    public Object value(Schema schema, Object data) {
        return convertKafkaValue(schema, data);
    }

    /**
     * convert value
     *
     * @param targetSchema
     * @param originalValue
     * @return
     */
    private Object convertKafkaValue(Schema targetSchema, Object originalValue) {
        if (targetSchema == null) {
            if (originalValue == null) {
                return null;
            }
            return  originalValue;
        }
        switch (targetSchema.type()) {
            case INT8:
            case INT16:
            case INT32:
            case INT64:
            case FLOAT32:
            case FLOAT64:
            case BOOLEAN:
            case STRING:
            case BYTES:
                return originalValue;
            case STRUCT:
                Struct toStruct = new Struct(targetSchema);
                if (originalValue != null) {
                    convertStructValue(toStruct, (org.apache.kafka.connect.data.Struct) originalValue);
                }
                return toStruct;
            case ARRAY:
                List<Object> array = (List<Object>) originalValue;
                List<Object> newArray = new ArrayList<>();
                array.forEach(item -> {
                    newArray.add(convertKafkaValue(targetSchema.valueSchema(), item));
                });
                return newArray;
            case MAP:
                Map mapData = (Map) originalValue;
                Map newMapData = new ConcurrentHashMap();
                mapData.forEach((k, v) -> {
                    newMapData.put(
                            convertKafkaValue(targetSchema.keySchema(), k),
                            convertKafkaValue(targetSchema.valueSchema(), v)
                    );
                });
                return newMapData;
            default:
                throw new RuntimeException(" Type not supported: {}" + targetSchema.type());

        }

    }

    /**
     * convert struct value
     *
     * @param toStruct
     * @param originalStruct
     */
    private void convertStructValue(Struct toStruct, org.apache.kafka.connect.data.Struct originalStruct) {

        for (Field field : toStruct.schema().fields()) {
            try {
                Schema.Type type = field.schema().type();
                Object value = originalStruct.get(field.name());
                switch (type) {
                    case INT8:
                    case INT16:
                    case INT32:
                    case INT64:
                    case FLOAT32:
                    case FLOAT64:
                    case BOOLEAN:
                    case STRING:
                    case BYTES:
                        toStruct.put(field.name(), value);
                        break;
                    case STRUCT:
                    case ARRAY:
                    case MAP:
                        toStruct.put(
                                field.name(),
                                convertKafkaValue(
                                        toStruct.schema().field(field.name()).schema(),
                                        value
                                )
                        );
                        break;
                }
            } catch (Exception ex) {
                logger.error("Convert to kafka schema failure, {}", ex);
                throw new ConnectException(ex);
            }
        }
    }
}
