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

package org.apache.connect.mongo.connector.builder;

import com.alibaba.fastjson.JSON;
import io.openmessaging.KeyValue;
import io.openmessaging.connector.api.data.ConnectRecord;
import io.openmessaging.connector.api.data.Field;
import io.openmessaging.connector.api.data.FieldType;
import io.openmessaging.connector.api.data.RecordOffset;
import io.openmessaging.connector.api.data.RecordPartition;
import io.openmessaging.connector.api.data.Schema;
import io.openmessaging.connector.api.data.SchemaBuilder;
import io.openmessaging.connector.api.data.Struct;
import io.openmessaging.internal.DefaultKeyValue;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.connect.mongo.replicator.Constants;
import org.apache.connect.mongo.replicator.Position;
import org.apache.connect.mongo.replicator.ReplicaSetConfig;
import org.apache.connect.mongo.replicator.event.ReplicationEvent;
import org.bson.BsonTimestamp;
import org.bson.Document;

public class MongoDataEntry {

    public static ConnectRecord createSourceDataEntry(ReplicationEvent event, ReplicaSetConfig replicaSetConfig) {
        final Position position = replicaSetConfig.getPosition();
        final int oldTimestamp = position.getTimeStamp();
        final BsonTimestamp timestamp = event.getTimestamp();
        if (oldTimestamp != 0 && timestamp != null &&  timestamp.getTime() <= oldTimestamp) {
            return null;
        }
        Schema schema = SchemaBuilder.struct().name(Constants.MONGO).build();
        final List<Field> fields = buildFields(event.getDocument());
        schema.setFields(fields);
        final ConnectRecord connectRecord = new ConnectRecord(buildRecordPartition(replicaSetConfig),
            buildRecordOffset(event, replicaSetConfig),
            System.currentTimeMillis(),
            schema,
            buildPayLoad(fields, event, schema));
        connectRecord.setExtensions(buildExtendFiled(event));
        return connectRecord;
    }

    private static RecordPartition buildRecordPartition(ReplicaSetConfig replicaSetConfig) {
        Map<String, String> partitionMap = new HashMap<>();
        partitionMap.put(Constants.REPLICA_SET_NAME, replicaSetConfig.getReplicaSetName());
        RecordPartition  recordPartition = new RecordPartition(partitionMap);
        return recordPartition;
    }

    private static RecordOffset buildRecordOffset(ReplicationEvent event, ReplicaSetConfig config)  {
        Map<String, Integer> offsetMap = new HashMap<>();
        final Position position = config.getPosition();
        offsetMap.put(Constants.TIMESTAMP, event.getTimestamp() != null ? event.getTimestamp().getTime() : position.getTimeStamp());
        RecordOffset recordOffset = new RecordOffset(offsetMap);
        return recordOffset;
    }

    private static List<Field> buildFields(Document document) {
        List<Field> fields = new ArrayList<>();
        int i = 0;
        for (Map.Entry<String, Object> entry : document.entrySet()) {
            final String key = entry.getKey();
            final Object value = entry.getValue();
            fields.add(new Field(i++, key, getSchema(value)));
        }
        return fields;
    }

    private static Struct buildPayLoad(List<Field> fields, ReplicationEvent event, Schema schema) {
        Struct payLoad = new Struct(schema);
        final Document document = event.getDocument();
        for (Field field : fields) {
            final Schema valueSchema = field.getSchema();
            if (valueSchema.getFieldType().equals(FieldType.STRING)) {
                payLoad.put(field, JSON.toJSONString(document.get(field.getName())));
            } else {
                payLoad.put(field, document.get(field.getName()));
            }
        }
        return payLoad;
    }

    private static KeyValue buildExtendFiled(ReplicationEvent event) {
        KeyValue keyValue = new DefaultKeyValue();
        keyValue.put(Constants.OPERATION_TYPE,  event.getOperationType().getOperation());
        keyValue.put(Constants.COLLECTION_NAME, event.getCollectionName());
        keyValue.put(Constants.NAMESPACE, event.getNamespace());
        keyValue.put(Constants.REPLICA_SET_NAME, event.getReplicaSetName());
        return keyValue;
    }

    private static Schema getSchema(Object value) {
        Schema schema = null;
        if (value instanceof Date) {
            schema = SchemaBuilder.time().build();
        } else if (value instanceof Document) {
            schema = SchemaBuilder.string().build();
        } else if (value instanceof Long) {
            schema = SchemaBuilder.int64().build();
        } else if (value instanceof Integer) {
            schema = SchemaBuilder.int32().build();
        } else if (value instanceof Boolean) {
            schema = SchemaBuilder.bool().build();
        } else {
            schema = SchemaBuilder.string().build();
        }
        return schema;
    }

}
