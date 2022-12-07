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

package org.apache.rocketmq.connect.cassandra.connector;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.datastax.oss.driver.api.core.CqlSession;
import io.openmessaging.KeyValue;
import io.openmessaging.connector.api.component.task.source.SourceTask;
import io.openmessaging.connector.api.data.ConnectRecord;
import io.openmessaging.connector.api.data.Field;
import io.openmessaging.connector.api.data.RecordOffset;
import io.openmessaging.connector.api.data.RecordPartition;
import io.openmessaging.connector.api.data.Schema;
import io.openmessaging.connector.api.data.SchemaBuilder;
import io.openmessaging.connector.api.data.Struct;
import io.openmessaging.connector.api.storage.OffsetStorageReader;
import io.openmessaging.internal.DefaultKeyValue;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.connect.cassandra.common.ConstDefine;
import org.apache.rocketmq.connect.cassandra.common.DBUtils;
import org.apache.rocketmq.connect.cassandra.config.Config;
import org.apache.rocketmq.connect.cassandra.config.ConfigUtil;
import org.apache.rocketmq.connect.cassandra.schema.Table;
import org.apache.rocketmq.connect.cassandra.source.Querier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CassandraSourceTask extends SourceTask {

    private static final Logger log = LoggerFactory.getLogger(CassandraSourceTask.class);

    private Config config;

    private CqlSession cqlSession;

    BlockingQueue<Querier> tableQueue = new LinkedBlockingQueue<Querier>();
    private Querier querier;

    public CassandraSourceTask() {
        this.config = new Config();
    }

    @Override
    public List<ConnectRecord> poll() {
        List<ConnectRecord> res = new ArrayList<>();
        try {
            if (tableQueue.size() > 1)
                querier = tableQueue.poll(1000, TimeUnit.MILLISECONDS);
            else
                querier = tableQueue.peek();
            try {
                TimeUnit.MILLISECONDS.sleep(1000);
            } catch (Exception e) {
                throw e;
            }
            querier.poll();

            for (Table dataRow : querier.getList()) {

                Schema schema = SchemaBuilder.struct().name(dataRow.getName()).build();
                final List<Field> fields = buildFields(dataRow);
                schema.setFields(fields);
                ConnectRecord connectRecord = new ConnectRecord(
                    buildRecordPartition(dataRow),
                    buildRecordOffset(dataRow),
                    System.currentTimeMillis(),
                    schema,
                    buildPayLoad(fields, schema, dataRow));

                connectRecord.setExtensions(buildExtensions(dataRow));
                res.add(connectRecord);
                log.debug("connectRecord : {}", JSONObject.toJSONString(connectRecord));
            }
        } catch (Exception e) {
            log.error("Cassandra task poll error, current config:" + JSON.toJSONString(config), e);
        }
        log.debug("connectRecord poll successfully,{}", JSONObject.toJSONString(res));
        return res;
    }

    @Override
    public void start(KeyValue props) {
        try {
            ConfigUtil.load(props, this.config);
            cqlSession = DBUtils.initCqlSession(config);
            log.info("init data source success");
        } catch (Exception e) {
            log.error("Cannot start Cassandra Source Task because of configuration error{}", e);
        }
        String mode = config.getMode();
        if (mode.equals("bulk")) {
            final OffsetStorageReader reader = this.sourceTaskContext.offsetStorageReader();
            Querier querier = new Querier(config, cqlSession, reader);
            try {
                querier.start();
                tableQueue.add(querier);
            } catch (Exception e) {
                log.error("start querier failed in bulk mode{}", e);
            }
        }

    }

    @Override
    public void stop() {
        try {
            if (cqlSession != null) {
                cqlSession.close();
                log.info("Cassandra source task connection is closed.");
            }
        } catch (Throwable e) {
            log.warn("source task stop error while closing connection to {}", "Cassandra", e);
        }
    }

    private RecordPartition buildRecordPartition(Table table) {
        Map<String, String> partitionMap = new HashMap<>();
        partitionMap.put(ConstDefine.DATABASE_NAME, table.getDatabase());
        partitionMap.put(ConstDefine.TABLE, table.getName());
        RecordPartition  recordPartition = new RecordPartition(partitionMap);
        return recordPartition;
    }

    private KeyValue buildExtensions(Table table) {
        KeyValue keyValue = new DefaultKeyValue();
        keyValue.put(ConstDefine.DATABASE_NAME, table.getDatabase());
        keyValue.put(ConstDefine.TABLE, table.getName());
        return keyValue;
    }

    private static RecordOffset buildRecordOffset(Table table)  {
        Map<String, Long> offsetMap = new HashMap<>();
        offsetMap.put(ConstDefine.INCREASE + table.getDatabase() + table.getName(), Long.valueOf(table.getDataList().get(0).toString()));
        RecordOffset recordOffset = new RecordOffset(offsetMap);
        return recordOffset;
    }

    private static List<Field> buildFields(Table table) {
        List<Field> fields = new ArrayList<>();
        final List<String> colList = table.getColList();
        final List<String> rowDataTypeList = table.getRawDataTypeList();
        for (int i = 0; i < colList.size(); i++) {
            final Schema schema = getSchema(rowDataTypeList.get(i));
            fields.add(new Field(i, colList.get(i), schema));
        }
        return fields;
    }

    private static Struct buildPayLoad(List<Field> fields, Schema schema, Table table) {
        Struct payLoad = new Struct(schema);
        final List<Object> dataList = table.getDataList();
        for (int i = 0; i < dataList.size(); i++) {
            payLoad.put(fields.get(i), dataList.get(i));
        }
        return payLoad;
    }

    private static Schema getSchema(String type) {
        switch (type) {
            case "int": {
                return SchemaBuilder.int32().build();
            }
            case "bigInt": {
                return SchemaBuilder.int64().build();
            }
            case "text": {
                return SchemaBuilder.string().build();
            }
            case "boolean": {
                return SchemaBuilder.bool().build();
            }
            case "timestamp": {
                return SchemaBuilder.timestamp().build();
            }
            case "varchar": {
                return SchemaBuilder.string().build();
            }
            default:
                return SchemaBuilder.string().build();
        }
    }

}
