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

package org.apache.rocketmq.connect.iotdb.replicator.source;

import io.openmessaging.KeyValue;
import io.openmessaging.connector.api.data.RecordOffset;
import java.util.List;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.session.SessionDataSet;
import org.apache.rocketmq.connect.iotdb.config.IotdbConfig;
import org.apache.rocketmq.connect.iotdb.config.IotdbConstant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IotdbQuery {

    private static final Logger log = LoggerFactory.getLogger(IotdbQuery.class);

    private IotdbReplicator replicator;

    private IotdbConfig config;

    private Session session;

    public IotdbQuery(IotdbReplicator replicator) {
        this.replicator = replicator;
        this.config = replicator.getConfig();


    }

    public void start(RecordOffset recordOffset, KeyValue keyValue) {
        if (session == null) {
            session =
                new Session.Builder()
                    .host(config.getIotdbHost())
                    .port(config.getIotdbPort())
                    .build();
            try {
                session.open();
            } catch (IoTDBConnectionException e) {
                log.error("iotdb session open failed", e);
            }
        }

        String path = null;
        try {
            path = keyValue.getString(IotdbConstant.IOTDB_PATH);
            if (path == null || path.trim().equals("")) {
                log.warn("the path is empty,please check config");
                return;
            }
            long time = 0;
            if (recordOffset != null && recordOffset.getOffset() != null && recordOffset.getOffset().size() > 0) {
                final Long offsetValue = (Long) recordOffset.getOffset().get(keyValue.getString(IotdbConstant.IOTDB_PARTITION) + path);
                if (offsetValue != null) {
                    time = offsetValue;
                }
            }
            int limit = 500;
            long offset = 1;
            String sql = "select * from " + path + " where time > " + time + " limit " + limit + " offset " + offset;
            while (true) {
                sql = "select * from " + path + " where time > " + time + " limit " + limit + " offset " + offset;
                final SessionDataSet timeseries = session.executeQueryStatement(sql);

                final List<String> names = timeseries.getColumnNames();
                if (!timeseries.hasNext()) {
                    break;
                }
                final List<String> types = timeseries.getColumnTypes();
                while (timeseries.hasNext()) {
                    DeviceEntity entity = new DeviceEntity();
                    entity.setColumnNames(names);
                    entity.setColumnTypes(types);
                    entity.setRowRecord(timeseries.next());
                    entity.setPath(path);
                    this.replicator.getQueue().add(entity);
                }
                offset += limit;
            }

        } catch (StatementExecutionException e) {
            log.error("search data from path:[{}] failed, cause StatementExecutionException", path, e);
        } catch (IoTDBConnectionException e) {
            log.error("search data from path:[{}] failed, cause IoTDBConnectionException", path, e);
        } catch (Exception e) {
            log.error("search data from path:[{}] failed, cause Exception", path, e);
        }

    }

    public void stop() {
        try {
            session.close();
        } catch (IoTDBConnectionException e) {
            log.error("iotdb session close failed", e);
        }
    }
}
