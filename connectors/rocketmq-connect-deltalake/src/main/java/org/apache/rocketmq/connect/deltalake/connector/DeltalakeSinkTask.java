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


package org.apache.rocketmq.connect.deltalake.connector;

import org.apache.rocketmq.connect.deltalake.config.ConfigUtil;
import org.apache.rocketmq.connect.deltalake.config.DeltalakeConnectConfig;
import org.apache.rocketmq.connect.deltalake.exception.WriteParquetException;
import org.apache.rocketmq.connect.deltalake.writer.DeltalakeWriterOnHdfs;
import io.openmessaging.KeyValue;
import io.openmessaging.connector.api.component.task.sink.SinkTask;
import io.openmessaging.connector.api.data.ConnectRecord;
import io.openmessaging.connector.api.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * @author osgoo
 * @date 2022/8/19
 */
public class DeltalakeSinkTask extends SinkTask {
    private static final Logger log = LoggerFactory.getLogger(DeltalakeSinkTask.class);
    private DeltalakeConnectConfig deltalakeConnectConfig = new DeltalakeConnectConfig();
    private DeltalakeWriterOnHdfs writer;

    @Override
    public void put(List<ConnectRecord> list) throws ConnectException {
        try {
            writer.writeEntries(list);
        } catch (WriteParquetException e) {
            log.error("write to parquet exception,", e);
            throw new ConnectException("write to parquet exception,", e);
        }
    }

    @Override
    public void start(KeyValue props) {
        try {
            ConfigUtil.load(props, this.deltalakeConnectConfig);
            log.info("init data source success");
        } catch (Exception e) {
            log.error("Cannot start Hudi Sink Task because of configuration error{}", e);
        }
        try {
            writer = new DeltalakeWriterOnHdfs(deltalakeConnectConfig);
        } catch (Throwable e) {
            log.error("fail to start updater{}", e);
        }
    }

    @Override
    public void stop() {

    }

    @Override
    public void validate(KeyValue config) {
        ConfigUtil.load(config, deltalakeConnectConfig);
    }
}
