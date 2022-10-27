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

package org.apache.rocketmq.connect.mqtt.connector;

import io.openmessaging.KeyValue;
import io.openmessaging.connector.api.component.task.sink.SinkTask;
import io.openmessaging.connector.api.data.ConnectRecord;
import io.openmessaging.connector.api.errors.ConnectException;
import org.apache.rocketmq.connect.mqtt.config.SinkConnectorConfig;
import org.apache.rocketmq.connect.mqtt.util.ConfigUtil;
import org.apache.rocketmq.connect.mqtt.sink.Updater;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 *
 */
public class MqttSinkTask extends SinkTask {
    private static final Logger log = LoggerFactory.getLogger(MqttSinkTask.class);

    private SinkConnectorConfig sinkConnectConfig;
    private Updater updater;

    public MqttSinkTask() {
        this.sinkConnectConfig = new SinkConnectorConfig();
    }

    @Override
    public void put(List<ConnectRecord> sinkDataEntries) throws ConnectException {
        try {
            log.info("MQTT Sink Task trying to put()");
            for (ConnectRecord record : sinkDataEntries) {
                log.info("MQTT Sink Task trying to call updater.push()");
                Boolean isSuccess = updater.push(record);
                if (!isSuccess) {
                    log.error("mqtt sink push data error, record:{}", record);
                }
                log.debug("mqtt pushed data : " + record);
            }
        } catch (Exception e) {
            log.error("put sinkDataEntries error, {}", e);
        }
    }

    /**
     * @param props
     */
    @Override
    public void start(KeyValue props) {
        try {
            ConfigUtil.load(props, this.sinkConnectConfig);
            log.info("init data source success");
        } catch (Exception e) {
            log.error("Cannot start MQTT Sink Task because of configuration error{}", e);
        }
        try {
            updater = new Updater(sinkConnectConfig);
            updater.start();
        } catch (Throwable e) {
            log.error("fail to start updater{}", e);
        }

    }

    @Override
    public void stop() {
        try {
            updater.stop();
            log.info("mqtt sink task connection is closed.");
        } catch (Throwable e) {
            log.warn("sink task stop error while closing connection to mqtt", e);
        }
    }

}
