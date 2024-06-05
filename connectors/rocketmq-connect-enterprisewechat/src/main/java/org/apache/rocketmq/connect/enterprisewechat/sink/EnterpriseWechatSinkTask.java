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

package org.apache.rocketmq.connect.enterprisewechat.sink;

import io.openmessaging.KeyValue;
import io.openmessaging.connector.api.component.task.sink.SinkTask;
import io.openmessaging.connector.api.data.ConnectRecord;
import io.openmessaging.connector.api.errors.ConnectException;
import java.util.List;
import org.apache.rocketmq.connect.enterprisewechat.common.SinkConstants;
import org.apache.rocketmq.connect.enterprisewechat.util.OkHttpUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EnterpriseWechatSinkTask extends SinkTask {
    private static final Logger log = LoggerFactory.getLogger(EnterpriseWechatSinkTask.class);

    private String webHook;

    @Override
    public void put(List<ConnectRecord> sinkRecords) throws ConnectException {
        try {
            sinkRecords.forEach(sinkRecord -> {
                try {
                    String sync = OkHttpUtils.builder()
                        .url(webHook)
                        .addHeader(SinkConstants.CONTENT_TYPE, SinkConstants.APPLICATION_JSON_UTF_8_TYPE)
                        .postForStringBody(sinkRecord.getData())
                        .sync();
                    log.info("EnterpriseWechatSinkTask put sync : {}", sync);
                } catch (Exception e) {
                    log.error("EnterpriseWechatSinkTask | put | addParam | error => ", e);
                }
            });
        } catch (Exception e) {
            log.error("EnterpriseWechatSinkTask | put | error => ", e);
        }
    }

    @Override
    public void start(KeyValue config) {
        webHook = config.getString(SinkConstants.WEB_HOOK);
    }

    @Override
    public void stop() {

    }
}
