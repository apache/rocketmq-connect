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
import io.openmessaging.connector.api.component.task.Task;
import io.openmessaging.connector.api.component.task.sink.SinkConnector;
import io.openmessaging.connector.api.errors.ConnectException;
import java.net.URL;
import java.net.URLConnection;
import java.util.ArrayList;
import java.util.List;
import org.apache.rocketmq.connect.enterprisewechat.common.SinkConstants;
import org.apache.rocketmq.connect.enterprisewechat.config.SinkConfig;

public class EnterpriseWechatSinkConnector extends SinkConnector {
    private KeyValue connectConfig;

    @Override
    public void validate(KeyValue config) {
        for (String requestKey : SinkConfig.REQUEST_CONFIG) {
            if (!config.containsKey(requestKey)) {
                throw new ConnectException("Request config key: " + requestKey);
            }
        }

        try {
            URL urlConnect = new URL(config.getString(SinkConstants.WEB_HOOK));
            URLConnection urlConnection = urlConnect.openConnection();
            urlConnection.setConnectTimeout(5000);
            urlConnection.connect();
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage());
        }
    }

    @Override
    public List<KeyValue> taskConfigs(int maxTasks) {
        List<KeyValue> configs = new ArrayList<>(maxTasks);
        for (int i = 0; i < maxTasks; i++) {
            configs.add(this.connectConfig);
        }
        return configs;
    }

    @Override
    public Class<? extends Task> taskClass() {
        return EnterpriseWechatSinkTask.class;
    }

    @Override
    public void start(KeyValue config) {
        for (String requestKey : SinkConfig.REQUEST_CONFIG) {
            if (!config.containsKey(requestKey)) {
                throw new RuntimeException("Request config key: " + requestKey);
            }
        }
        this.connectConfig = config;
    }

    @Override
    public void stop() {
        this.connectConfig = null;
    }
}
