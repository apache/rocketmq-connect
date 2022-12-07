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

package org.apache.rocketmq.connect.http.sink;

import io.openmessaging.KeyValue;
import io.openmessaging.connector.api.component.task.Task;
import io.openmessaging.connector.api.component.task.source.SourceConnector;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SftpSourceConnector extends SourceConnector {

    private Logger log = LoggerFactory.getLogger(SftpConstant.LOGGER_NAME);

    private KeyValue config;

    @Override
    public List<KeyValue> taskConfigs(int i) {
        List<KeyValue> taskConfigs = new ArrayList<>();
        if (config != null) {
            taskConfigs.add(config);
        }
        return taskConfigs;
    }

    @Override
    public Class<? extends Task> taskClass() {
        return SftpSourceTask.class;
    }

    @Override
    public void start(KeyValue config) {
        log.info("Sftp connector started");
        this.config = config;
    }

    @Override
    public void stop() {
        log.info("Sftp connector stopped");
    }
}
