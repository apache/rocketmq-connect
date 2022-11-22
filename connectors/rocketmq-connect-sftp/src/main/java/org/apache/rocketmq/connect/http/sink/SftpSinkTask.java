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
import io.openmessaging.connector.api.component.task.sink.SinkTask;
import io.openmessaging.connector.api.data.ConnectRecord;
import io.openmessaging.connector.api.errors.ConnectException;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.List;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.rocketmq.connect.http.sink.SftpConstant.SFTP_FILENAME_KEY;
import static org.apache.rocketmq.connect.http.sink.SftpConstant.SFTP_HOST_KEY;
import static org.apache.rocketmq.connect.http.sink.SftpConstant.SFTP_PASSWORD_KEY;
import static org.apache.rocketmq.connect.http.sink.SftpConstant.SFTP_PATH_KEY;
import static org.apache.rocketmq.connect.http.sink.SftpConstant.SFTP_PORT_KEY;
import static org.apache.rocketmq.connect.http.sink.SftpConstant.SFTP_USERNAME_KEY;

public class SftpSinkTask extends SinkTask {
    private static final Logger log = LoggerFactory.getLogger(SftpSinkTask.class);

    private SftpClient sftpClient;

    private String filename;

    @Override public void put(List<ConnectRecord> sinkRecords) throws ConnectException {
        try {
            for (ConnectRecord connectRecord : sinkRecords) {
                String str = (String) connectRecord.getData();
                str = str + System.lineSeparator();
                sftpClient.append(new ByteArrayInputStream(str.getBytes()), filename);
            }
        } catch (IOException e) {
            log.error("sink task ioexception", e);
        } finally {
            sftpClient.close();
        }
        sinkRecords.forEach(System.out::println);
    }

    @Override public void validate(KeyValue config) {
        if (StringUtils.isBlank(config.getString(SFTP_HOST_KEY))
            || StringUtils.isBlank(config.getString(SFTP_PORT_KEY))
            || StringUtils.isBlank(config.getString(SFTP_USERNAME_KEY))
            || StringUtils.isBlank(config.getString(SFTP_PASSWORD_KEY))
            || StringUtils.isBlank(config.getString(SFTP_PATH_KEY))
            || StringUtils.isBlank(config.getString(SFTP_FILENAME_KEY))) {
            throw new RuntimeException("missing required config");
        }
    }

    @Override public void start(KeyValue config) {
        String host = config.getString(SFTP_HOST_KEY);
        int port = config.getInt(SFTP_PORT_KEY);
        String username = config.getString(SFTP_USERNAME_KEY);
        String password = config.getString(SFTP_PASSWORD_KEY);
        String path = config.getString(SFTP_PATH_KEY);
        filename = config.getString(SFTP_FILENAME_KEY);
        sftpClient = new SftpClient(host, port, username, password, path);
    }

    @Override public void stop() {

    }
}
