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

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import io.openmessaging.KeyValue;
import io.openmessaging.connector.api.component.task.sink.SinkTask;
import io.openmessaging.connector.api.data.ConnectRecord;
import io.openmessaging.connector.api.errors.ConnectException;
import java.io.IOException;
import java.io.OutputStream;
import java.util.EnumSet;
import java.util.List;
import java.util.regex.Pattern;
import net.schmizz.sshj.sftp.OpenMode;
import net.schmizz.sshj.sftp.RemoteFile;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.rocketmq.connect.http.sink.SftpConstant.SFTP_FIELD_SCHEMA;
import static org.apache.rocketmq.connect.http.sink.SftpConstant.SFTP_FIELD_SEPARATOR;
import static org.apache.rocketmq.connect.http.sink.SftpConstant.SFTP_HOST_KEY;
import static org.apache.rocketmq.connect.http.sink.SftpConstant.SFTP_PASSWORD_KEY;
import static org.apache.rocketmq.connect.http.sink.SftpConstant.SFTP_PATH_KEY;
import static org.apache.rocketmq.connect.http.sink.SftpConstant.SFTP_PORT_KEY;
import static org.apache.rocketmq.connect.http.sink.SftpConstant.SFTP_USERNAME_KEY;

public class SftpSinkTask extends SinkTask {
    private static final Logger log = LoggerFactory.getLogger(SftpSinkTask.class);

    private SftpClient sftpClient;

    private String filePath;

    private String fieldSeparator;

    private String[] fieldSchema;

    @Override public void validate(KeyValue config) {
        if (StringUtils.isBlank(config.getString(SFTP_HOST_KEY))
            || StringUtils.isBlank(config.getString(SFTP_PORT_KEY))
            || StringUtils.isBlank(config.getString(SFTP_USERNAME_KEY))
            || StringUtils.isBlank(config.getString(SFTP_PASSWORD_KEY))
            || StringUtils.isBlank(config.getString(SFTP_PATH_KEY))) {
            throw new RuntimeException("missing required config");
        }
    }

    @Override public void start(KeyValue config) {
        String host = config.getString(SFTP_HOST_KEY);
        int port = config.getInt(SFTP_PORT_KEY);
        String username = config.getString(SFTP_USERNAME_KEY);
        String password = config.getString(SFTP_PASSWORD_KEY);
        this.filePath = config.getString(SFTP_PATH_KEY);
        this.sftpClient = new SftpClient(host, port, username, password);
        fieldSeparator = config.getString(SFTP_FIELD_SEPARATOR);
        String fieldSchemaStr = config.getString(SFTP_FIELD_SCHEMA);
        fieldSchema = fieldSchemaStr.split(Pattern.quote(fieldSeparator));
    }

    @Override public void put(List<ConnectRecord> sinkRecords) throws ConnectException {
        try (RemoteFile remoteFile = sftpClient.open(filePath, EnumSet.of(OpenMode.READ, OpenMode.CREAT, OpenMode.WRITE, OpenMode.APPEND));
             OutputStream outputStream = remoteFile.new RemoteFileOutputStream()) {
            for (ConnectRecord connectRecord : sinkRecords) {
                String str = (String) connectRecord.getData();
                JSONObject jsonObject = JSON.parseObject(str);
                StringBuilder lineBuilder = new StringBuilder();
                for (int i = 0; i < fieldSchema.length; i++) {
                    lineBuilder.append(jsonObject.getString(fieldSchema[i])).append(fieldSeparator);
                }
                lineBuilder.append(System.lineSeparator());
                byte[] line = lineBuilder.toString().getBytes();
                outputStream.write(line, 0, line.length);
            }
        } catch (IOException e) {
            log.error("sink task ioexception", e);
        }
    }

    @Override public void stop() {
        sftpClient.close();
    }
}
