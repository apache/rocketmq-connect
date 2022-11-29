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

import com.alibaba.fastjson.JSONObject;
import io.openmessaging.KeyValue;
import io.openmessaging.connector.api.component.task.source.SourceTask;
import io.openmessaging.connector.api.component.task.source.SourceTaskContext;
import io.openmessaging.connector.api.data.ConnectRecord;
import io.openmessaging.connector.api.data.RecordOffset;
import io.openmessaging.connector.api.data.RecordPartition;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.FileSystemException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import net.schmizz.sshj.sftp.RemoteFile;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.rocketmq.connect.http.sink.SftpConstant.RECORD_OFFSET_STORAGE_KEY;
import static org.apache.rocketmq.connect.http.sink.SftpConstant.RECORD_PARTITION_STORAGE_KEY;
import static org.apache.rocketmq.connect.http.sink.SftpConstant.SFTP_FIELD_SCHEMA;
import static org.apache.rocketmq.connect.http.sink.SftpConstant.SFTP_FIELD_SEPARATOR;
import static org.apache.rocketmq.connect.http.sink.SftpConstant.SFTP_HOST_KEY;
import static org.apache.rocketmq.connect.http.sink.SftpConstant.SFTP_PASSWORD_KEY;
import static org.apache.rocketmq.connect.http.sink.SftpConstant.SFTP_PATH_KEY;
import static org.apache.rocketmq.connect.http.sink.SftpConstant.SFTP_PORT_KEY;
import static org.apache.rocketmq.connect.http.sink.SftpConstant.SFTP_USERNAME_KEY;

public class SftpSourceTask extends SourceTask {

    private final Logger log = LoggerFactory.getLogger(SftpConstant.LOGGER_NAME);

    private SftpClient sftpClient;

    private String filePath;

    private String fieldSeparator;

    private String[] fieldSchema;

    private static final int MAX_NUMBER_SEND_CONNECT_RECORD_EACH_TIME = 2000;

    @Override public void init(SourceTaskContext sourceTaskContext) {
        super.init(sourceTaskContext);
    }

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

    @Override public void stop() {
        sftpClient.close();
    }

    @Override public List<ConnectRecord> poll() {
        int offset = readRecordOffset();
        try (RemoteFile remoteFile = sftpClient.open(filePath);
             BufferedReader reader = new BufferedReader(new InputStreamReader(remoteFile.new RemoteFileInputStream(offset)))) {
            List<ConnectRecord> records = new ArrayList<>();
            String line;
            ConnectRecord connectRecord;

            while ((line = reader.readLine()) != null) {
                offset = offset + line.getBytes().length + 1;

                // do not send empty string to mq
                if (!StringUtils.isEmpty(line)) {
                    String[] data = line.split(Pattern.quote(fieldSeparator));
                    JSONObject jsonObject = new JSONObject();
                    for (int i = 0; i < fieldSchema.length; i++) {
                        jsonObject.put(fieldSchema[i], data[i]);
                    }
                    connectRecord = new ConnectRecord(buildRecordPartition(filePath), buildRecordOffset(offset), System.currentTimeMillis());

                    connectRecord.setData(jsonObject.toString());
                    records.add(connectRecord);
                    if (records.size() > MAX_NUMBER_SEND_CONNECT_RECORD_EACH_TIME) {
                        break;
                    }
                }
            }
            return records;
        } catch (FileSystemException e) {
            log.error("File system error", e);
        } catch (IOException e) {
            log.error("SFTP IOException", e);
        }
        return null;
    }

    private RecordOffset buildRecordOffset(int offset) {
        Map<String, Integer> offsetMap = new HashMap<>();
        offsetMap.put(RECORD_OFFSET_STORAGE_KEY, offset);
        return new RecordOffset(offsetMap);
    }

    private RecordPartition buildRecordPartition(String partitionValue) {
        Map<String, String> partitionMap = new HashMap<>();
        partitionMap.put(RECORD_PARTITION_STORAGE_KEY, partitionValue);
        return new RecordPartition(partitionMap);
    }

    private int readRecordOffset() {
        RecordOffset positionInfo = this.sourceTaskContext.offsetStorageReader().readOffset(buildRecordPartition(filePath));
        if (positionInfo == null) {
            return 0;
        }
        Object offset = positionInfo.getOffset().get(RECORD_OFFSET_STORAGE_KEY);
        if (offset == null) {
            return 0;
        } else {
            return (int) offset;
        }
    }
}
