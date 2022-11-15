package org.apache.rocketmq.connect.http.sink;

import static org.apache.rocketmq.connect.http.sink.SftpConstant.SFTP_FILENAME_KEY;
import static org.apache.rocketmq.connect.http.sink.SftpConstant.SFTP_HOST_KEY;
import static org.apache.rocketmq.connect.http.sink.SftpConstant.SFTP_PASSWORD_KEY;
import static org.apache.rocketmq.connect.http.sink.SftpConstant.SFTP_PATH_KEY;
import static org.apache.rocketmq.connect.http.sink.SftpConstant.SFTP_PORT_KEY;
import static org.apache.rocketmq.connect.http.sink.SftpConstant.SFTP_USERNAME_KEY;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.file.FileSystemException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.openmessaging.KeyValue;
import io.openmessaging.connector.api.component.task.source.SourceTask;
import io.openmessaging.connector.api.component.task.source.SourceTaskContext;
import io.openmessaging.connector.api.data.ConnectRecord;
import io.openmessaging.connector.api.data.RecordOffset;
import io.openmessaging.connector.api.data.RecordPartition;

public class SftpSourceTask extends SourceTask {

    private final Logger log = LoggerFactory.getLogger(SftpConstant.LOGGER_NAME);

    private SftpClient sftpClient;

    private String filename;

    @Override
    public void init(SourceTaskContext sourceTaskContext) {
        super.init(sourceTaskContext);
    }

    @Override
    public void start(KeyValue keyValue) {
        String host = keyValue.getString(SFTP_HOST_KEY);
        int port = keyValue.getInt(SFTP_PORT_KEY);
        String username = keyValue.getString(SFTP_USERNAME_KEY);
        String password = keyValue.getString(SFTP_PASSWORD_KEY);
        String path = keyValue.getString(SFTP_PATH_KEY);
        filename = keyValue.getString(SFTP_FILENAME_KEY);
        sftpClient = new SftpClient(host, port, username, password, path);
    }

    @Override
    public void stop() {
    }

    @Override
    public List<ConnectRecord> poll() {
        try (InputStream inputStream = sftpClient.get(filename);
             BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream))) {
            List<ConnectRecord> records = new ArrayList<>();
            String line;
            ConnectRecord connectRecord;

            while ((line = reader.readLine()) != null) {
                connectRecord = new ConnectRecord(buildRecordPartition(), buildRecordOffset(), System.currentTimeMillis());
                connectRecord.setData(line);
                records.add(connectRecord);
            }
            return records;
        } catch (FileSystemException e) {
            log.error("File system error", e);
        } catch (IOException e) {
            log.error("SFTP IOException", e);
        } finally {
            sftpClient.close();
        }
        return null;
    }

    private RecordOffset buildRecordOffset()  {
        Map<String, Long> offsetMap = new HashMap<>();
        return new RecordOffset(offsetMap);
    }

    private RecordPartition buildRecordPartition() {
        Map<String, String> partitionMap = new HashMap<>();
        partitionMap.put("partition", "defaultPartition");
        RecordPartition  recordPartition = new RecordPartition(partitionMap);
        return recordPartition;
    }

    @Override
    public void validate(KeyValue config) {
        if (StringUtils.isBlank(config.getString(SFTP_HOST_KEY))
         || StringUtils.isBlank(config.getString(SFTP_PORT_KEY))
                || StringUtils.isBlank(config.getString(SFTP_USERNAME_KEY))
                || StringUtils.isBlank(config.getString(SFTP_PASSWORD_KEY))
                || StringUtils.isBlank(config.getString(SFTP_PATH_KEY))
                || StringUtils.isBlank(config.getString(SFTP_FILENAME_KEY))) {
            throw new RuntimeException("missing required config");
        }
    }
}
