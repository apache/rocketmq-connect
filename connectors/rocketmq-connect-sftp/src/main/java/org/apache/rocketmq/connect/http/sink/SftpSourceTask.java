package org.apache.rocketmq.connect.http.sink;

import io.openmessaging.KeyValue;
import io.openmessaging.connector.api.component.task.source.SourceTask;
import io.openmessaging.connector.api.component.task.source.SourceTaskContext;
import io.openmessaging.connector.api.data.ConnectRecord;
import io.openmessaging.connector.api.data.RecordOffset;
import io.openmessaging.connector.api.data.RecordPartition;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.file.FileSystemException;
import java.util.*;

import static org.apache.rocketmq.connect.http.sink.SftpConstant.*;

public class SftpSourceTask extends SourceTask {

    private final Logger log = LoggerFactory.getLogger(SftpConstant.LOGGER_NAME);

    private SftpClient sftpClient;

    private String filename;

    private static final int MAX_NUMBER_SEND_CONNECT_RECORD_EACH_TIME = 2000;

    @Override
    public void init(SourceTaskContext sourceTaskContext) {
        super.init(sourceTaskContext);
    }

    @Override
    public void start(KeyValue config) {
        String host = config.getString(SFTP_HOST_KEY);
        int port = config.getInt(SFTP_PORT_KEY);
        String username = config.getString(SFTP_USERNAME_KEY);
        String password = config.getString(SFTP_PASSWORD_KEY);
        String path = config.getString(SFTP_PATH_KEY);
        filename = config.getString(SFTP_FILENAME_KEY);
        sftpClient = new SftpClient(host, port, username, password, path);
    }

    @Override
    public void stop() {
    }

    @Override
    public List<ConnectRecord> poll() {
        int offset = readRecordOffset();
        try (InputStream inputStream = sftpClient.get(filename);
                BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream))) {
            inputStream.skip(offset);
            List<ConnectRecord> records = new ArrayList<>();
            String line;
            ConnectRecord connectRecord;

            while ((line = reader.readLine()) != null) {
                offset = offset + line.getBytes().length + 1;
                connectRecord = new ConnectRecord(buildRecordPartition(filename), buildRecordOffset(offset),
                        System.currentTimeMillis());
                connectRecord.setData(line);
                records.add(connectRecord);
                if (records.size() > MAX_NUMBER_SEND_CONNECT_RECORD_EACH_TIME) {
                    break;
                }
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
        RecordOffset positionInfo = this.sourceTaskContext.offsetStorageReader()
                .readOffset(buildRecordPartition(filename));
        if(positionInfo == null) {
            return 0;
        }
        Object offset = positionInfo.getOffset().get(RECORD_OFFSET_STORAGE_KEY);
        if(offset == null) {
            return 0;
        } else {
            return (int) offset;
        }
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
