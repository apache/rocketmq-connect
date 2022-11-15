package org.apache.rocketmq.connect.http.sink;

import io.openmessaging.KeyValue;
import io.openmessaging.connector.api.component.task.sink.SinkTask;
import io.openmessaging.connector.api.data.ConnectRecord;
import io.openmessaging.connector.api.errors.ConnectException;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.nio.file.FileSystemException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;

public class SftpSinkTask extends SinkTask {
    private static final Logger log = LoggerFactory.getLogger(SftpSinkTask.class);

    private SftpClient sftpClient;

    private String filename;

    @Override
    public void put(List<ConnectRecord> sinkRecords) throws ConnectException {
        try (BufferedOutputStream bufferedOutputStream = new BufferedOutputStream(Files.newOutputStream(Paths.get(filename)))) {
            for (ConnectRecord connectRecord : sinkRecords) {
                byte[] objectBytes = parseObject(connectRecord.getData());
                bufferedOutputStream.write(objectBytes);
            }
        } catch (FileSystemException e) {
            log.error("File system error", e);
        } catch (IOException e) {
            log.error("SFTP IOException", e);
        } finally {
            sftpClient.close();
        }
    }

    private byte[] parseObject(Object data) {
        if(data instanceof String) {
            return ((String) data).getBytes();
        } else if(data instanceof byte[]) {
            return (byte[]) data;
        } else {
            throw new IllegalArgumentException();
        }
    }


    @Override
    public void validate(KeyValue config) {
        if (StringUtils.isBlank(config.getString("filename"))) {
            throw new IllegalArgumentException();
        }
    }

    @Override
    public void start(KeyValue config) {
        String host = config.getString("host");
        int port = config.getInt("port");
        String username = config.getString("username");
        String password = config.getString("password");
        String path = config.getString("path");
        filename = config.getString("filename");
        sftpClient = new SftpClient(host, port, username, password, path);
    }

    @Override
    public void stop() {

    }
}
