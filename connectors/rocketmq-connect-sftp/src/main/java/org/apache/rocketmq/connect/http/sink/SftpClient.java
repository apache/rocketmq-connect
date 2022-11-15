package org.apache.rocketmq.connect.http.sink;

import com.jcraft.jsch.*;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;

public class SftpClient implements Closeable {

    private String host;

    private int port;

    private String username;

    private String password;

    private String path;

    private Session session;

    private ChannelSftp channel;

    public SftpClient(String host, int port, String username, String password, String path) {
        this.host = host;
        this.port = port;
        this.username = username;
        this.password = password;
        this.path = path;
    }

    public InputStream get(String filename) throws IOException {
        JSch jsch = new JSch();
        try {
            session = jsch.getSession(username, host, port);
            session.setConfig("StrictHostKeyChecking", "no");
            session.setPassword(password);
            session.connect(30000);
            channel = (ChannelSftp) session.openChannel("sftp");
            channel.connect(30000);
            channel.cd(path);
            return channel.get(filename);
        } catch (JSchException | SftpException e) {
            throw new IOException(e);
        }
    }

    public void append(InputStream inputStream, String filename) throws IOException {
        JSch jsch = new JSch();
        try {
            session = jsch.getSession(username, host, port);
            session.setConfig("StrictHostKeyChecking", "no");
            session.setPassword(password);
            session.connect(30000);
            channel = (ChannelSftp) session.openChannel("sftp");
            channel.connect(30000);
            channel.cd(path);
            channel.put(inputStream, filename, ChannelSftp.APPEND);
        } catch (JSchException | SftpException e) {
            throw new IOException(e);
        }
    }

    @Override
    public void close() {
        if (channel != null) {
            channel.disconnect();
        }
        if (session != null) {
            session.disconnect();
        }
    }
}