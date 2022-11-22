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

import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;
import com.jcraft.jsch.SftpException;
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