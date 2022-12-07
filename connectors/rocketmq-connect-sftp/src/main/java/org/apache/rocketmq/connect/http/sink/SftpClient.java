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

import java.io.Closeable;
import java.io.IOException;
import java.util.Set;
import net.schmizz.sshj.SSHClient;
import net.schmizz.sshj.sftp.OpenMode;
import net.schmizz.sshj.sftp.RemoteFile;
import net.schmizz.sshj.sftp.SFTPClient;
import net.schmizz.sshj.transport.TransportException;
import net.schmizz.sshj.transport.verification.PromiscuousVerifier;
import net.schmizz.sshj.userauth.UserAuthException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SftpClient implements Closeable {

    private static final Logger log = LoggerFactory.getLogger(SftpClient.class);

    private SSHClient sshClient;

    private SFTPClient internalSFTPClient;

    public SftpClient(String host, int port, String username, String password) {
        sshClient = new SSHClient();
        sshClient.addHostKeyVerifier(new PromiscuousVerifier());
        try {
            sshClient.connect(host, port);
            sshClient.authPassword(username, password);
            internalSFTPClient = sshClient.newSFTPClient();
        } catch (UserAuthException e) {
            log.error(e.getMessage(), e);
        } catch (TransportException e) {
            log.error(e.getMessage(), e);
        } catch (IOException e) {
            log.error(e.getMessage(), e);
        }
    }

    public RemoteFile open(String filename) throws IOException {
        return internalSFTPClient.getSFTPEngine().open(filename);
    }

    public RemoteFile open(String filename, Set<OpenMode> modes) throws IOException {
        return internalSFTPClient.getSFTPEngine().open(filename, modes);
    }

    @Override
    public void close() {
        try {
            internalSFTPClient.close();
            sshClient.close();
        } catch (IOException e) {
            log.error(e.getMessage(), e);
        }
    }
}