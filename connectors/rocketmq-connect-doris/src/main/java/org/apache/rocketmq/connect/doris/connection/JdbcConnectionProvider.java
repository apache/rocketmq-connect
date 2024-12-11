/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.rocketmq.connect.doris.connection;

import java.io.Serializable;
import java.net.Authenticator;
import java.net.PasswordAuthentication;
import java.net.Proxy;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.NoSuchElementException;
import java.util.Objects;
import org.apache.rocketmq.connect.doris.cfg.DorisOptions;
import org.apache.rocketmq.connect.doris.cfg.DorisOptions.ProxyConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JdbcConnectionProvider implements ConnectionProvider, Serializable {

    private static final Logger LOG = LoggerFactory.getLogger(JdbcConnectionProvider.class);
    protected final String driverName = "com.mysql.jdbc.Driver";
    protected final String cjDriverName = "com.mysql.cj.jdbc.Driver";
    private static final String JDBC_URL_TEMPLATE = "jdbc:mysql://%s";
    private static final String JDBC_URL_PROXY_TEMPLATE = "jdbc:mysql://%s?useSSL=false&proxyHost=%s&proxyPort=%d";

    private static final long serialVersionUID = 1L;

    private final DorisOptions options;

    private transient Connection connection;

    public JdbcConnectionProvider(DorisOptions options) {
        this.options = options;
    }

    @Override
    public Connection getOrEstablishConnection() throws ClassNotFoundException, SQLException {
        if (connection != null && !connection.isClosed() && connection.isValid(10)) {
            return connection;
        }
        try {
            Class.forName(cjDriverName);
        } catch (ClassNotFoundException ex) {
            LOG.warn(
                "can not found class com.mysql.cj.jdbc.Driver, use class com.mysql.jdbc.Driver");
            Class.forName(driverName);
        }
        String jdbcUrl;
        if (options.customCluster()) {
            ProxyConfig proxyConfig = options.getProxyConfig()
                .orElseThrow(() -> new NoSuchElementException("Failed to get ProxyConfig."));
            String proxyHost = proxyConfig.getSocks5Host();
            int proxyPort = proxyConfig.getSocks5Port();
            String queryUrl = options.getQueryUrl();
            jdbcUrl = String.format(JDBC_URL_PROXY_TEMPLATE, queryUrl, proxyHost, proxyPort);
            configureSocks5ProxyAuthentication();
        } else {
            jdbcUrl = String.format(JDBC_URL_TEMPLATE, options.getQueryUrl());
        }

        if (!Objects.isNull(options.getUser())) {
            connection =
                DriverManager.getConnection(jdbcUrl, options.getUser(), options.getPassword());
        } else {
            connection = DriverManager.getConnection(jdbcUrl);
        }
        return connection;
    }

    private void configureSocks5ProxyAuthentication() {
        if (options.customCluster()) {
            ProxyConfig proxyConfig = options.getProxyConfig()
                .orElseThrow(() -> new NoSuchElementException("Failed to get ProxyConfig."));
            String socks5UserName = proxyConfig.getSocks5UserName();
            String socks5Password = proxyConfig.getSocks5Password();
            Authenticator.setDefault(new Authenticator() {
                @Override
                protected PasswordAuthentication getPasswordAuthentication() {
                    if (Proxy.Type.SOCKS.name().equalsIgnoreCase(getRequestingProtocol())) {
                        return new PasswordAuthentication(socks5UserName, socks5Password.toCharArray());
                    }
                    return null;
                }
            });
            System.setProperty("java.net.socks.username", socks5UserName);
            System.setProperty("java.net.socks.password", socks5Password);
        }
    }

    @Override
    public void closeConnection() {
        if (connection != null) {
            try {
                connection.close();
            } catch (SQLException e) {
                LOG.warn("JDBC connection close failed.", e);
            } finally {
                connection = null;
            }
        }
    }
}
