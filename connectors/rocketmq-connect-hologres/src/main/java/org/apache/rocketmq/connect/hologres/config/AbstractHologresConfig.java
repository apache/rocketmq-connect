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

package org.apache.rocketmq.connect.hologres.config;

import io.openmessaging.KeyValue;

import java.util.HashSet;
import java.util.Set;

public class AbstractHologresConfig {

    public static final Set<String> REQUEST_CONFIG = new HashSet<String>(){
        {
            add(HologresConstant.HOST);
            add(HologresConstant.PORT);
            add(HologresConstant.DATABASE);
        }
    };

    private KeyValue keyValue;
    private String host;
    private int port;
    private String username;
    private String password;
    private String database;
    private String table;

    public AbstractHologresConfig(KeyValue keyValue) {
        this.keyValue = keyValue;
        ConfigUtils.load(keyValue, this);
    }

    public KeyValue getKeyValue() {
        return keyValue;
    }

    public void setKeyValue(KeyValue keyValue) {
        this.keyValue = keyValue;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getDatabase() {
        return database;
    }

    public void setDatabase(String database) {
        this.database = database;
    }

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public String getJdbcUrl() {
        return String.format("%s%s:%d/%s", HologresConstant.URL_PREFIX, getHost(), getPort(), getDatabase());
    }
}
