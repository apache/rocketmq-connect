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
package org.apache.rocketmq.connect.http.auth;

public class SocksProxyConfig {

    private String socks5Endpoint;
    private String socks5UserName;
    private String socks5Password;

    public SocksProxyConfig(String socks5Endpoint, String socks5UserName, String socks5Password) {
        this.socks5Endpoint = socks5Endpoint;
        this.socks5UserName = socks5UserName;
        this.socks5Password = socks5Password;
    }

    public String getSocks5Endpoint() {
        return socks5Endpoint;
    }

    public void setSocks5Endpoint(String socks5Endpoint) {
        this.socks5Endpoint = socks5Endpoint;
    }

    public String getSocks5UserName() {
        return socks5UserName;
    }

    public void setSocks5UserName(String socks5UserName) {
        this.socks5UserName = socks5UserName;
    }

    public String getSocks5Password() {
        return socks5Password;
    }

    public void setSocks5Password(String socks5Password) {
        this.socks5Password = socks5Password;
    }
}
