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

package org.apache.rocketmq.connect.doris.utils;

import java.util.NoSuchElementException;
import java.util.Objects;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.DefaultRedirectStrategy;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.rocketmq.connect.doris.cfg.DorisOptions;
import org.apache.rocketmq.connect.doris.cfg.DorisOptions.ProxyConfig;

/**
 * util to build http client.
 */
public class HttpUtils {
    private final DorisOptions dorisOptions;
    private final ProxyConfig proxyConfig;

    public HttpUtils(DorisOptions dorisOptions) {
        this.dorisOptions = dorisOptions;
        this.proxyConfig = dorisOptions.getProxyConfig()
            .orElseThrow(() -> new NoSuchElementException("Failed to get ProxyConfig."));
    }

    private final HttpClientBuilder httpClientBuilder =
        HttpClients.custom()
            .setRedirectStrategy(
                new DefaultRedirectStrategy() {
                    @Override
                    protected boolean isRedirectable(String method) {
                        return true;
                    }
                })
            .setDefaultRequestConfig(createRequestConfigWithProxy())
            .setDefaultCredentialsProvider(createCredentialsProvider());

    private RequestConfig createRequestConfigWithProxy() {
        if (Objects.requireNonNull(dorisOptions).customCluster()) {
            String socksProxyHost = proxyConfig.getSocks5Host();
            int socksProxyPort = proxyConfig.getSocks5Port();           // Socks5 代理端口
            HttpHost proxy = new HttpHost(socksProxyHost, socksProxyPort);
            return RequestConfig.custom()
                .setProxy(proxy)
                .build();
        } else {
            return RequestConfig.custom().build();
        }
    }

    private CredentialsProvider createCredentialsProvider() {
        CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        if (Objects.requireNonNull(dorisOptions).customCluster()) {
            credentialsProvider.setCredentials(
                new AuthScope(proxyConfig.getSocks5Host(), proxyConfig.getSocks5Port()),
                new UsernamePasswordCredentials(proxyConfig.getSocks5UserName(), proxyConfig.getSocks5Password())
            );
        }
        return credentialsProvider;
    }

    public CloseableHttpClient getHttpClient() {
        return httpClientBuilder.build();
    }
}
