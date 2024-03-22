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

import com.google.common.base.Strings;
import lombok.SneakyThrows;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.text.MessageFormat;
import java.util.concurrent.Callable;

public class HttpRequestCallable implements Callable<String> {

    private static final Logger log = LoggerFactory.getLogger(HttpCallback.class);

    private CloseableHttpClient httpclient;
    private HttpCallback httpCallback;
    private HttpUriRequest httpUriRequest;
    private HttpClientContext context;
    private SocksProxyConfig socksProxyConfig;
    private static final String SOCKS_ADDRESS_KEY = "socks.address";

    public HttpRequestCallable(CloseableHttpClient httpclient, HttpUriRequest httpUriRequest, HttpClientContext context,
                               SocksProxyConfig socksProxyConfig, HttpCallback httpCallback) {
        this.httpclient = httpclient;
        this.httpUriRequest = httpUriRequest;
        this.context = context;
        this.socksProxyConfig = socksProxyConfig;
        this.httpCallback = httpCallback;
    }

    public void loadSocks5ProxyConfig() {
        if (!Strings.isNullOrEmpty(socksProxyConfig.getSocks5Endpoint())) {
            String[] socksAddrAndPor = socksProxyConfig.getSocks5Endpoint()
                    .split(":");
            InetSocketAddress socksaddr = new InetSocketAddress(socksAddrAndPor[0],
                    Integer.parseInt(socksAddrAndPor[1]));
            context.setAttribute(SOCKS_ADDRESS_KEY, socksaddr);
            ThreadLocalProxyAuthenticator.getInstance()
                    .setCredentials(socksProxyConfig.getSocks5UserName(), socksProxyConfig.getSocks5Password());
        }
    }

    @SneakyThrows
    @Override
    public String call() throws Exception {
        CloseableHttpResponse response = null;
        try {
            Long startTime = System.currentTimeMillis();
            loadSocks5ProxyConfig();
            response = httpclient.execute(httpUriRequest, context);
            if (response != null && response.getEntity() != null) {
                String result = EntityUtils.toString(response.getEntity(), StandardCharsets.UTF_8);
                if (response.getStatusLine()
                        .getStatusCode() / 100 != 2) {
                    String msg = MessageFormat.format("Http Status:{0},Msg:{1}", response.getStatusLine()
                            .getStatusCode(), result);
                    httpCallback.setMsg(msg);
                    httpCallback.setFailed(Boolean.TRUE);
                }
                log.info("The cost of one http request:{}, Connection Connection={}, Keep-Alive={}",
                        System.currentTimeMillis() - startTime, response.getHeaders("Connection"),
                        response.getHeaders("Keep-Alive"));
                return result;
            }
        } catch (Throwable e) {
            log.error("http execute failed.", e);
            httpCallback.setFailed(Boolean.TRUE);
            httpCallback.setMsg(e.getLocalizedMessage());
        } finally {
            httpCallback.getCountDownLatch()
                    .countDown();
            if (null != response.getEntity()) {
                EntityUtils.consume(response.getEntity());
            }
        }
        return null;
    }
}
