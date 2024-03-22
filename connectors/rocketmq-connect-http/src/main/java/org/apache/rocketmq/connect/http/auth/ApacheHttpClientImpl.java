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

import com.google.common.net.MediaType;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import io.netty.util.concurrent.DefaultThreadFactory;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.NameValuePair;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpOptions;
import org.apache.http.client.methods.HttpPatch;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.config.Registry;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.conn.DnsResolver;
import org.apache.http.conn.socket.ConnectionSocketFactory;
import org.apache.http.conn.socket.PlainConnectionSocketFactory;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.conn.ssl.TrustStrategy;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.entity.mime.MultipartEntityBuilder;
import org.apache.http.entity.mime.content.StringBody;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.protocol.HTTP;
import org.apache.http.protocol.HttpContext;
import org.apache.http.ssl.SSLContextBuilder;
import org.apache.rocketmq.connect.http.constant.HttpConstant;
import org.apache.rocketmq.connect.http.entity.ProxyConfig;
import org.apache.rocketmq.connect.http.entity.HttpDeleteWithEntity;
import org.apache.rocketmq.connect.http.entity.HttpGetWithEntity;
import org.apache.rocketmq.connect.http.entity.HttpHeadWithEntity;
import org.apache.rocketmq.connect.http.entity.HttpRequest;
import org.apache.rocketmq.connect.http.entity.HttpTraceWithEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSession;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Proxy;
import java.net.Socket;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class ApacheHttpClientImpl implements AbstractHttpClient {
    private static final Logger log = LoggerFactory.getLogger(ApacheHttpClientImpl.class);

    private static ExecutorService executorServicePool;
    private CloseableHttpClient httpClient = null;
    private SocksProxyConfig socksProxyConfig;
    private static final String SOCKS_ADDRESS_KEY = "socks.address";

    public ApacheHttpClientImpl() {
        executorServicePool = new ThreadPoolExecutor(200, 2000, 600, TimeUnit.SECONDS,
                new LinkedBlockingDeque<Runnable>(1000), new DefaultThreadFactory("ApacheHttpClientRequestThread"));
    }

    public ApacheHttpClientImpl(Integer concurrency) {
        executorServicePool = new ThreadPoolExecutor(concurrency, concurrency, 600, TimeUnit.SECONDS,
                new LinkedBlockingDeque<Runnable>(1000), new DefaultThreadFactory("ApacheHttpClientRequestThread"));
    }

    @Override
    public void init(ProxyConfig config) {
        try {
            SSLContextBuilder sslContextBuilder = new SSLContextBuilder().loadTrustMaterial(null, new TrustStrategy() {
                @Override
                public boolean isTrusted(X509Certificate[] chain, String authType) {
                    return true;
                }
            });
            Registry<ConnectionSocketFactory> reg = RegistryBuilder.<ConnectionSocketFactory>create().register("http",
                            new SocksPlainConnectionSocketFactory())
                    .register("https", new SocksSSLConnectionSocketFactory(sslContextBuilder.build()))
                    .build();
            PoolingHttpClientConnectionManager connManager = new PoolingHttpClientConnectionManager(reg,
                    new FakeDnsResolver());
            connManager.setMaxTotal(400);
            connManager.setDefaultMaxPerRoute(500);
            httpClient = HttpClients.custom()
                    .setConnectionManager(connManager)
                    .build();

            this.socksProxyConfig = new SocksProxyConfig(config.getSocks5Endpoint(), config.getSocks5UserName(), config.getSocks5Password());
        } catch (Exception e) {
            log.error("ApacheHttpClientImpl | init | error => ", e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public String execute(HttpRequest httpRequest, HttpCallback httpCallback) throws Exception {
        HttpRequestCallable httpRequestCallable = getHttpRequestCallable(httpRequest, httpCallback);
        Future<String> submit = executorServicePool.submit(httpRequestCallable);
        String result = submit.get();
        log.info("ApacheHttpClientImpl | execute | success | result : {}", result);
        return result;
    }

    @Override
    public void executeNotReturn(HttpRequest httpRequest, HttpCallback httpCallback) throws Exception {
        HttpRequestCallable httpRequestCallable = getHttpRequestCallable(httpRequest, httpCallback);
        executorServicePool.submit(httpRequestCallable);
    }

    private HttpRequestCallable getHttpRequestCallable(HttpRequest httpRequest, HttpCallback httpCallback) throws UnsupportedEncodingException {
        CloseableHttpResponse response;
        HttpRequestBase httpRequestBase = null;
        if (httpRequest != null) {
            if (!StringUtils.isEmpty(httpRequest.getBody())) {
                httpRequestBase = extracted(httpRequest.getUrl(), httpRequest.getMethod(), httpRequest.getHeaderMap(), httpRequest.getBody());
            } else if (httpRequest.getBytesBody() != null && httpRequest.getBytesBody().length != 0) {
                httpRequestBase = extracted(httpRequest.getUrl(), httpRequest.getMethod(), httpRequest.getHeaderMap(), httpRequest.getBytesBody());
            }
            if (StringUtils.isNotBlank(httpRequest.getTimeout())) {
                final RequestConfig requestConfig = RequestConfig.custom().
                        setConnectionRequestTimeout(Integer.parseInt(httpRequest.getTimeout())).
                        setSocketTimeout(Integer.parseInt(httpRequest.getTimeout())).
                        setConnectTimeout(Integer.parseInt(httpRequest.getTimeout())).build();
                httpRequestBase.setConfig(requestConfig);
            }
        }

        HttpRequestCallable httpRequestCallable = new HttpRequestCallable(httpClient, httpRequestBase,
                HttpClientContext.create(), this.socksProxyConfig, httpCallback);
        return httpRequestCallable;
    }

    private HttpRequestBase extracted(String url, String method, Map<String, String> headerMap, String body) throws UnsupportedEncodingException {
        switch (method) {
            case "GET":
                return getHttpGetWithEntity(url, headerMap, body);
            case "POST":
                return getHttpPost(url, headerMap, body);
            case "DELETE":
                return getHttpDeleteWithEntity(url, headerMap, body);
            case "PUT":
                return getHttpPut(url, headerMap, body);
            case "HEAD":
                return getHttpHeadWithEntity(url, headerMap, body);
            case "TRACE":
                return getHttpTraceWithEntity(url, headerMap, body);
            case "PATCH":
                return getHttpPatch(url, headerMap, body);
            default:
        }
        HttpOptions httpOptions = new HttpOptions(url);
        headerMap.forEach(httpOptions::addHeader);
        httpOptions.addHeader(HTTP.CONTENT_TYPE, MediaType.JSON_UTF_8.toString());
        return httpOptions;
    }

    private HttpRequestBase extracted(String url, String method, Map<String, String> headerMap, byte[] bytesBody) throws UnsupportedEncodingException {
        switch (method) {
            case "POST":
                return getHttpPostByte(url, headerMap, bytesBody);
            default:
                throw new UnsupportedEncodingException();
        }
    }

    private HttpTraceWithEntity getHttpTraceWithEntity(String url, Map<String, String> headerMap, String body) {
        HttpTraceWithEntity httpTraceWithEntity = new HttpTraceWithEntity(url);
        headerMap.forEach(httpTraceWithEntity::addHeader);
        httpTraceWithEntity.addHeader(HTTP.CONN_DIRECTIVE, HTTP.CONN_KEEP_ALIVE);
        String contentType = headerMap.get(HTTP.CONTENT_TYPE);
        if (StringUtils.isNotBlank(body)
                && StringUtils.isNotBlank(contentType)
                && (contentType.contains(ContentType.APPLICATION_JSON.getMimeType())
                || contentType.contains(HttpConstant.CONTENT_TYPE_APPLICATION_JAVASCRIPT)
                || contentType.contains(ContentType.APPLICATION_XML.getMimeType())
                || contentType.contains(ContentType.TEXT_PLAIN.getMimeType())
                || contentType.contains(ContentType.TEXT_XML.getMimeType())
                || contentType.contains(ContentType.TEXT_HTML.getMimeType()))) {
            HttpEntity entityPot = new StringEntity(body, MediaType.JSON_UTF_8.charset()
                    .get());
            httpTraceWithEntity.setEntity(entityPot);
        }
        if (StringUtils.isNotBlank(body)
                && StringUtils.isNotBlank(contentType)
                && contentType.contains(ContentType.APPLICATION_FORM_URLENCODED.getMimeType())) {
            UrlEncodedFormEntity urlEncodedFormEntity = getUrlEncodedFormEntity(body);
            httpTraceWithEntity.setEntity(urlEncodedFormEntity);
        }
        if (StringUtils.isNotBlank(body)
                && StringUtils.isNotBlank(contentType)
                && contentType.contains(ContentType.MULTIPART_FORM_DATA.getMimeType())) {
            MultipartEntityBuilder multipartEntityBuilder = getMultipartEntityBuilder(body);
            httpTraceWithEntity.setEntity(multipartEntityBuilder.build());
        }
        if (StringUtils.isNotBlank(body)
                && StringUtils.isBlank(contentType)) {
            HttpEntity entityPot = new StringEntity(body, MediaType.JSON_UTF_8.charset()
                    .get());
            httpTraceWithEntity.setEntity(entityPot);
        }
        return httpTraceWithEntity;
    }

    private HttpHeadWithEntity getHttpHeadWithEntity(String url, Map<String, String> headerMap, String body) {
        HttpHeadWithEntity httpHeadWithEntity = new HttpHeadWithEntity(url);
        headerMap.forEach(httpHeadWithEntity::addHeader);
        httpHeadWithEntity.addHeader(HTTP.CONN_DIRECTIVE, HTTP.CONN_KEEP_ALIVE);
        String contentType = headerMap.get(HTTP.CONTENT_TYPE);
        if (StringUtils.isNotBlank(body)
                && StringUtils.isNotBlank(contentType)
                && (contentType.contains(ContentType.APPLICATION_JSON.getMimeType())
                || contentType.contains(HttpConstant.CONTENT_TYPE_APPLICATION_JAVASCRIPT)
                || contentType.contains(ContentType.APPLICATION_XML.getMimeType())
                || contentType.contains(ContentType.TEXT_PLAIN.getMimeType())
                || contentType.contains(ContentType.TEXT_XML.getMimeType())
                || contentType.contains(ContentType.TEXT_HTML.getMimeType()))) {
            HttpEntity entityPot = new StringEntity(body, MediaType.JSON_UTF_8.charset()
                    .get());
            httpHeadWithEntity.setEntity(entityPot);
        }
        if (StringUtils.isNotBlank(body)
                && StringUtils.isNotBlank(contentType)
                && contentType.contains(ContentType.APPLICATION_FORM_URLENCODED.getMimeType())) {
            UrlEncodedFormEntity urlEncodedFormEntity = getUrlEncodedFormEntity(body);
            httpHeadWithEntity.setEntity(urlEncodedFormEntity);
        }
        if (StringUtils.isNotBlank(body)
                && StringUtils.isNotBlank(contentType)
                && contentType.contains(ContentType.MULTIPART_FORM_DATA.getMimeType())) {
            MultipartEntityBuilder multipartEntityBuilder = getMultipartEntityBuilder(body);
            httpHeadWithEntity.setEntity(multipartEntityBuilder.build());
        }
        if (StringUtils.isNotBlank(body)
                && StringUtils.isBlank(contentType)) {
            HttpEntity entityPot = new StringEntity(body, MediaType.JSON_UTF_8.charset()
                    .get());
            httpHeadWithEntity.setEntity(entityPot);
        }
        return httpHeadWithEntity;
    }

    private HttpDeleteWithEntity getHttpDeleteWithEntity(String url, Map<String, String> headerMap, String body) {
        HttpDeleteWithEntity httpDeleteWithEntity = new HttpDeleteWithEntity(url);
        headerMap.forEach(httpDeleteWithEntity::addHeader);
        httpDeleteWithEntity.addHeader(HTTP.CONN_DIRECTIVE, HTTP.CONN_KEEP_ALIVE);
        String contentType = headerMap.get(HTTP.CONTENT_TYPE);
        if (StringUtils.isNotBlank(body)
                && StringUtils.isNotBlank(contentType)
                && (contentType.contains(ContentType.APPLICATION_JSON.getMimeType())
                || contentType.contains(HttpConstant.CONTENT_TYPE_APPLICATION_JAVASCRIPT)
                || contentType.contains(ContentType.APPLICATION_XML.getMimeType())
                || contentType.contains(ContentType.TEXT_PLAIN.getMimeType())
                || contentType.contains(ContentType.TEXT_XML.getMimeType())
                || contentType.contains(ContentType.TEXT_HTML.getMimeType()))) {
            HttpEntity entityPot = new StringEntity(body, MediaType.JSON_UTF_8.charset()
                    .get());
            httpDeleteWithEntity.setEntity(entityPot);
        }
        if (StringUtils.isNotBlank(body)
                && StringUtils.isNotBlank(contentType)
                && contentType.contains(ContentType.APPLICATION_FORM_URLENCODED.getMimeType())) {
            UrlEncodedFormEntity urlEncodedFormEntity = getUrlEncodedFormEntity(body);
            httpDeleteWithEntity.setEntity(urlEncodedFormEntity);
        }
        if (StringUtils.isNotBlank(body)
                && StringUtils.isNotBlank(contentType)
                && contentType.contains(ContentType.MULTIPART_FORM_DATA.getMimeType())) {
            MultipartEntityBuilder multipartEntityBuilder = getMultipartEntityBuilder(body);
            httpDeleteWithEntity.setEntity(multipartEntityBuilder.build());
        }
        if (StringUtils.isNotBlank(body)
                && StringUtils.isBlank(contentType)) {
            HttpEntity entityPot = new StringEntity(body, MediaType.JSON_UTF_8.charset()
                    .get());
            httpDeleteWithEntity.setEntity(entityPot);
        }
        return httpDeleteWithEntity;
    }

    private HttpGetWithEntity getHttpGetWithEntity(String url, Map<String, String> headerMap, String body) {
        HttpGetWithEntity httpGetWithEntity = new HttpGetWithEntity(url);
        headerMap.forEach(httpGetWithEntity::addHeader);
        httpGetWithEntity.addHeader(HTTP.CONN_DIRECTIVE, HTTP.CONN_KEEP_ALIVE);
        String contentType = headerMap.get(HTTP.CONTENT_TYPE);
        if (StringUtils.isNotBlank(body)
                && StringUtils.isNotBlank(contentType)
                && (contentType.contains(ContentType.APPLICATION_JSON.getMimeType())
                || contentType.contains(HttpConstant.CONTENT_TYPE_APPLICATION_JAVASCRIPT)
                || contentType.contains(ContentType.APPLICATION_XML.getMimeType())
                || contentType.contains(ContentType.TEXT_PLAIN.getMimeType())
                || contentType.contains(ContentType.TEXT_XML.getMimeType())
                || contentType.contains(ContentType.TEXT_HTML.getMimeType()))) {
            HttpEntity entityPot = new StringEntity(body, MediaType.JSON_UTF_8.charset()
                    .get());
            httpGetWithEntity.setEntity(entityPot);
        }
        if (StringUtils.isNotBlank(body)
                && StringUtils.isNotBlank(contentType)
                && contentType.contains(ContentType.APPLICATION_FORM_URLENCODED.getMimeType())) {
            UrlEncodedFormEntity urlEncodedFormEntity = getUrlEncodedFormEntity(body);
            httpGetWithEntity.setEntity(urlEncodedFormEntity);
        }
        if (StringUtils.isNotBlank(body)
                && StringUtils.isNotBlank(contentType)
                && contentType.contains(ContentType.MULTIPART_FORM_DATA.getMimeType())) {
            MultipartEntityBuilder multipartEntityBuilder = getMultipartEntityBuilder(body);
            httpGetWithEntity.setEntity(multipartEntityBuilder.build());
        }
        if (StringUtils.isNotBlank(body)
                && StringUtils.isBlank(contentType)) {
            HttpEntity entityPot = new StringEntity(body, MediaType.JSON_UTF_8.charset()
                    .get());
            httpGetWithEntity.setEntity(entityPot);
        }
        return httpGetWithEntity;
    }

    private HttpPut getHttpPut(String url, Map<String, String> headerMap, String body) {
        HttpPut httpPut = new HttpPut(url);
        headerMap.forEach(httpPut::addHeader);
        httpPut.addHeader(HTTP.CONN_DIRECTIVE, HTTP.CONN_KEEP_ALIVE);
        String contentType = headerMap.get(HTTP.CONTENT_TYPE);
        if (StringUtils.isNotBlank(body)
                && StringUtils.isNotBlank(contentType)
                && (contentType.contains(ContentType.APPLICATION_JSON.getMimeType())
                || contentType.contains(HttpConstant.CONTENT_TYPE_APPLICATION_JAVASCRIPT)
                || contentType.contains(ContentType.APPLICATION_XML.getMimeType())
                || contentType.contains(ContentType.TEXT_PLAIN.getMimeType())
                || contentType.contains(ContentType.TEXT_XML.getMimeType())
                || contentType.contains(ContentType.TEXT_HTML.getMimeType()))) {
            HttpEntity entityPot = new StringEntity(body, MediaType.JSON_UTF_8.charset()
                    .get());
            httpPut.setEntity(entityPot);
        }
        if (StringUtils.isNotBlank(body)
                && StringUtils.isNotBlank(contentType)
                && contentType.contains(ContentType.APPLICATION_FORM_URLENCODED.getMimeType())) {
            UrlEncodedFormEntity urlEncodedFormEntity = getUrlEncodedFormEntity(body);
            httpPut.setEntity(urlEncodedFormEntity);
        }
        if (StringUtils.isNotBlank(body)
                && StringUtils.isNotBlank(contentType)
                && contentType.contains(ContentType.MULTIPART_FORM_DATA.getMimeType())) {
            MultipartEntityBuilder multipartEntityBuilder = getMultipartEntityBuilder(body);
            httpPut.setEntity(multipartEntityBuilder.build());
        }
        if (StringUtils.isNotBlank(body)
                && StringUtils.isBlank(contentType)) {
            HttpEntity entityPot = new StringEntity(body, MediaType.JSON_UTF_8.charset()
                    .get());
            httpPut.setEntity(entityPot);
        }
        return httpPut;
    }

    private HttpPatch getHttpPatch(String url, Map<String, String> headerMap, String body) {
        HttpPatch httpPatch = new HttpPatch(url);
        headerMap.forEach(httpPatch::addHeader);
        httpPatch.addHeader(HTTP.CONN_DIRECTIVE, HTTP.CONN_KEEP_ALIVE);
        String contentType = headerMap.get(HTTP.CONTENT_TYPE);
        if (StringUtils.isNotBlank(body)
                && StringUtils.isNotBlank(contentType)
                && (contentType.contains(ContentType.APPLICATION_JSON.getMimeType())
                || contentType.contains(HttpConstant.CONTENT_TYPE_APPLICATION_JAVASCRIPT)
                || contentType.contains(ContentType.APPLICATION_XML.getMimeType())
                || contentType.contains(ContentType.TEXT_PLAIN.getMimeType())
                || contentType.contains(ContentType.TEXT_XML.getMimeType())
                || contentType.contains(ContentType.TEXT_HTML.getMimeType()))) {
            HttpEntity entityPot = new StringEntity(body, MediaType.JSON_UTF_8.charset()
                    .get());
            httpPatch.setEntity(entityPot);
        }
        if (StringUtils.isNotBlank(body)
                && StringUtils.isNotBlank(contentType)
                && contentType.contains(ContentType.APPLICATION_FORM_URLENCODED.getMimeType())) {
            UrlEncodedFormEntity urlEncodedFormEntity = getUrlEncodedFormEntity(body);
            httpPatch.setEntity(urlEncodedFormEntity);
        }
        if (StringUtils.isNotBlank(body)
                && StringUtils.isNotBlank(contentType)
                && contentType.contains(ContentType.MULTIPART_FORM_DATA.getMimeType())) {
            MultipartEntityBuilder multipartEntityBuilder = getMultipartEntityBuilder(body);
            httpPatch.setEntity(multipartEntityBuilder.build());
        }
        if (StringUtils.isNotBlank(body)
                && StringUtils.isBlank(contentType)) {
            HttpEntity entityPot = new StringEntity(body, MediaType.JSON_UTF_8.charset()
                    .get());
            httpPatch.setEntity(entityPot);
        }
        return httpPatch;
    }

    private UrlEncodedFormEntity getUrlEncodedFormEntity(String body) {
        List<NameValuePair> params = new ArrayList<>();
        Map<String, String> bodyMap = new Gson().fromJson(body, new TypeToken<HashMap<String, String>>() {
        }.getType());
        bodyMap.forEach((k, v) -> params.add(new NameValuePair() {
            @Override
            public String getName() {
                return k;
            }

            @Override
            public String getValue() {
                return v;
            }
        }));
        return new UrlEncodedFormEntity(params, StandardCharsets.UTF_8);
    }

    private HttpPost getHttpPost(String url, Map<String, String> headerMap, String body) {
        HttpPost httpPost = new HttpPost(url);
        headerMap.forEach(httpPost::addHeader);
        httpPost.addHeader(HTTP.CONN_DIRECTIVE, HTTP.CONN_KEEP_ALIVE);
        String contentType = headerMap.get(HTTP.CONTENT_TYPE);
        if (StringUtils.isNotBlank(body)
                && StringUtils.isNotBlank(contentType)
                && (contentType.contains(ContentType.APPLICATION_JSON.getMimeType())
                || contentType.contains(HttpConstant.CONTENT_TYPE_APPLICATION_JAVASCRIPT)
                || contentType.contains(ContentType.APPLICATION_XML.getMimeType())
                || contentType.contains(ContentType.TEXT_PLAIN.getMimeType())
                || contentType.contains(ContentType.TEXT_XML.getMimeType())
                || contentType.contains(ContentType.TEXT_HTML.getMimeType()))) {
            HttpEntity entityPot = new StringEntity(body, MediaType.JSON_UTF_8.charset()
                    .get());
            httpPost.setEntity(entityPot);
        }
        if (StringUtils.isNotBlank(body)
                && StringUtils.isNotBlank(contentType)
                && contentType.contains(ContentType.APPLICATION_FORM_URLENCODED.getMimeType())) {
            UrlEncodedFormEntity urlEncodedFormEntity = getUrlEncodedFormEntity(body);
            httpPost.setEntity(urlEncodedFormEntity);
        }
        if (StringUtils.isNotBlank(body)
                && StringUtils.isNotBlank(contentType)
                && contentType.contains(ContentType.MULTIPART_FORM_DATA.getMimeType())) {
            MultipartEntityBuilder multipartEntityBuilder = getMultipartEntityBuilder(body);
            httpPost.setEntity(multipartEntityBuilder.build());
        }
        if (StringUtils.isNotBlank(body)
                && StringUtils.isBlank(contentType)) {
            HttpEntity entityPot = new StringEntity(body, MediaType.JSON_UTF_8.charset()
                    .get());
            httpPost.setEntity(entityPot);
        }
        return httpPost;
    }

    private HttpPost getHttpPostByte(String url, Map<String, String> headerMap, byte[] body) {
        HttpPost httpPost = new HttpPost(url);
        headerMap.forEach(httpPost::addHeader);
        ByteArrayEntity byteEntity = new ByteArrayEntity(body);
        httpPost.setEntity(byteEntity);

        return httpPost;
    }

    private MultipartEntityBuilder getMultipartEntityBuilder(String body) {
        MultipartEntityBuilder multipartEntityBuilder = MultipartEntityBuilder.create();
        multipartEntityBuilder.setCharset(StandardCharsets.UTF_8);
        Map<String, String> bodyMap = new Gson().fromJson(body, new TypeToken<HashMap<String, String>>() {
        }.getType());
        bodyMap.forEach((k, v) -> multipartEntityBuilder.addPart(k, new StringBody(v, ContentType.MULTIPART_FORM_DATA)));
        return multipartEntityBuilder;
    }


    @Override
    public void close() {
        try {
            executorServicePool.shutdownNow();
            httpClient.close();
        } catch (IOException e) {
            log.error("ApacheHttpClientImpl | close | error => ", e);
        }
    }

    @Override
    public void updateProxyConfig(ProxyConfig config) {
        this.socksProxyConfig = new SocksProxyConfig(config.getSocks5Endpoint(), config.getSocks5UserName(), config.getSocks5Password());
    }

    static class SocksPlainConnectionSocketFactory extends PlainConnectionSocketFactory {

        @Override
        public Socket createSocket(final HttpContext context) throws IOException {
            InetSocketAddress socksaddr = (InetSocketAddress) context.getAttribute(SOCKS_ADDRESS_KEY);
            if (socksaddr != null) {
                Proxy proxy = new Proxy(Proxy.Type.SOCKS, socksaddr);
                return new Socket(proxy);
            } else {
                return new Socket();
            }
        }

        @Override
        public Socket connectSocket(int connectTimeout, Socket socket, HttpHost host, InetSocketAddress remoteAddress,
                                    InetSocketAddress localAddress, HttpContext context) throws IOException {
            InetSocketAddress socksaddr = (InetSocketAddress) context.getAttribute(SOCKS_ADDRESS_KEY);
            if (socksaddr != null) {
                //make proxy server to resolve host in http url
                remoteAddress = InetSocketAddress.createUnresolved(host.getHostName(), host.getPort());
            }
            return super.connectSocket(connectTimeout, socket, host, remoteAddress, localAddress, context);
        }
    }

    static class FakeDnsResolver implements DnsResolver {
        @Override
        public InetAddress[] resolve(String host) throws UnknownHostException {
            // Return some fake DNS record for every request, we won't be using it
            try {
                return new InetAddress[]{InetAddress.getByName(host)};
            } catch (Throwable e) {
                return new InetAddress[]{InetAddress.getByAddress(new byte[]{0, 0, 0, 0})};
            }
        }
    }

    static class SocksSSLConnectionSocketFactory extends SSLConnectionSocketFactory {
        public SocksSSLConnectionSocketFactory(SSLContext sslContext) {
            super(sslContext, NoopHostnameVerifier.INSTANCE);
        }

        @Override
        public Socket createSocket(final HttpContext context) throws IOException {
            InetSocketAddress socksaddr = (InetSocketAddress) context.getAttribute(SOCKS_ADDRESS_KEY);
            if (socksaddr != null) {
                Proxy proxy = new Proxy(Proxy.Type.SOCKS, socksaddr);
                return new Socket(proxy);
            } else {
                return new Socket();
            }
        }

        @Override
        public Socket connectSocket(int connectTimeout, Socket socket, HttpHost host, InetSocketAddress remoteAddress,
                                    InetSocketAddress localAddress, HttpContext context) throws IOException {
            InetSocketAddress socksaddr = (InetSocketAddress) context.getAttribute(SOCKS_ADDRESS_KEY);
            if (socksaddr != null) {
                remoteAddress = InetSocketAddress.createUnresolved(host.getHostName(), host.getPort());
            }
            return super.connectSocket(connectTimeout, socket, host, remoteAddress, localAddress, context);
        }
    }

    static class NoopHostnameVerifier implements javax.net.ssl.HostnameVerifier {
        public static final NoopHostnameVerifier INSTANCE = new NoopHostnameVerifier();

        @Override
        public boolean verify(String s, SSLSession sslSession) {
            return true;
        }
    }

    @Override
    public String toString() {
        return new StringBuilder()
                .append("Socks5Endpoint=").append(this.socksProxyConfig.getSocks5Endpoint())
                .append(", Socks5UserName=").append(this.socksProxyConfig.getSocks5UserName())
                .append(", Socks5Password=").append(this.socksProxyConfig.getSocks5Password())
                .toString();
    }
}
