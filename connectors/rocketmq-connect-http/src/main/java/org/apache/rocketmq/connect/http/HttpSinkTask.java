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
package org.apache.rocketmq.connect.http;

import com.alibaba.fastjson.JSONObject;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import io.openmessaging.KeyValue;
import io.openmessaging.connector.api.component.task.sink.SinkTask;
import io.openmessaging.connector.api.data.ConnectRecord;
import io.openmessaging.connector.api.errors.ConnectException;
import io.openmessaging.connector.api.errors.RetriableException;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.connect.http.auth.Auth;
import org.apache.rocketmq.connect.http.auth.BasicAuthImpl;
import org.apache.rocketmq.connect.http.auth.ApiKeyAuthImpl;
import org.apache.rocketmq.connect.http.auth.OAuthClientImpl;
import org.apache.rocketmq.connect.http.constant.AuthTypeEnum;
import org.apache.rocketmq.connect.http.constant.HttpConstant;
import org.apache.rocketmq.connect.http.auth.ApacheHttpClientImpl;
import org.apache.rocketmq.connect.http.auth.AbstractHttpClient;
import org.apache.rocketmq.connect.http.auth.HttpCallback;
import org.apache.rocketmq.connect.http.entity.HttpBasicAuthParameters;
import org.apache.rocketmq.connect.http.entity.HttpApiKeyAuthParameters;
import org.apache.rocketmq.connect.http.entity.HttpAuthParameters;
import org.apache.rocketmq.connect.http.entity.HttpOAuthParameters;
import org.apache.rocketmq.connect.http.entity.HttpRequest;
import org.apache.rocketmq.connect.http.entity.ProxyConfig;
import org.apache.rocketmq.connect.http.util.CheckUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class HttpSinkTask extends SinkTask {
    private static final Logger log = LoggerFactory.getLogger(HttpSinkTask.class);
    protected static final int DEFAULT_CONSUMER_TIMEOUT_SECONDS = 30;
    protected static final String DEFAULT_REQUEST_TIMEOUT_MILL_SECONDS = "3000";
    protected static final int DEFAULT_OAUTH_DELAY_SECONDS = 1;
    protected static final int DEFAULT_CONCURRENCY = 1;

    private String url;
    private String method;
    private String headerParameters;
    private String fixedHeaderParameters;
    private String queryParameters;
    private String fixedQueryParameters;
    private String body;
    private String socks5UserName;
    private String socks5Password;
    private String socks5Endpoint;
    private String timeout;
    private String concurrency;
    private String authType;
    private String basicUsername;
    private String basicPassword;
    private String apiKeyUsername;
    private String apiKeyPassword;
    private String oAuthEndpoint;
    private String oAuthHttpMethod;
    private String oAuthClientId;
    private String oAuthClientSecret;
    private String oAuthHeaderParameters;
    private String oAuthQueryParameters;
    private String oAuthBody;
    private String token;

    private Long awaitTimeoutSeconds;
    private Auth auth;
    private HttpAuthParameters httpAuthParameters;
    private ScheduledExecutorService scheduledExecutorService;
    private AbstractHttpClient httpClient;

    @Override
    public void put(List<ConnectRecord> records) throws ConnectException {
        try {
            Long startTime = System.currentTimeMillis();
            CountDownLatch countDownLatch = new CountDownLatch(records.size());
            HttpCallback httpCallback = new HttpCallback(countDownLatch);
            for (ConnectRecord connectRecord : records) {
                // body
                // header
                Map<String, String> headerMap = renderHeaderMap(headerParameters, fixedHeaderParameters, token);
                if (auth != null) {
                    headerMap.putAll(auth.auth());
                }
                // render query to url
                String urlWithQueryParameters = renderQueryParametersToUrl(url, queryParameters, fixedQueryParameters);
                HttpRequest httpRequest = new HttpRequest();
                httpRequest.setUrl(urlWithQueryParameters);
                httpRequest.setMethod(method);
                httpRequest.setHeaderMap(headerMap);
                if (body != null) {
                    httpRequest.setBody(body);
                } else {
                    httpRequest.setBody(new Gson().toJson(connectRecord.getData()));
                }
                httpRequest.setTimeout(StringUtils.isNotBlank(timeout) ? timeout : DEFAULT_REQUEST_TIMEOUT_MILL_SECONDS);
                log.info("HttpSinkTask send request | url:{} | method: {} | headerMap: {} | body: {}",
                        httpRequest.getUrl(), httpRequest.getMethod(), httpRequest.getHeaderMap(), httpRequest.getBody());
                httpClient.executeNotReturn(httpRequest, httpCallback);
            }
            boolean consumeSucceed = Boolean.FALSE;
            try {
                consumeSucceed = countDownLatch.await(awaitTimeoutSeconds, TimeUnit.SECONDS);
            } catch (Throwable e) {
                log.error("count down latch failed.", e);
            }
            if (!consumeSucceed) {
                throw new RetriableException("Request Timeout");
            }
            if (httpCallback.isFailed()) {
                throw new RetriableException(httpCallback.getMsg());
            }
            log.info("HttpSinkTask put size:{},cost:{}", records.size(), System.currentTimeMillis() - startTime);
        } catch (Exception e) {
            log.error("HttpSinkTask | put | error => ", e);
            throw new RuntimeException(e);
        }
    }

    private Map<String, String> renderHeaderMap(String headerParameters, String fixedHeaderParameters, String token) {
        Map<String, String> headerMap = new HashMap<>();
        if (headerParameters != null) {
            Map<String, String> userDefinedHeaders = new Gson().fromJson(headerParameters, new TypeToken<Map<String, String>>() {
            }.getType());
            headerMap.putAll(userDefinedHeaders);
        }
        if (fixedHeaderParameters != null) {
            Map<String, String> fixedHeaders = new Gson().fromJson(fixedHeaderParameters, new TypeToken<Map<String, String>>() {
            }.getType());
            headerMap.putAll(fixedHeaders);
        }
        if (StringUtils.isNotBlank(token)) {
            headerMap.put(HttpConstant.TOKEN, token);
        }
        return headerMap;
    }

    private String renderQueryParametersToUrl(String url, String queryParameters, String fixedQueryParameters) throws UnsupportedEncodingException {
        Map<String, Object> queryParametersMap = new HashMap<>();
        if (queryParameters != null) {
            queryParametersMap.putAll(new Gson().fromJson(queryParameters, Map.class));
        }
        if (fixedQueryParameters != null) {
            queryParametersMap.putAll(new Gson().fromJson(fixedQueryParameters, Map.class));
        }
        StringBuilder queryParametersStringBuilder = new StringBuilder();
        for (Map.Entry<String, Object> entry : queryParametersMap.entrySet()) {
            if (entry.getValue() instanceof JSONObject) {
                queryParametersStringBuilder.append(URLEncoder.encode(entry.getKey(), "UTF-8")).append("=").append(URLEncoder.encode(((JSONObject) entry.getValue()).toJSONString(), "UTF-8")).append("&");
            } else {
                queryParametersStringBuilder.append(URLEncoder.encode(entry.getKey(), "UTF-8")).append("=").append(URLEncoder.encode((String) entry.getValue(), "UTF-8")).append("&");
            }
        }
        String path = queryParametersStringBuilder.toString();
        if (StringUtils.isNotBlank(path) && StringUtils.isNotBlank(url)) {
            if (url.contains("?")) {
                return url + "&" + path.substring(0, path.length() - 1);
            }
            return url + "?" + path.substring(0, path.length() - 1);
        }
        return url;
    }

    @Override
    public void validate(KeyValue config) {
        if (CheckUtils.checkNull(config.getString(HttpConstant.URL)) ||
                CheckUtils.checkNull(config.getString(HttpConstant.METHOD))) {
            throw new RuntimeException("http required parameter is null !");
        }
    }

    @Override
    public void start(KeyValue config) {
        // http
        url = CheckUtils.checkNullReturnDefault(config.getString(HttpConstant.URL));
        method = CheckUtils.checkNullReturnDefault(config.getString(HttpConstant.METHOD));
        headerParameters = CheckUtils.checkNullReturnDefault(config.getString(HttpConstant.HEADER_PARAMETERS));
        fixedHeaderParameters = CheckUtils.checkNullReturnDefault(config.getString(HttpConstant.FIXED_HEADER_PARAMETERS));
        queryParameters = CheckUtils.checkNullReturnDefault(config.getString(HttpConstant.QUERY_PARAMETERS));
        fixedQueryParameters = CheckUtils.checkNullReturnDefault(config.getString(HttpConstant.FIXED_QUERY_PARAMETERS));
        body = CheckUtils.checkNullReturnDefault(config.getString(HttpConstant.BODY));
        socks5Endpoint = CheckUtils.checkNullReturnDefault(config.getString(HttpConstant.SOCKS5_ENDPOINT));
        socks5UserName = CheckUtils.checkNullReturnDefault(config.getString(HttpConstant.SOCKS5_USERNAME));
        socks5Password = CheckUtils.checkNullReturnDefault(config.getString(HttpConstant.SOCKS5_PASSWORD));
        timeout = CheckUtils.checkNullReturnDefault(config.getString(HttpConstant.TIMEOUT));
        concurrency = CheckUtils.checkNullReturnDefault(config.getString(HttpConstant.CONCURRENCY));
        authType = CheckUtils.checkNullReturnDefault(config.getString(HttpConstant.AUTH_TYPE));
        basicUsername = CheckUtils.checkNullReturnDefault(config.getString(HttpConstant.BASIC_USERNAME));
        basicPassword = CheckUtils.checkNullReturnDefault(config.getString(HttpConstant.BASIC_PASSWORD));
        apiKeyUsername = CheckUtils.checkNullReturnDefault(config.getString(HttpConstant.API_KEY_USERNAME));
        apiKeyPassword = CheckUtils.checkNullReturnDefault(config.getString(HttpConstant.API_KEY_PASSWORD));
        oAuthEndpoint = CheckUtils.checkNullReturnDefault(config.getString(HttpConstant.OAUTH_ENDPOINT));
        oAuthHttpMethod = CheckUtils.checkNullReturnDefault(config.getString(HttpConstant.OAUTH_HTTP_METHOD));
        oAuthClientId = CheckUtils.checkNullReturnDefault(config.getString(HttpConstant.OAUTH_CLIENT_ID));
        oAuthClientSecret = CheckUtils.checkNullReturnDefault(config.getString(HttpConstant.OAUTH_CLIENT_SECRET));
        oAuthHeaderParameters = CheckUtils.checkNullReturnDefault(config.getString(HttpConstant.OAUTH_HEADER_PARAMETERS));
        oAuthQueryParameters = CheckUtils.checkNullReturnDefault(config.getString(HttpConstant.OAUTH_QUERY_PARAMETERS));
        oAuthBody = CheckUtils.checkNullReturnDefault(config.getString(HttpConstant.OAUTH_BODY));
        token = config.getString(HttpConstant.TOKEN);
        try {
            httpClient = new ApacheHttpClientImpl(StringUtils.isNotBlank(concurrency) ? Integer.parseInt(concurrency) : DEFAULT_CONCURRENCY);
            ProxyConfig proxyConfig = new ProxyConfig();
            proxyConfig.setSocks5Endpoint(socks5Endpoint);
            proxyConfig.setSocks5UserName(socks5UserName);
            proxyConfig.setSocks5Password(socks5Password);
            httpClient.init(proxyConfig);
            initAuth(httpClient);
            awaitTimeoutSeconds = Long.min(StringUtils.isNotBlank(timeout) ? Long.parseLong(timeout) + 5 : DEFAULT_CONSUMER_TIMEOUT_SECONDS, DEFAULT_CONSUMER_TIMEOUT_SECONDS);
        } catch (Exception e) {
            log.error("HttpSinkTask | start | error => ", e);
        }
        log.info("HttpSinkTask | start | success => config : {}", new Gson().toJson(config));
    }

    @Override
    public void stop() {
        httpClient.close();
        if (scheduledExecutorService != null) {
            this.scheduledExecutorService.shutdownNow();
            this.scheduledExecutorService = null;
        }
    }

    private void initAuth(AbstractHttpClient httpClient) throws Exception {
        httpAuthParameters = initAuthParameters();
        if (scheduledExecutorService != null) {
            scheduledExecutorService.shutdownNow();
            scheduledExecutorService = null;
        }
        if (httpAuthParameters == null || StringUtils.isBlank(httpAuthParameters.getAuthType())) {
            this.auth = null;
            return;
        }
        if (httpAuthParameters.getAuthType().equals(AuthTypeEnum.BASIC.getAuthType())) {
            this.auth = new BasicAuthImpl(httpAuthParameters);
        } else if (httpAuthParameters.getAuthType().equals(AuthTypeEnum.API_KEY.getAuthType())) {
            this.auth = new ApiKeyAuthImpl(httpAuthParameters);
        } else if (httpAuthParameters.getAuthType().equals(AuthTypeEnum.OAUTH_CLIENT_CREDENTIALS.getAuthType())) {
            if (httpClient == null) {
                log.error("HttpSinkTask | initOrUpdateAuth | httpClient is null !");
                throw new RuntimeException("httpClient is null ! init httpClient before initOrUpdateAuth");
            }
            OAuthClientImpl oAuthClient = new OAuthClientImpl(httpAuthParameters);
            oAuthClient.setHttpClient(httpClient);
            scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
            scheduledExecutorService.scheduleWithFixedDelay(oAuthClient, DEFAULT_OAUTH_DELAY_SECONDS, DEFAULT_OAUTH_DELAY_SECONDS, TimeUnit.SECONDS);
            this.auth = oAuthClient;
        } else {
            this.auth = null;
        }
    }

    private HttpAuthParameters initAuthParameters() {
        HttpBasicAuthParameters httpBasicAuthParameters = new HttpBasicAuthParameters(basicUsername, basicPassword);
        HttpApiKeyAuthParameters httpApiKeyAuthParameters = new HttpApiKeyAuthParameters(apiKeyUsername, apiKeyPassword);
        HttpOAuthParameters httpOAuthParameters = new HttpOAuthParameters(oAuthEndpoint, oAuthHttpMethod, oAuthClientId, oAuthClientSecret, oAuthHeaderParameters, oAuthQueryParameters, oAuthBody, DEFAULT_REQUEST_TIMEOUT_MILL_SECONDS);
        return new HttpAuthParameters(authType, httpBasicAuthParameters, httpApiKeyAuthParameters, httpOAuthParameters);
    }

    /*
     * update proxy
     * */
    protected void updateProxy(ProxyConfig proxyConfig) {
        this.httpClient.updateProxyConfig(proxyConfig);
    }

    protected void updateAuth(String authType,
                              String basicUsername, String basicPassword,
                              String apiKeyUsername, String apiKeyPassword,
                              String oAuthEndpoint, String oAuthHttpMethod, String oAuthClientId, String oAuthClientSecret, String oAuthHeaderParameters, String oAuthQueryParameters, String oAuthBody) {
        if (AuthTypeEnum.parse(authType) == null) {
            throw new RuntimeException("authType is null !");
        }
        this.authType = authType;
        this.basicUsername = basicUsername;
        this.basicPassword = basicPassword;
        this.apiKeyUsername = apiKeyUsername;
        this.apiKeyPassword = apiKeyPassword;
        this.oAuthEndpoint = oAuthEndpoint;
        this.oAuthHttpMethod = oAuthHttpMethod;
        this.oAuthClientId = oAuthClientId;
        this.oAuthClientSecret = oAuthClientSecret;
        this.oAuthHeaderParameters = oAuthHeaderParameters;
        this.oAuthQueryParameters = oAuthQueryParameters;
        this.oAuthBody = oAuthBody;
        try {
            initAuth(httpClient);
        } catch (Exception e) {
            log.error("HttpSinkTask | updateAuth | error => ", e);
        }
    }

    protected Auth getAuth() {
        return auth;
    }

    protected AbstractHttpClient getHttpClient() {
        return httpClient;
    }

    protected Map<String, String> getCustomHeaders() throws Exception {
        return renderHeaderMap(headerParameters, fixedHeaderParameters, token);
    }

    protected String getCustomUrlWithQueryParameters() throws Exception {
        return renderQueryParametersToUrl(url, queryParameters, fixedQueryParameters);
    }

    protected String getCustomBody(ConnectRecord connectRecord) throws Exception {
        return body;
    }
}
