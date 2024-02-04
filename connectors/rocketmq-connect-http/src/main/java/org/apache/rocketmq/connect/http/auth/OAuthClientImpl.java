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

import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Maps;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.connect.http.constant.HttpHeaderConstant;
import org.apache.rocketmq.connect.http.entity.HttpAuthParameters;
import org.apache.rocketmq.connect.http.entity.HttpOAuthParameters;
import org.apache.rocketmq.connect.http.entity.HttpRequest;
import org.apache.rocketmq.connect.http.entity.TokenEntity;
import org.apache.rocketmq.connect.http.util.JsonUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.CountDownLatch;

public class OAuthClientImpl implements Auth, Runnable {
    private static final Logger log = LoggerFactory.getLogger(OAuthClientImpl.class);
    private volatile HttpOAuthParameters httpOAuthParameters;
    private volatile TokenEntity tokenEntity;
    private AbstractHttpClient httpClient;
    private HttpAuthParameters httpAuthParameters;

    public OAuthClientImpl(HttpAuthParameters httpAuthParameters) {
        this.httpAuthParameters = httpAuthParameters;
    }

    @Override
    public Map<String, String> auth() {
        try {
            HttpOAuthParameters httpOAuthParameters = httpAuthParameters.getHttpOAuthParameters();
            String resultToken = "";
            if (httpOAuthParameters != null && StringUtils.isNotBlank(httpOAuthParameters.getEndpoint())
                    && StringUtils.isNotBlank(httpOAuthParameters.getHttpMethod())) {
                if (tokenEntity != null) {
                    Map<String, String> headMap = Maps.newHashMap();
                    headMap.put(HttpHeaderConstant.AUTHORIZATION, "Bearer " + tokenEntity.getAccessToken());
                    return headMap;
                }
                this.httpOAuthParameters = httpOAuthParameters;
                HttpRequest httpRequest = new HttpRequest();
                resultToken = getResultToken(httpOAuthParameters, httpRequest);
                if (StringUtils.isNotBlank(resultToken)) {
                    final TokenEntity token = JSONObject.parseObject(resultToken, TokenEntity.class);
                    if (StringUtils.isNotBlank(token.getAccessToken())) {
                        Map<String, String> tokenHeadMap = Maps.newHashMap();
                        tokenHeadMap.put(HttpHeaderConstant.AUTHORIZATION, "Bearer " + token.getAccessToken());
                        token.setTokenTimestamp(Long.toString(System.currentTimeMillis()));
                        tokenEntity = token;
                        return tokenHeadMap;
                    } else {
                        throw new RuntimeException(token.getError());
                    }
                }
            }
        } catch (Exception e) {
            log.error("OAuthClientImpl | auth | error => ", e);
            throw new RuntimeException(e);
        }
        return Maps.newHashMap();
    }

    public String getResultToken(HttpOAuthParameters httpOAuthParameters, HttpRequest httpRequest) throws Exception {
        Map<String, String> headMap = Maps.newHashMap();
        if (HttpHeaderConstant.POST_METHOD.equals(httpOAuthParameters.getHttpMethod())
                || HttpHeaderConstant.PUT_METHOD.equals(httpOAuthParameters.getHttpMethod())
                || HttpHeaderConstant.PATCH_METHOD.equals(httpOAuthParameters.getHttpMethod())) {
            String headerParameters = httpOAuthParameters.getHeaderParameters();
            JSONObject jsonObject = JSONObject.parseObject(headerParameters);
            if (jsonObject != null) {
                for (Map.Entry<String, Object> entry : jsonObject.entrySet()) {
                    headMap.put(entry.getKey(), (String) entry.getValue());
                }
            }
            httpRequest.setBody(httpOAuthParameters.getBodyParameters());
            httpRequest.setUrl(JsonUtils.addQueryStringAndPathValueToUrl(httpOAuthParameters.getEndpoint(), getOAuthQueryParameters(httpOAuthParameters)));
            httpRequest.setMethod(httpOAuthParameters.getHttpMethod());
            httpRequest.setTimeout(httpOAuthParameters.getTimeout());
            httpRequest.setHeaderMap(headMap);
        } else {
            httpRequest.setUrl(JsonUtils.addQueryStringAndPathValueToUrl(httpOAuthParameters.getEndpoint(), getOAuthQueryParameters(httpOAuthParameters)));
            httpRequest.setTimeout(httpOAuthParameters.getTimeout());
            httpRequest.setMethod(httpOAuthParameters.getHttpMethod());
            httpRequest.setHeaderMap(headMap);
            httpRequest.setBody(StringUtils.EMPTY);
        }
        CountDownLatch countDownLatch = new CountDownLatch(1);
        HttpCallback httpCallback = new HttpCallback(countDownLatch);
        return httpClient.execute(httpRequest, httpCallback);
    }

    private String getOAuthQueryParameters(HttpOAuthParameters httpOAuthParameters) {
        JSONObject queryParameters = StringUtils.isBlank(httpOAuthParameters.getQueryParameters()) ?
                new JSONObject() : JSONObject.parseObject(httpOAuthParameters.getQueryParameters());
        if (httpOAuthParameters.getClientID() != null && httpOAuthParameters.getClientSecret() != null) {
            JSONObject additionalQueryParameters = new JSONObject();
            additionalQueryParameters.put("client_id", httpOAuthParameters.getClientID());
            additionalQueryParameters.put("client_secret", httpOAuthParameters.getClientSecret());
            queryParameters = JsonUtils.mergeJson(additionalQueryParameters, queryParameters);
        }
        return queryParameters.toJSONString();
    }

    @Override
    public void run() {
        log.info("OAuthTokenRunnable | run");
        if (httpAuthParameters == null || httpOAuthParameters == null) {
            log.info("httpAuthParameters or httpOAuthParameters is null | break");
            return;
        }
        String resultToken = "";
        long tokenTimestamp = Long.parseLong(tokenEntity.getTokenTimestamp()) + (tokenEntity.getExpiresIn() * 1000L);
        log.info("OAuthTokenRunnable | run | tokenTimestamp : {} | system.currentTimeMillis : {} | boolean : {}", tokenTimestamp, System.currentTimeMillis(), System.currentTimeMillis() > tokenTimestamp);
        if (System.currentTimeMillis() > tokenTimestamp) {
            log.info("OAuthTokenRunnable | run | update token");
            HttpRequest httpRequest = new HttpRequest();
            try {
                resultToken = this.getResultToken(httpOAuthParameters, httpRequest);
            } catch (Exception e) {
                log.error("OAuthTokenRunnable | update token | scheduledExecutorService | error => ", e);
                throw new RuntimeException(e);
            }
            if (StringUtils.isNotBlank(resultToken)) {
                final TokenEntity token = JSONObject.parseObject(resultToken, TokenEntity.class);
                if (StringUtils.isNotBlank(token.getAccessToken())) {
                    updateTokenEntity(tokenEntity, token);
                } else {
                    throw new RuntimeException(token.getError());
                }
            }
        }
    }

    private void updateTokenEntity(TokenEntity oldTokenEntity, TokenEntity newTokenEntity) {
        if (newTokenEntity != null) {
            if (StringUtils.isNotBlank(newTokenEntity.getAccessToken())) {
                oldTokenEntity.setAccessToken(newTokenEntity.getAccessToken());
            }
            oldTokenEntity.setExpiresIn(newTokenEntity.getExpiresIn());
            if (StringUtils.isNotBlank(newTokenEntity.getScope())) {
                oldTokenEntity.setScope(newTokenEntity.getScope());
            }
            if (StringUtils.isNotBlank(newTokenEntity.getTokenType())) {
                oldTokenEntity.setTokenType(newTokenEntity.getTokenType());
            }
        }
        oldTokenEntity.setTokenTimestamp(Long.toString(System.currentTimeMillis()));
    }

    public AbstractHttpClient getHttpClient() {
        return httpClient;
    }

    public void setHttpClient(AbstractHttpClient httpClient) {
        this.httpClient = httpClient;
    }
}
