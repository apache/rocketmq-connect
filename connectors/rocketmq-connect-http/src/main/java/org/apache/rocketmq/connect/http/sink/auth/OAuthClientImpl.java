package org.apache.rocketmq.connect.http.sink.auth;

import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Maps;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.connect.http.sink.constant.HttpConstant;
import org.apache.rocketmq.connect.http.sink.entity.ClientConfig;
import org.apache.rocketmq.connect.http.sink.entity.HttpRequest;
import org.apache.rocketmq.connect.http.sink.entity.OAuthEntity;
import org.apache.rocketmq.connect.http.sink.entity.TokenEntity;
import org.apache.rocketmq.connect.http.sink.util.JsonUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

public class OAuthClientImpl implements Auth, Runnable {
    private static final Logger log = LoggerFactory.getLogger(OAuthClientImpl.class);
    public Map<OAuthEntity, TokenEntity> oauthMap;

    public void setOauthMap(Map<OAuthEntity, TokenEntity> oauthMap) {
        this.oauthMap = oauthMap;
    }

    @Override
    public Map<String, String> auth(ClientConfig config) {
        Map<String, String> headMap = Maps.newHashMap();
        try {
            String resultToken = "";
            if (StringUtils.isNotBlank(config.getOauth2Endpoint())
                    && StringUtils.isNotBlank(config.getOauth2HttpMethod())) {
                OAuthEntity oAuthEntity = new OAuthEntity();
                oAuthEntity.setOauth2ClientId(config.getOauth2ClientId());
                oAuthEntity.setOauth2ClientSecret(config.getOauth2ClientSecret());
                oAuthEntity.setOauth2Endpoint(config.getOauth2Endpoint());
                oAuthEntity.setOauth2HttpMethod(config.getOauth2HttpMethod());
                oAuthEntity.setHeaderParamsters(config.getHeaderParameters());
                oAuthEntity.setQueryStringParameters(config.getQueryStringParameters());
                oAuthEntity.setTimeout(config.getTimeout());
                oAuthEntity.setHttpClient(config.getHttpClient());
                queryParameterClient(config);
                final TokenEntity tokenEntity = oauthMap.get(oAuthEntity);
                if (tokenEntity != null) {
                    headMap.put(HttpConstant.AUTHORIZATION, "Bearer " + tokenEntity.getAccessToken());
                    return headMap;
                }
                HttpRequest httpRequest = new HttpRequest();
                resultToken = getResultToken(oAuthEntity, headMap, oAuthEntity.getHttpClient(), httpRequest);
                if (StringUtils.isNotBlank(resultToken)) {
                    final TokenEntity token = JSONObject.parseObject(resultToken, TokenEntity.class);
                    if (StringUtils.isNotBlank(token.getAccessToken())) {
                        headMap.put(HttpConstant.AUTHORIZATION, "Bearer " + token.getAccessToken());
                        token.setTokenTimestamp(Long.toString(System.currentTimeMillis()));
                        oauthMap.putIfAbsent(oAuthEntity, token);
                    } else {
                        throw new RuntimeException(token.getError());
                    }
                }
            }
        } catch (Exception e) {
            log.error("OAuthClientImpl | auth | error => ", e);
            throw new RuntimeException(e);
        }
        return headMap;
    }

    private void queryParameterClient(ClientConfig config) {
        Map<String, String> map = Maps.newHashMap();
        map.put("client_id", config.getOauth2ClientId());
        map.put("client_secret", config.getOauth2ClientSecret());
        JSONObject queryString = JSONObject.parseObject(config.getQueryStringParameters());
        JSONObject jsonObject = JSONObject.parseObject(JSONObject.toJSONString(map));
        config.setQueryStringParameters(JsonUtils.mergeJson(jsonObject, queryString).toJSONString());
    }

    public String getResultToken(OAuthEntity config, Map<String, String> headMap, AbstractHttpClient httpClient, HttpRequest httpRequest) throws Exception {
        if (HttpConstant.POST_METHOD.equals(config.getOauth2HttpMethod())
            || HttpConstant.PUT_METHOD.equals(config.getOauth2HttpMethod())
            || HttpConstant.PATCH_METHOD.equals(config.getOauth2HttpMethod())) {
            String headerParamsters = config.getHeaderParamsters();
            JSONObject jsonObject = JSONObject.parseObject(headerParamsters);
            for (Map.Entry<String, Object> entry : jsonObject.entrySet()) {
                headMap.put(entry.getKey(), (String) entry.getValue());
            }
            httpRequest.setBody(StringUtils.EMPTY);
            httpRequest.setUrl(JsonUtils.queryStringAndPathValue(config.getOauth2Endpoint(), config.getQueryStringParameters(), null));
            httpRequest.setMethod(config.getOauth2HttpMethod());
            httpRequest.setTimeout(config.getTimeout());
            httpRequest.setHeaderMap(headMap);
        } else {
            httpRequest.setUrl(JsonUtils.queryStringAndPathValue(config.getOauth2Endpoint(), config.getQueryStringParameters(), null));
            httpRequest.setTimeout(config.getTimeout());
            httpRequest.setMethod(config.getOauth2HttpMethod());
            httpRequest.setHeaderMap(headMap);
            httpRequest.setBody(StringUtils.EMPTY);
        }
        CountDownLatch countDownLatch = new CountDownLatch(1);
        HttpCallback httpCallback = new HttpCallback(countDownLatch);
        return httpClient.execute(httpRequest, httpCallback);
    }

    @Override
    public void run() {
        log.info("OAuthTokenRunnable | run");
        oauthMap.forEach((oAuthEntity1, tokenEntity) -> {
            String resultToken = "";
            long tokenTimestamp = Long.parseLong(tokenEntity.getTokenTimestamp()) + (tokenEntity.getExpiresIn() * 1000L);
            log.info("OAuthTokenRunnable | run | tokenTimestamp : {} | system.currentTimeMillis : {} | boolean : {}", tokenTimestamp, System.currentTimeMillis(), System.currentTimeMillis() > tokenTimestamp);
            if (System.currentTimeMillis() > tokenTimestamp) {
                log.info("OAuthTokenRunnable | run | update token");
                HttpRequest httpRequest = new HttpRequest();
                try {
                    resultToken = this.getResultToken(oAuthEntity1, new HashMap<>(16), oAuthEntity1.getHttpClient(), httpRequest);
                } catch (Exception e) {
                    log.error("OAuthTokenRunnable | update token | scheduledExecutorService | error => ", e);
                    throw new RuntimeException(e);
                }
                if (StringUtils.isNotBlank(resultToken)) {
                    final TokenEntity token = JSONObject.parseObject(resultToken, TokenEntity.class);
                    if (StringUtils.isNotBlank(token.getAccessToken())) {
                        oauthMap.putIfAbsent(oAuthEntity1, updateTokenEntity(tokenEntity, token));
                    } else {
                        throw new RuntimeException(token.getError());
                    }
                }
            }
        });
    }

    private TokenEntity updateTokenEntity(TokenEntity oldTokenEntity, TokenEntity newTokenEntity) {
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
        return oldTokenEntity;
    }
}
