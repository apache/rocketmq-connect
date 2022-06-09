package org.apache.rocketmq.connect.http.sink.auth;

import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Maps;
import com.sun.org.apache.xerces.internal.impl.dv.util.Base64;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.connect.http.sink.client.AbstractHttpClient;
import org.apache.rocketmq.connect.http.sink.client.ApacheHttpClientImpl;
import org.apache.rocketmq.connect.http.sink.common.ClientConfig;
import org.apache.rocketmq.connect.http.sink.constant.HttpConstant;
import org.apache.rocketmq.connect.http.sink.entity.HttpRequest;
import org.apache.rocketmq.connect.http.sink.entity.OAuthEntity;
import org.apache.rocketmq.connect.http.sink.entity.TokenEntity;
import org.apache.rocketmq.connect.http.sink.utils.JsonUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;

public class OAuthClientImpl implements Auth {
    private static final Logger log = LoggerFactory.getLogger(OAuthClientImpl.class);
    private static AbstractHttpClient httpClient = null;
    public static final Map<OAuthEntity, TokenEntity> OAUTH_MAP = Maps.newConcurrentMap();

    @Override
    public Map<String, String> auth(ClientConfig config) {
        Map<String, String> headMap = Maps.newHashMap();
        try {
            init(config);
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
                final JSONObject jsonObject = JSONObject.parseObject(config.getHeaderParameters());
                oAuthEntity.setOauth2BasicKey((String) jsonObject.get(HttpConstant.OAUTH_BASIC_KEY));
                oAuthEntity.setOauth2BasicValue((String) jsonObject.get(HttpConstant.OAUTH_BASIC_VALUE));
                final TokenEntity tokenEntity = OAUTH_MAP.get(oAuthEntity);
                if (tokenEntity != null) {
                    headMap.put(HttpConstant.AUTHORIZATION, "Bearer " + tokenEntity.getAccessToken());
                    return headMap;
                }
                HttpRequest httpRequest = new HttpRequest();
                resultToken = getResultToken(oAuthEntity, headMap, resultToken, httpRequest);
                if (StringUtils.isNotBlank(resultToken)) {
                    final TokenEntity token = JSONObject.parseObject(resultToken, TokenEntity.class);
                    if (StringUtils.isNotBlank(token.getAccessToken())) {
                        headMap.put(HttpConstant.AUTHORIZATION, "Bearer " + token.getAccessToken());
                        token.setTokenTimestamp(Long.toString(System.currentTimeMillis()));
                        OAUTH_MAP.putIfAbsent(oAuthEntity, token);
                    } else {
                        throw new RuntimeException(token.getError());
                    }
                }
            }
        } catch (Exception e) {
            log.error("BasicAuthImpl | auth | error => ", e);
        }
        return headMap;
    }

    public static String getResultToken(OAuthEntity config, Map<String, String> headMap, String resultToken, HttpRequest httpRequest) throws IOException {
        headMap.put("Content-Type", "application/x-www-form-urlencoded");
        if (HttpConstant.GET_METHOD.equals(config.getOauth2HttpMethod())) {
            httpRequest.setUrl(JsonUtils.queryStringAndPathValue(config.getOauth2Endpoint(), config.getQueryStringParameters(), null));
            httpRequest.setTimeout(config.getTimeout());
            httpRequest.setMethod(config.getOauth2HttpMethod());
            httpRequest.setHeaderMap(headMap);
            httpRequest.setBody(StringUtils.EMPTY);
            resultToken = httpClient.execute(httpRequest);
        }
        if (HttpConstant.POST_METHOD.equals(config.getOauth2HttpMethod())) {
            String basic = config.getOauth2BasicKey() + ":" + config.getOauth2BasicValue();
            headMap.put(HttpConstant.AUTHORIZATION, "Basic " + Base64.encode(basic.getBytes(StandardCharsets.UTF_8)));
            httpRequest.setBody(StringUtils.EMPTY);
            httpRequest.setUrl(JsonUtils.queryStringAndPathValue(config.getOauth2Endpoint(), config.getQueryStringParameters(), null));
            httpRequest.setMethod(config.getOauth2HttpMethod());
            httpRequest.setTimeout(config.getTimeout());
            httpRequest.setHeaderMap(headMap);
            resultToken = httpClient.execute(httpRequest);
        }
        return resultToken;
    }

    private void init(ClientConfig config) {
        httpClient = new ApacheHttpClientImpl();
        httpClient.init(config);
    }
}
