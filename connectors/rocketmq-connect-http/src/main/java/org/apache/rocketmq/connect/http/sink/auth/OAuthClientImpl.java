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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;

public class OAuthClientImpl implements Auth {
    private static final Logger log = LoggerFactory.getLogger(OAuthClientImpl.class);
    private static final AbstractHttpClient HTTP_CLIENT = new ApacheHttpClientImpl();
    private static final Map<OAuthEntity, Map<String, String>> OAUTH_MAP = Maps.newConcurrentMap();

    @Override
    public Map<String, String> auth(ClientConfig config) {
        Map<String, String> headMap = Maps.newHashMap();
        try {
            String resultToken = "";
            if (StringUtils.isNotBlank(config.getOauth2ClientId()) && StringUtils.isNotBlank(config.getOauth2ClientSecret())
                    && StringUtils.isNotBlank(config.getOauth2HttpMethod())) {
                HttpRequest httpRequest = new HttpRequest();
                resultToken = getResultToken(config, headMap, resultToken, httpRequest);
                // TODO 获取过期时间
                if (StringUtils.isNotBlank(resultToken)) {
                    final TokenEntity tokenEntity = JSONObject.parseObject(resultToken, TokenEntity.class);
                    if (StringUtils.isNotBlank(tokenEntity.getAccessToken())) {
                        headMap.put(HttpConstant.AUTHORIZATION, "Bearer " + tokenEntity.getAccessToken());
                        OAuthEntity oAuthEntity = new OAuthEntity();
                        oAuthEntity.setOauth2ClientId(config.getOauth2ClientId());
                    }
                }
            }
        } catch (Exception e) {
            log.error("BasicAuthImpl | auth | error => ", e);
        }
        return headMap;
    }

    private static String getResultToken(ClientConfig config, Map<String, String> headMap, String resultToken, HttpRequest httpRequest) throws IOException {
        if (HttpConstant.GET_METHOD.equals(config.getOauth2HttpMethod())) {
            String url = config.getOauth2Endpoint() + "/grant_type=client_credentials&client_id=" + config.getOauth2ClientId() + "&client_secret=" + config.getOauth2ClientSecret();
            httpRequest.setUrl(url);
            httpRequest.setTimeout(config.getTimeout());
            httpRequest.setMethod(config.getOauth2HttpMethod());
            httpRequest.setHeaderMap(headMap);
            httpRequest.setBody(StringUtils.EMPTY);
            resultToken = HTTP_CLIENT.execute(httpRequest);
        }
        if (HttpConstant.POST_METHOD.equals(config.getOauth2HttpMethod())) {
            String basic = config.getOauth2ClientId() + ":" + config.getOauth2ClientSecret();
            headMap.put(HttpConstant.AUTHORIZATION, "Basic " + Base64.encode(basic.getBytes(StandardCharsets.UTF_8)));
            headMap.put("grant_type", "client_credentials");
            headMap.put("Content-Type", "application/x-www-form-urlencoded");
            httpRequest.setBody(StringUtils.EMPTY);
            httpRequest.setUrl(config.getOauth2Endpoint());
            httpRequest.setMethod(config.getOauth2HttpMethod());
            httpRequest.setTimeout(config.getTimeout());
            httpRequest.setHeaderMap(headMap);
            resultToken = HTTP_CLIENT.execute(httpRequest);
        }
        return resultToken;
    }
}
