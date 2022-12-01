package org.apache.rocketmq.connect.http.sink;

import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Maps;
import io.openmessaging.KeyValue;
import io.openmessaging.connector.api.component.task.sink.SinkTask;
import io.openmessaging.connector.api.data.ConnectRecord;
import io.openmessaging.connector.api.errors.ConnectException;
import io.openmessaging.connector.api.errors.RetriableException;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.connect.http.sink.auth.AbstractHttpClient;
import org.apache.rocketmq.connect.http.sink.auth.ApacheHttpClientImpl;
import org.apache.rocketmq.connect.http.sink.auth.ApiKeyImpl;
import org.apache.rocketmq.connect.http.sink.auth.BasicAuthImpl;
import org.apache.rocketmq.connect.http.sink.auth.HttpCallback;
import org.apache.rocketmq.connect.http.sink.auth.OAuthClientImpl;
import org.apache.rocketmq.connect.http.sink.constant.AuthTypeEnum;
import org.apache.rocketmq.connect.http.sink.constant.HttpConstant;
import org.apache.rocketmq.connect.http.sink.entity.ClientConfig;
import org.apache.rocketmq.connect.http.sink.entity.HttpRequest;
import org.apache.rocketmq.connect.http.sink.util.CheckUtils;
import org.apache.rocketmq.connect.http.sink.util.JsonUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class HttpSinkTask extends SinkTask {
    private static final Logger log = LoggerFactory.getLogger(HttpSinkTask.class);
    private static final int DEFAULT_CONSUMER_TIMEOUT_SECONDS = 30;

    protected ScheduledExecutorService scheduledExecutorService;
    protected String urlPattern;
    protected String method;
    protected String queryStringParameters;
    protected String headerParameters;
    protected String bodys;
    protected String authType;
    protected String basicUser;
    protected String basicPassword;
    protected String oauth2Endpoint;
    protected String oauth2ClientId;
    protected String oauth2ClientSecret;
    protected String oauth2HttpMethod;
    protected String proxyType;
    protected String proxyHost;
    protected String proxyPort;
    protected String proxyUser;
    protected String proxyPassword;
    protected String apiKeyName;
    protected String apiKeyValue;
    protected String timeout;

    private AbstractHttpClient httpClient;

    private OAuthClientImpl oAuthClient;

    private BasicAuthImpl basicAuth;

    private ApiKeyImpl apiKey;

    @Override
    public void put(List<ConnectRecord> sinkRecords) throws ConnectException {
        try {
            CountDownLatch countDownLatch = new CountDownLatch(sinkRecords.size());
            HttpCallback httpCallback = new HttpCallback(countDownLatch);
            for (ConnectRecord connectRecord : sinkRecords) {
                ClientConfig clientConfig = getClientConfig(connectRecord);
                Map<String, String> headerMap = Maps.newHashMap();
                addHeaderMap(headerMap, clientConfig);
                if (StringUtils.isNotBlank(clientConfig.getAuthType())) {
                    headerMap.putAll(auth(clientConfig));
                }
                HttpRequest httpRequest = new HttpRequest();
                httpRequest.setBody(clientConfig.getBodys());
                httpRequest.setHeaderMap(headerMap);
                httpRequest.setMethod(clientConfig.getMethod());
                httpRequest.setTimeout(clientConfig.getTimeout());
                httpRequest.setUrl(JsonUtils.queryStringAndPathValue(clientConfig.getUrlPattern(), clientConfig.getQueryStringParameters(), connectRecord.getExtension(HttpConstant.HTTP_PATH_VALUE)));
                httpClient.execute(httpRequest, httpCallback);
            }
            boolean consumeSucceed = Boolean.FALSE;
            try {
                consumeSucceed = countDownLatch.await(DEFAULT_CONSUMER_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            } catch (Throwable e) {
                log.error("count down latch failed.", e);
            }
            if (!consumeSucceed) {
                throw new RetriableException("Request Timeout");
            }
            if (httpCallback.isFailed()) {
                throw new RetriableException(httpCallback.getMsg());
            }
        } catch (Exception e) {
            log.error("HttpSinkTask | put | error => ", e);
            throw new RuntimeException(e);
        }
    }

    private ClientConfig getClientConfig(ConnectRecord connectRecord) {
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setHttpClient(httpClient);
        clientConfig.setUrlPattern(urlPattern);
        clientConfig.setMethod(CheckUtils.checkNull(method) ? connectRecord.getExtension(HttpConstant.HTTP_METHOD) : method);
        clientConfig.setAuthType(authType);
        clientConfig.setHttpPathValue(connectRecord.getExtension(HttpConstant.HTTP_PATH_VALUE));
        clientConfig.setQueryStringParameters(JsonUtils.mergeJson(JSONObject.parseObject(connectRecord.getExtension(HttpConstant.HTTP_QUERY_VALUE)), JSONObject.parseObject(queryStringParameters)) == null ? null : JsonUtils.mergeJson(JSONObject.parseObject(connectRecord.getExtension(HttpConstant.HTTP_QUERY_VALUE)), JSONObject.parseObject(queryStringParameters)).toJSONString());
        clientConfig.setHeaderParameters(JsonUtils.mergeJson(JSONObject.parseObject(connectRecord.getExtension(HttpConstant.HTTP_HEADER)), JSONObject.parseObject(headerParameters)) == null ? null : JsonUtils.mergeJson(JSONObject.parseObject(connectRecord.getExtension(HttpConstant.HTTP_HEADER)), JSONObject.parseObject(headerParameters)).toJSONString());
        clientConfig.setBodys(bodys);
        clientConfig.setProxyUser(proxyUser);
        clientConfig.setProxyPassword(proxyPassword);
        clientConfig.setProxyType(proxyType);
        clientConfig.setProxyPort(proxyPort);
        clientConfig.setProxyHost(proxyHost);
        clientConfig.setOauth2ClientId(oauth2ClientId);
        clientConfig.setOauth2ClientSecret(oauth2ClientSecret);
        clientConfig.setTimeout(timeout);
        clientConfig.setOauth2HttpMethod(oauth2HttpMethod);
        clientConfig.setOauth2Endpoint(oauth2Endpoint);
        clientConfig.setBasicUser(basicUser);
        clientConfig.setBasicPassword(basicPassword);
        clientConfig.setApiKeyName(apiKeyName);
        clientConfig.setApiKeyValue(apiKeyValue);
        return clientConfig;
    }

    private void addHeaderMap(Map<String, String> headerMap, ClientConfig clientConfig) {
        String header = clientConfig.getHeaderParameters();
        if (StringUtils.isBlank(header)) {
            return;
        }
        JSONObject jsonObject = JSONObject.parseObject(header);
        for (Map.Entry<String, Object> entry : jsonObject.entrySet()) {
            if (entry.getValue() instanceof JSONObject) {
                headerMap.put(entry.getKey(), ((JSONObject) entry.getValue()).toJSONString());
            } else {
                headerMap.put(entry.getKey(), (String) entry.getValue());
            }
        }
    }

    @Override
    public void validate(KeyValue config) {
        if (CheckUtils.checkNull(config.getString(HttpConstant.URL_PATTERN_CONSTANT))
            || CheckUtils.checkNull(config.getString(HttpConstant.AUTH_TYPE_CONSTANT))
            || CheckUtils.checkNull(config.getString(HttpConstant.BODYS_CONSTANT))) {
            throw new RuntimeException("http required parameter is null !");
        }
        final List<AuthTypeEnum> collect = Arrays.stream(AuthTypeEnum.values()).filter(authTypeEnum -> authTypeEnum.getAuthType().equals(config.getString(HttpConstant.AUTH_TYPE_CONSTANT))).collect(Collectors.toList());
        if (collect.isEmpty()) {
            throw new RuntimeException("authType required parameter check is fail !");
        }
    }

    @Override
    public void start(KeyValue config) {
        urlPattern = CheckUtils.checkNullReturnDefault(config.getString(HttpConstant.URL_PATTERN_CONSTANT));
        method = CheckUtils.checkNullReturnDefault(config.getString(HttpConstant.METHOD_CONSTANT));
        bodys = CheckUtils.checkNullReturnDefault(config.getString(HttpConstant.BODYS_CONSTANT));
        authType = CheckUtils.checkNullReturnDefault(config.getString(HttpConstant.AUTH_TYPE_CONSTANT));
        basicUser = CheckUtils.checkNullReturnDefault(config.getString(HttpConstant.BASIC_USER_CONSTANT));
        basicPassword = CheckUtils.checkNullReturnDefault(config.getString(HttpConstant.BASIC_PASSWORD_CONSTANT));
        oauth2Endpoint = CheckUtils.checkNullReturnDefault(config.getString(HttpConstant.OAUTH2_ENDPOINT_CONSTANT));
        oauth2ClientId = CheckUtils.checkNullReturnDefault(config.getString(HttpConstant.OAUTH2_CLIENTID_CONSTANT));
        oauth2ClientSecret = CheckUtils.checkNullReturnDefault(config.getString(HttpConstant.OAUTH2_CLIENTSECRET_CONSTANT));
        oauth2HttpMethod = CheckUtils.checkNullReturnDefault(config.getString(HttpConstant.OAUTH2_HTTP_METHOD_CONSTANT));
        proxyType = CheckUtils.checkNullReturnDefault(config.getString(HttpConstant.PROXY_TYPE_CONSTANT));
        proxyHost = CheckUtils.checkNullReturnDefault(config.getString(HttpConstant.PROXY_HOST_CONSTANT));
        proxyPort = CheckUtils.checkNullReturnDefault(config.getString(HttpConstant.PROXY_PORT_CONSTANT));
        proxyUser = CheckUtils.checkNullReturnDefault(config.getString(HttpConstant.PROXY_USER_CONSTANT));
        proxyPassword = CheckUtils.checkNullReturnDefault(config.getString(HttpConstant.PROXY_PASSWORD_CONSTANT));
        timeout = CheckUtils.checkNullReturnDefault(config.getString(HttpConstant.TIMEOUT_CONSTANT));
        apiKeyName = CheckUtils.checkNullReturnDefault(config.getString(HttpConstant.API_KEY_NAME));
        apiKeyValue = CheckUtils.checkNullReturnDefault(config.getString(HttpConstant.API_KEY_VALUE));
        queryStringParameters = CheckUtils.checkNullReturnDefault(config.getString(HttpConstant.QUERY_STRING_PARAMETERS_CONSTANT));
        headerParameters = CheckUtils.checkNullReturnDefault(config.getString(HttpConstant.HEADER_PARAMETERS_CONSTANT));
        try {
            httpClient = new ApacheHttpClientImpl();
            ClientConfig proxyConfig = new ClientConfig();
            proxyConfig.setProxyHost(proxyHost);
            proxyConfig.setProxyPort(proxyPort);
            proxyConfig.setProxyType(proxyType);
            proxyConfig.setProxyUser(proxyUser);
            proxyConfig.setProxyPassword(proxyPassword);
            httpClient.init(proxyConfig);
            oAuthClient = new OAuthClientImpl();
            oAuthClient.setOauthMap(new ConcurrentHashMap<>(16));
            scheduledExecutorService.scheduleAtFixedRate(oAuthClient, 1, 1, TimeUnit.SECONDS);
            basicAuth = new BasicAuthImpl();
            apiKey = new ApiKeyImpl();
        } catch (Exception e) {
            log.error("HttpSinkTask | start | error => ", e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public void stop() {
        httpClient.close();
    }

    private Map<String, String> auth(ClientConfig config) {
        switch (config.getAuthType()) {
            case "BASIC_AUTH":
                return basicAuth.auth(config);
            case "OAUTH_AUTH":
                return oAuthClient.auth(config);
            case "API_KEY_AUTH":
                return apiKey.auth(config);
            default:
                break;
        }
        return new HashMap<>(16);
    }

    public void setScheduledExecutorService(ScheduledExecutorService scheduledExecutorService) {
        this.scheduledExecutorService = scheduledExecutorService;
    }
}
