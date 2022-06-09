package org.apache.rocketmq.connect.http.sink;

import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Maps;
import io.openmessaging.KeyValue;
import io.openmessaging.connector.api.component.task.sink.SinkTaskContext;
import io.openmessaging.connector.api.data.ConnectRecord;
import io.openmessaging.connector.api.errors.ConnectException;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.connect.http.sink.client.AbstractHttpClient;
import org.apache.rocketmq.connect.http.sink.client.ApacheHttpClientImpl;
import org.apache.rocketmq.connect.http.sink.common.ClientConfig;
import org.apache.rocketmq.connect.http.sink.constant.HttpConstant;
import org.apache.rocketmq.connect.http.sink.entity.ApiDestionationEntity;
import org.apache.rocketmq.connect.http.sink.entity.AuthParameters;
import org.apache.rocketmq.connect.http.sink.entity.BodyParameter;
import org.apache.rocketmq.connect.http.sink.entity.ConnectionEntity;
import org.apache.rocketmq.connect.http.sink.entity.HeaderParameter;
import org.apache.rocketmq.connect.http.sink.entity.HttpRequest;
import org.apache.rocketmq.connect.http.sink.entity.OAuthHttpParameters;
import org.apache.rocketmq.connect.http.sink.entity.OAuthParameters;
import org.apache.rocketmq.connect.http.sink.entity.QueryStringParameter;
import org.apache.rocketmq.connect.http.sink.signature.PushSecretBuilder;
import org.apache.rocketmq.connect.http.sink.signature.PushSignatureBuilder;
import org.apache.rocketmq.connect.http.sink.signature.PushSignatureConstants;
import org.apache.rocketmq.connect.http.sink.signature.PushStringToSignBuilder;
import org.apache.rocketmq.connect.http.sink.utils.CheckUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.PrivateKey;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class ApiDestinationSinkTask extends HttpSinkTask implements Runnable {
    private static final Logger log = LoggerFactory.getLogger(ApiDestinationSinkConnector.class);

    private static final ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();

    private String apiDestinationName;

    private static String endpoint;

    private String pushCertSignMethod;

    private String pushCertSignVersion;

    private String pushCertPublicKeyUrl;

    private PrivateKey privateKey;

    private static AbstractHttpClient httpClient;

    private static ApiDestionationEntity apiDestionationEntity = new ApiDestionationEntity();

    @Override
    public void init(KeyValue config) {
        apiDestinationName = config.getString(HttpConstant.API_DESTINATION_NAME);
        endpoint = config.getString(HttpConstant.ENDPOINT);
        String pushCertPrivateKey = config.getString(HttpConstant.PUSH_CERT_PRIVATE_KEY);
        pushCertSignMethod = config.getString(HttpConstant.PUSH_CERT_SIGN_METHOD);
        pushCertSignVersion = config.getString(HttpConstant.PUSH_CERT_SIGN_VERSION);
        pushCertPublicKeyUrl = config.getString(HttpConstant.PUSH_CERT_PUBLIC_KEY_URL);
        if (CheckUtils.checkNotNull(pushCertPrivateKey)) {
            String privateKeyStr = new String(Base64.getDecoder()
                    .decode(pushCertPrivateKey), StandardCharsets.UTF_8);
            privateKey = PushSecretBuilder.buildPrivateKey(privateKeyStr);
        }
        super.init(config);
    }

    @Override
    public void validate(KeyValue config) {
        super.validate(config);
    }

    @Override
    public void start(SinkTaskContext sinkTaskContext) {
        try {
            httpClient = new ApacheHttpClientImpl();
            ClientConfig config = new ClientConfig();
            config.setProxyHost(super.proxyHost);
            config.setProxyPort(super.proxyPort);
            config.setProxyType(super.proxyType);
            config.setProxyUser(super.proxyUser);
            config.setProxyPassword(super.proxyPassword);
            httpClient.init(config);
            getApiDestination();
            scheduledExecutorService.scheduleAtFixedRate(new ApiDestinationSinkTask(), 10, 10, TimeUnit.SECONDS);
            super.start(sinkTaskContext);
        } catch (Exception e) {
            log.error("ApiDestinationSinkTask | start | error => ", e);
        }
    }

    @Override
    public void stop() {
        super.stop();
    }

    @Override
    public void put(List<ConnectRecord> sinkRecords) throws ConnectException {
        super.put(sinkRecords);
    }

    @Override
    public void run() {
        log.info("Thread name : {}", Thread.currentThread().getName());
        try {
            if (HttpConstant.SUCCESS.equalsIgnoreCase(apiDestionationEntity.getCode()) && StringUtils.isNotBlank(apiDestionationEntity.getConnectionName())) {
                HttpRequest httpRequest = new HttpRequest();
                Map<String, String> map = Maps.newHashMap();
                map.put("Content-Type", "application/json");
                map.put("Accept", "application/json");
                super.urlPattern = apiDestionationEntity.getHttpApiParameters().getEndpoint();
                super.method = apiDestionationEntity.getHttpApiParameters().getMethod();
                Map<String, String> mapBody = Maps.newHashMap();
                mapBody.put("connectionName", apiDestionationEntity.getConnectionName());
                httpRequest.setUrl(endpoint + "/connection/getConnection");
                httpRequest.setMethod(HttpConstant.POST_METHOD);
                httpRequest.setHeaderMap(map);
                httpRequest.setBody(JSONObject.toJSONString(mapBody));
                final String connectionResult = httpClient.execute(httpRequest);
                log.info("connectionResult : {}", connectionResult);
                final ConnectionEntity connectionEntity = JSONObject.parseObject(connectionResult, ConnectionEntity.class);
                if (HttpConstant.SUCCESS.equalsIgnoreCase(connectionEntity.getCode())) {
                    final AuthParameters authParameters = connectionEntity.getAuthParameters();
                    super.authType = authParameters.getAuthorizationType();
                    if (authParameters.getApiKeyAuthParameters() != null) {
                        super.apiKeyName = authParameters.getApiKeyAuthParameters().getApiKeyName();
                        super.apiKeyValue = authParameters.getApiKeyAuthParameters().getApiKeyValue();
                    }
                    if (authParameters.getBasicAuthParameters() != null) {
                        super.basicUser = authParameters.getBasicAuthParameters().getUsername();
                        super.basicPassword = authParameters.getBasicAuthParameters().getPassword();
                    }
                    Map<String, String> queryParameterMap = Maps.newHashMap();
                    Map<String, String> bodyMap = Maps.newHashMap();
                    Map<String, String> headerMap = Maps.newHashMap();
                    if (authParameters.getOauthParameters() != null) {
                        parseOauthParameters(authParameters, queryParameterMap, bodyMap, headerMap);
                    }
                    if (authParameters.getInvocationHttpParameters() != null) {
                        parseInvocationHttpParameters(authParameters, queryParameterMap, bodyMap, headerMap);
                    }
                }
            }
        } catch (Exception e) {
            log.error("ApiDestinationSinkTask | run | error => ", e);
        }
    }

    private void parseInvocationHttpParameters(AuthParameters authParameters, Map<String, String> queryParameterMap, Map<String, String> bodyMap, Map<String, String> headerMap) throws Exception {
        final List<BodyParameter> bodyParameters = authParameters.getInvocationHttpParameters().getBodyParameters();
        final List<HeaderParameter> headerParameters = authParameters.getInvocationHttpParameters().getHeaderParameters();
        final List<QueryStringParameter> queryStringParameters = authParameters.getInvocationHttpParameters().getQueryStringParameters();
        queryStringParameters.forEach(queryStringParameter -> queryParameterMap.put(queryStringParameter.getKey(), queryStringParameter.getValue()));
        super.queryStringParameters = JSONObject.toJSONString(queryParameterMap);
        bodyParameters.forEach(bodyParameter -> bodyMap.put(bodyParameter.getKey(), bodyParameter.getValue()));
        super.bodys = JSONObject.toJSONString(bodyMap);
        headerParameters.forEach(headerParameter -> headerMap.put(headerParameter.getKey(), headerParameter.getValue()));
        final Map<String, String> officialHeaders = PushStringToSignBuilder.officialHeaders(pushCertSignMethod, pushCertSignVersion, pushCertPublicKeyUrl);
        String stringToSign = PushStringToSignBuilder.defaultStringToSign(super.urlPattern, officialHeaders,
                super.bodys);
        String secret = PushSecretBuilder.buildRandomSecret();
        if (privateKey != null) {
            String encryptedSecret = PushSignatureBuilder.signWithRSA(secret, privateKey);
            officialHeaders.put(PushSignatureConstants.HEADER_X_EVENTBRIDGE_SIGNATURE_SECRET, encryptedSecret);
        }
        String signature = PushSignatureBuilder.signByHmacSHA1(stringToSign, secret);
        officialHeaders.put(PushSignatureConstants.HEADER_X_EVENTBRIDGE_SIGNATURE, signature);
        headerMap.putAll(officialHeaders);
        super.headerParameters = JSONObject.toJSONString(headerMap);
    }

    private void parseOauthParameters(AuthParameters authParameters, Map<String, String> queryParameterMap, Map<String, String> bodyMap, Map<String, String> headerMap) {
        super.oauth2Endpoint = authParameters.getOauthParameters().getAuthorizationEndpoint();
        super.oauth2HttpMethod = authParameters.getOauthParameters().getHttpMethod();
        super.oauth2ClientId = authParameters.getOauthParameters().getClientParameters().getClientID();
        super.oauth2ClientSecret = authParameters.getOauthParameters().getClientParameters().getClientSecret();
        // 在获取OAuth的token时需要添加的Body、Query和Header,现在的逻辑是添加到Body、Query、Header
        final OAuthParameters oauthParameters = authParameters.getOauthParameters();
        final OAuthHttpParameters oauthHttpParameters = oauthParameters.getOauthHttpParameters();
        final List<HeaderParameter> oauthHeaderParameters = oauthHttpParameters.getHeaderParameters();
        oauthHeaderParameters.forEach(headerParameter -> headerMap.put(headerParameter.getKey(), headerParameter.getValue()));
        for (HeaderParameter headerParameter : oauthHeaderParameters) {
            if (Boolean.parseBoolean(headerParameter.getIsValueSecret())) {
                headerMap.put(HttpConstant.OAUTH_BASIC_KEY, headerParameter.getKey());
                headerMap.put(HttpConstant.OAUTH_BASIC_VALUE, headerParameter.getValue());
            }
        }
        final List<BodyParameter> oauthBodyParameters = oauthHttpParameters.getBodyParameters();
        oauthBodyParameters.forEach(oauthBodyParameter -> bodyMap.put(oauthBodyParameter.getKey(), oauthBodyParameter.getValue()));
        for (BodyParameter bodyParameter : oauthBodyParameters) {
            if (Boolean.parseBoolean(bodyParameter.getIsValueSecret())) {
                headerMap.put(HttpConstant.OAUTH_BASIC_KEY, bodyParameter.getKey());
                headerMap.put(HttpConstant.OAUTH_BASIC_VALUE, bodyParameter.getValue());
            }
        }
        final List<QueryStringParameter> oauthQueryStringParameters = oauthHttpParameters.getQueryStringParameters();
        oauthQueryStringParameters.forEach(oauthQueryStringParameter -> queryParameterMap.put(oauthQueryStringParameter.getKey(), oauthQueryStringParameter.getValue()));
        for (QueryStringParameter queryStringParameter : oauthQueryStringParameters) {
            if (Boolean.parseBoolean(queryStringParameter.getIsValueSecret())) {
                headerMap.put(HttpConstant.OAUTH_BASIC_KEY, queryStringParameter.getKey());
                headerMap.put(HttpConstant.OAUTH_BASIC_VALUE, queryStringParameter.getValue());
            }
        }
    }

    private void getApiDestination() throws IOException {
        HttpRequest httpRequest = new HttpRequest();
        Map<String, String> map = Maps.newHashMap();
        map.put("Content-Type", "application/json");
        map.put("Accept", "application/json");
        Map<String, String> mapBody = Maps.newHashMap();
        mapBody.put(HttpConstant.API_DESTINATION_NAME, apiDestinationName);
        httpRequest.setUrl(endpoint + "/api-destination/getApiDestination");
        httpRequest.setHeaderMap(map);
        httpRequest.setTimeout("6000");
        httpRequest.setMethod(HttpConstant.POST_METHOD);
        httpRequest.setBody(JSONObject.toJSONString(mapBody));
        final String result = httpClient.execute(httpRequest);
        apiDestionationEntity = JSONObject.parseObject(result, ApiDestionationEntity.class);
        run();
    }
}
