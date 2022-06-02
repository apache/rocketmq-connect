package org.apache.rocketmq.connect.http.sink;

import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Maps;
import io.openmessaging.KeyValue;
import io.openmessaging.connector.api.component.task.Task;
import io.openmessaging.internal.DefaultKeyValue;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.connect.http.sink.client.AbstractHttpClient;
import org.apache.rocketmq.connect.http.sink.client.ApacheHttpClientImpl;
import org.apache.rocketmq.connect.http.sink.constant.HttpConstant;
import org.apache.rocketmq.connect.http.sink.entity.ApiDestionationEntity;
import org.apache.rocketmq.connect.http.sink.entity.AuthParameters;
import org.apache.rocketmq.connect.http.sink.entity.BodyParameter;
import org.apache.rocketmq.connect.http.sink.entity.ConnectionEntity;
import org.apache.rocketmq.connect.http.sink.entity.HeaderParameter;
import org.apache.rocketmq.connect.http.sink.entity.HttpRequest;
import org.apache.rocketmq.connect.http.sink.entity.QueryStringParameter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class ApiDestinationSinkConnector extends HttpSinkConnector implements Runnable{
    private static final Logger log = LoggerFactory.getLogger(ApiDestinationSinkConnector.class);

    private String apiDestinationName;

    private String regionId;

    private String accessKeyId;

    private String accessKeySecret;

    private String accountId;

    private String endpoint;

    private static final ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();

    private static final AbstractHttpClient HTTP_CLIENT = new ApacheHttpClientImpl();

    @Override
    public List<KeyValue> taskConfigs(int maxTasks) {
        super.taskConfigs(maxTasks);
        List<KeyValue> keyValueList = new ArrayList<>(11);
        KeyValue keyValue = new DefaultKeyValue();
        keyValue.put("apiDestinationName", apiDestinationName);
        keyValue.put("regionId", regionId);
        keyValue.put("accessKeyId", accessKeyId);
        keyValue.put("accessKeySecret", accessKeySecret);
        keyValue.put("accountId", accountId);
        keyValue.put("endpoint", endpoint);
        return keyValueList;
    }

    @Override
    public void validate(KeyValue config) {
        super.validate(config);
    }

    @Override
    public void init(KeyValue config) {
        apiDestinationName = config.getString("apiDestinationName");
        regionId = config.getString("regionId");
        accessKeyId = config.getString("accessKeyId");
        accessKeySecret = config.getString("accessKeySecret");
        accountId = config.getString("accountId");
        endpoint = config.getString("endpoint");
        scheduledExecutorService.scheduleAtFixedRate(() -> {

        }, 10, 10, TimeUnit.SECONDS);
        super.init(config);
    }

    @Override
    public Class<? extends Task> taskClass() {
        return ApiDestinationSinkTask.class;
    }

    @Override
    public void run() {
        Map<String, String> map = Maps.newHashMap();
        map.put("Content-Type", "application/json");
        Map<String, String> mapBody = Maps.newHashMap();
        mapBody.put("apiDestinationName", apiDestinationName);
        try {
            HttpRequest httpRequest = new HttpRequest();
            httpRequest.setUrl(endpoint+ "/api-destination/getApiDestination");
            httpRequest.setHeaderMap(map);
            httpRequest.setTimeout("6000");
            httpRequest.setMethod(HttpConstant.POST_METHOD);
            httpRequest.setBody(mapBody.toString());
            final String result = HTTP_CLIENT.execute(httpRequest);
            final ApiDestionationEntity apiDestionationEntity = JSONObject.parseObject(result, ApiDestionationEntity.class);
            if (HttpConstant.SUCCESS.equalsIgnoreCase(apiDestionationEntity.getCode()) && StringUtils.isNotBlank(apiDestionationEntity.getConnectionName())) {
                super.urlPattern = apiDestionationEntity.getHttpApiParameters().getEndpoint();
                super.method = apiDestionationEntity.getHttpApiParameters().getMethod();
                mapBody.put("connectionName", apiDestionationEntity.getConnectionName());
                httpRequest.setUrl(endpoint + "/connection/getConnection");
                httpRequest.setMethod(HttpConstant.POST_METHOD);
                httpRequest.setHeaderMap(map);
                httpRequest.setBody(mapBody.toString());
                final String connectionResult = HTTP_CLIENT.execute(httpRequest);
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
                    if (authParameters.getOauthParameters() != null) {
                        super.oauth2Endpoint = authParameters.getOauthParameters().getAuthorizationEndpoint();
                        super.oauth2HttpMethod = authParameters.getOauthParameters().getHttpMethod();
                        super.oauth2ClientId = authParameters.getOauthParameters().getClientParameters().getClientID();
                        super.oauth2ClientSecret = authParameters.getOauthParameters().getClientParameters().getClientSecret();
                        // TODO 没有处理OAuth的Body、Query和Header
                    }
                    if (authParameters.getInvocationHttpParameters() != null) {
                        final List<BodyParameter> bodyParameters = authParameters.getInvocationHttpParameters().getBodyParameters();
                        final List<HeaderParameter> headerParameters = authParameters.getInvocationHttpParameters().getHeaderParameters();
                        final List<QueryStringParameter> queryStringParameters = authParameters.getInvocationHttpParameters().getQueryStringParameters();
                        StringBuilder queryParameter = new StringBuilder();
                        queryStringParameters.forEach(queryStringParameter -> {
                            queryParameter.append("&")
                                    .append(queryStringParameter.getKey())
                                    .append("=")
                                    .append(queryStringParameter.getValue());
                        });
                        super.queryStringParameters = queryParameter.toString();
                        Map<String, String> bodyMap = Maps.newHashMap();
                        bodyParameters.forEach(bodyParameter -> bodyMap.put(bodyParameter.getKey(), bodyParameter.getValue()));
                        super.bodys = bodyMap.toString();
                        Map<String, String> headerMap = Maps.newHashMap();
                        headerParameters.forEach(headerParameter -> headerMap.put(headerParameter.getKey(), headerParameter.getValue()));
                        super.headerParameters = headerMap.toString();
                    }
                }
            }
        } catch (IOException e) {
            log.error("ApiDestinationSinkConnector | init | error => ", e);
        }
    }
}
