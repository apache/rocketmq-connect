package org.apache.rocketmq.connect.http.sink;

import org.apache.rocketmq.connect.http.sink.constant.HttpConstant;
import io.openmessaging.KeyValue;
import io.openmessaging.connector.api.component.task.Task;
import io.openmessaging.connector.api.component.task.sink.SinkConnector;
import io.openmessaging.internal.DefaultKeyValue;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.connect.http.sink.enums.AuthTypeEnum;

import java.net.URL;
import java.net.URLConnection;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class HttpSinkConnector extends SinkConnector {

    private String urlPattern;
    private String method;
    private String queryStringParameters;
    private String headerParameters;
    private String bodys;
    private String authType;
    private String basicUser;
    private String basicPassword;
    private String oauth2Endpoint;
    private String oauth2ClientId;
    private String oauth2ClientSecret;
    private String oauth2HttpMethod;
    private String proxyType;
    private String proxyHost;
    private String proxyPort;
    private String proxyUser;
    private String proxyPassword;
    private String timeout;


    @Override
    public void pause() {

    }

    @Override
    public void resume() {

    }

    @Override
    public List<KeyValue> taskConfigs(int maxTasks) {
        List<KeyValue> keyValueList = new ArrayList<>(11);
        KeyValue keyValue = new DefaultKeyValue();
        keyValue.put(HttpConstant.URL_PATTERN_CONSTANT, urlPattern);
        keyValue.put(HttpConstant.METHOD_CONSTANT, method);
        keyValue.put(HttpConstant.QUERY_STRING_PARAMETERS_CONSTANT, queryStringParameters);
        keyValue.put(HttpConstant.HEADER_PARAMETERS_CONSTANT, headerParameters);
        keyValue.put(HttpConstant.BODYS_CONSTANT, bodys);
        keyValue.put(HttpConstant.AUTH_TYPE_CONSTANT, authType);
        keyValue.put(HttpConstant.BASIC_USER_CONSTANT, basicUser);
        keyValue.put(HttpConstant.BASIC_PASSWORD_CONSTANT, basicPassword);
        keyValue.put(HttpConstant.OAUTH2_ENDPOINT_CONSTANT, oauth2Endpoint);
        keyValue.put(HttpConstant.OAUTH2_CLIENTID_CONSTANT, oauth2ClientId);
        keyValue.put(HttpConstant.OAUTH2_CLIENTSECRET_CONSTANT, oauth2ClientSecret);
        keyValue.put(HttpConstant.OAUTH2_HTTP_METHOD_CONSTANT, oauth2HttpMethod);
        keyValue.put(HttpConstant.PROXY_TYPE_CONSTANT, proxyType);
        keyValue.put(HttpConstant.PROXY_HOST_CONSTANT, proxyHost);
        keyValue.put(HttpConstant.PROXY_PORT_CONSTANT, proxyPort);
        keyValue.put(HttpConstant.PROXY_USER_CONSTANT, proxyUser);
        keyValue.put(HttpConstant.PROXY_PASSWORD_CONSTANT, proxyPassword);
        keyValue.put(HttpConstant.TIMEOUT_CONSTANT, timeout);
        keyValueList.add(keyValue);
        return keyValueList;
    }

    @Override
    public Class<? extends Task> taskClass() {
        return HttpSinkTask.class;
    }

    @Override
    public void validate(KeyValue config) {
        if (StringUtils.isBlank(config.getString(HttpConstant.URL_PATTERN_CONSTANT))
                || StringUtils.isBlank(config.getString(HttpConstant.AUTH_TYPE_CONSTANT))
                || StringUtils.isBlank(config.getString(HttpConstant.BODYS_CONSTANT))) {
            throw new RuntimeException("http required parameter is null !");
        }
        final List<AuthTypeEnum> collect = Arrays.stream(AuthTypeEnum.values()).filter(authTypeEnum -> authTypeEnum.getAuthType().equals(config.getString(HttpConstant.AUTH_TYPE_CONSTANT))).collect(Collectors.toList());
        if (collect.isEmpty()) {
            throw new RuntimeException("authType required parameter check is fail !");
        }
        try {
            URL urlConnect = new URL(config.getString(HttpConstant.URL_PATTERN_CONSTANT));
            URLConnection urlConnection = urlConnect.openConnection();
            urlConnection.setConnectTimeout(5000);
            urlConnection.connect();
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage());
        }
    }

    @Override
    public void init(KeyValue config) {
        urlPattern = config.getString(HttpConstant.URL_PATTERN_CONSTANT);
        method = config.getString(HttpConstant.METHOD_CONSTANT);
        queryStringParameters = config.getString(HttpConstant.QUERY_STRING_PARAMETERS_CONSTANT);
        headerParameters = config.getString(HttpConstant.HEADER_PARAMETERS_CONSTANT);
        bodys = config.getString(HttpConstant.BODYS_CONSTANT);
        authType = config.getString(HttpConstant.AUTH_TYPE_CONSTANT);
        basicUser = config.getString(HttpConstant.BASIC_USER_CONSTANT);
        basicPassword = config.getString(HttpConstant.BASIC_PASSWORD_CONSTANT);
        oauth2Endpoint = config.getString(HttpConstant.OAUTH2_ENDPOINT_CONSTANT);
        oauth2ClientId = config.getString(HttpConstant.OAUTH2_CLIENTID_CONSTANT);
        oauth2ClientSecret = config.getString(HttpConstant.OAUTH2_CLIENTSECRET_CONSTANT);
        oauth2HttpMethod = config.getString(HttpConstant.OAUTH2_HTTP_METHOD_CONSTANT);
        proxyType = config.getString(HttpConstant.PROXY_TYPE_CONSTANT);
        proxyHost = config.getString(HttpConstant.PROXY_HOST_CONSTANT);
        proxyPort = config.getString(HttpConstant.PROXY_PORT_CONSTANT);
        proxyUser = config.getString(HttpConstant.PROXY_USER_CONSTANT);
        proxyPassword = config.getString(HttpConstant.PROXY_PASSWORD_CONSTANT);
        timeout = config.getString(HttpConstant.TIMEOUT_CONSTANT);
    }

    @Override
    public void stop() {

    }
}
