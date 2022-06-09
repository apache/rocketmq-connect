package org.apache.rocketmq.connect.http.sink;

import io.openmessaging.KeyValue;
import io.openmessaging.connector.api.component.task.Task;
import io.openmessaging.connector.api.component.task.sink.SinkConnector;
import io.openmessaging.internal.DefaultKeyValue;
import org.apache.rocketmq.connect.http.sink.constant.HttpConstant;
import org.apache.rocketmq.connect.http.sink.utils.CheckUtils;

import java.util.ArrayList;
import java.util.List;

public class HttpSinkConnector extends SinkConnector {

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
    protected String timeout;
    protected String apiKeyName;
    protected String apiKeyValue;

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
    }

    @Override
    public void init(KeyValue config) {
        urlPattern = CheckUtils.checkNullReturnDefault(config.getString(HttpConstant.URL_PATTERN_CONSTANT));
        method = CheckUtils.checkNullReturnDefault(config.getString(HttpConstant.METHOD_CONSTANT));
        queryStringParameters = CheckUtils.checkNullReturnDefault(config.getString(HttpConstant.QUERY_STRING_PARAMETERS_CONSTANT));
        headerParameters = CheckUtils.checkNullReturnDefault(config.getString(HttpConstant.HEADER_PARAMETERS_CONSTANT));
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
    }

    @Override
    public void stop() {

    }
}
