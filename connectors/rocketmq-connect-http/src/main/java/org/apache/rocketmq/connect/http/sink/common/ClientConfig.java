package org.apache.rocketmq.connect.http.sink.common;

public class ClientConfig {

    private String urlPattern;
    private String method;
    private String queryStringParameters;
    private String headerParameters;
    private String bodys;
    private String httpPathValue;
    private String authType;
    private String basicUser;
    private String basicPassword;
    private String apiKeyName;
    private String apiKeyValue;
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

    public String getProxyType() {
        return proxyType;
    }

    public void setProxyType(String proxyType) {
        this.proxyType = proxyType;
    }

    public String getProxyHost() {
        return proxyHost;
    }

    public void setProxyHost(String proxyHost) {
        this.proxyHost = proxyHost;
    }

    public String getProxyPort() {
        return proxyPort;
    }

    public void setProxyPort(String proxyPort) {
        this.proxyPort = proxyPort;
    }

    public String getProxyUser() {
        return proxyUser;
    }

    public void setProxyUser(String proxyUser) {
        this.proxyUser = proxyUser;
    }

    public String getProxyPassword() {
        return proxyPassword;
    }

    public void setProxyPassword(String proxyPassword) {
        this.proxyPassword = proxyPassword;
    }

    public String getUrlPattern() {
        return urlPattern;
    }

    public void setUrlPattern(String urlPattern) {
        this.urlPattern = urlPattern;
    }

    public String getMethod() {
        return method;
    }

    public void setMethod(String method) {
        this.method = method;
    }

    public String getQueryStringParameters() {
        return queryStringParameters;
    }

    public void setQueryStringParameters(String queryStringParameters) {
        this.queryStringParameters = queryStringParameters;
    }

    public String getHeaderParameters() {
        return headerParameters;
    }

    public void setHeaderParameters(String headerParameters) {
        this.headerParameters = headerParameters;
    }

    public String getBodys() {
        return bodys;
    }

    public void setBodys(String bodys) {
        this.bodys = bodys;
    }

    public String getAuthType() {
        return authType;
    }

    public void setAuthType(String authType) {
        this.authType = authType;
    }

    public String getBasicUser() {
        return basicUser;
    }

    public void setBasicUser(String basicUser) {
        this.basicUser = basicUser;
    }

    public String getBasicPassword() {
        return basicPassword;
    }

    public void setBasicPassword(String basicPassword) {
        this.basicPassword = basicPassword;
    }

    public String getOauth2Endpoint() {
        return oauth2Endpoint;
    }

    public void setOauth2Endpoint(String oauth2Endpoint) {
        this.oauth2Endpoint = oauth2Endpoint;
    }

    public String getOauth2ClientId() {
        return oauth2ClientId;
    }

    public void setOauth2ClientId(String oauth2ClientId) {
        this.oauth2ClientId = oauth2ClientId;
    }

    public String getOauth2ClientSecret() {
        return oauth2ClientSecret;
    }

    public void setOauth2ClientSecret(String oauth2ClientSecret) {
        this.oauth2ClientSecret = oauth2ClientSecret;
    }

    public String getOauth2HttpMethod() {
        return oauth2HttpMethod;
    }

    public String getHttpPathValue() {
        return httpPathValue;
    }

    public void setHttpPathValue(String httpPathValue) {
        this.httpPathValue = httpPathValue;
    }

    public void setOauth2HttpMethod(String oauth2HttpMethod) {
        this.oauth2HttpMethod = oauth2HttpMethod;
    }

    public String getTimeout() {
        return timeout;
    }

    public void setTimeout(String timeout) {
        this.timeout = timeout;
    }

    public String getApiKeyName() {
        return apiKeyName;
    }

    public void setApiKeyName(String apiKeyName) {
        this.apiKeyName = apiKeyName;
    }

    public String getApiKeyValue() {
        return apiKeyValue;
    }

    public void setApiKeyValue(String apiKeyValue) {
        this.apiKeyValue = apiKeyValue;
    }
}
