package org.apache.rocketmq.connect.http.sink.entity;


import org.apache.commons.lang3.builder.ToStringBuilder;

public class AuthParameters {

    private String authorizationType;

    private ApiKeyAuthParameters apiKeyAuthParameters;

    private BasicAuthParameters basicAuthParameters;

    private InvocationHttpParameters invocationHttpParameters;

    private OAuthParameters oauthParameters;

    public String getAuthorizationType() {
        return authorizationType;
    }

    public void setAuthorizationType(String authorizationType) {
        this.authorizationType = authorizationType;
    }

    public ApiKeyAuthParameters getApiKeyAuthParameters() {
        return apiKeyAuthParameters;
    }

    public void setApiKeyAuthParameters(ApiKeyAuthParameters apiKeyAuthParameters) {
        this.apiKeyAuthParameters = apiKeyAuthParameters;
    }

    public BasicAuthParameters getBasicAuthParameters() {
        return basicAuthParameters;
    }

    public void setBasicAuthParameters(BasicAuthParameters basicAuthParameters) {
        this.basicAuthParameters = basicAuthParameters;
    }

    public InvocationHttpParameters getInvocationHttpParameters() {
        return invocationHttpParameters;
    }

    public void setInvocationHttpParameters(InvocationHttpParameters invocationHttpParameters) {
        this.invocationHttpParameters = invocationHttpParameters;
    }

    public OAuthParameters getOauthParameters() {
        return oauthParameters;
    }

    public void setOauthParameters(OAuthParameters oauthParameters) {
        this.oauthParameters = oauthParameters;
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("authorizationType", authorizationType)
                .append("apiKeyAuthParameters", apiKeyAuthParameters)
                .append("basicAuthParameters", basicAuthParameters)
                .append("invocationHttpParameters", invocationHttpParameters)
                .append("oauthParameters", oauthParameters)
                .toString();
    }
}
