package org.apache.rocketmq.connect.http.sink.entity;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;

public class OAuthEntity {
    private String oauth2Endpoint;
    private String oauth2ClientId;
    private String oauth2ClientSecret;
    private String oauth2HttpMethod;
    private String timeout;

    public String getTimeout() {
        return timeout;
    }

    public void setTimeout(String timeout) {
        this.timeout = timeout;
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

    public void setOauth2HttpMethod(String oauth2HttpMethod) {
        this.oauth2HttpMethod = oauth2HttpMethod;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;

        if (o == null || getClass() != o.getClass()) return false;

        OAuthEntity that = (OAuthEntity) o;

        return new EqualsBuilder().append(oauth2Endpoint, that.oauth2Endpoint).append(oauth2ClientId, that.oauth2ClientId).append(oauth2ClientSecret, that.oauth2ClientSecret).append(oauth2HttpMethod, that.oauth2HttpMethod).isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37).append(oauth2Endpoint).append(oauth2ClientId).append(oauth2ClientSecret).append(oauth2HttpMethod).toHashCode();
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("oauth2Endpoint", oauth2Endpoint)
                .append("oauth2ClientId", oauth2ClientId)
                .append("oauth2ClientSecret", oauth2ClientSecret)
                .append("oauth2HttpMethod", oauth2HttpMethod)
                .toString();
    }
}
