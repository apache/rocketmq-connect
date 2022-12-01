package org.apache.rocketmq.connect.http.sink.entity;

import org.apache.commons.lang3.builder.ToStringBuilder;

public class TokenEntity {

    private String accessToken;
    private String tokenType;
    private int expiresIn;
    private String exampleParameter;
    private String timestamp;
    private String status;
    private String error;
    private String message;
    private String path;
    private String tokenTimestamp;

    private String scope;

    public String getAccessToken() {
        return accessToken;
    }

    public void setAccessToken(String accessToken) {
        this.accessToken = accessToken;
    }

    public String getTokenType() {
        return tokenType;
    }

    public void setTokenType(String tokenType) {
        this.tokenType = tokenType;
    }

    public int getExpiresIn() {
        return expiresIn;
    }

    public void setExpiresIn(int expiresIn) {
        this.expiresIn = expiresIn;
    }

    public String getTokenTimestamp() {
        return tokenTimestamp;
    }

    public void setTokenTimestamp(String tokenTimestamp) {
        this.tokenTimestamp = tokenTimestamp;
    }

    public String getExampleParameter() {
        return exampleParameter;
    }

    public void setExampleParameter(String exampleParameter) {
        this.exampleParameter = exampleParameter;
    }

    public String getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(String timestamp) {
        this.timestamp = timestamp;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public String getError() {
        return error;
    }

    public void setError(String error) {
        this.error = error;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }

    public String getScope() {
        return scope;
    }

    public void setScope(String scope) {
        this.scope = scope;
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("accessToken", accessToken)
                .append("tokenType", tokenType)
                .append("expiresIn", expiresIn)
                .append("exampleParameter", exampleParameter)
                .append("timestamp", timestamp)
                .append("status", status)
                .append("error", error)
                .append("message", message)
                .append("path", path)
                .append("tokenTimestamp", tokenTimestamp)
                .append("scope", scope)
                .toString();
    }
}
