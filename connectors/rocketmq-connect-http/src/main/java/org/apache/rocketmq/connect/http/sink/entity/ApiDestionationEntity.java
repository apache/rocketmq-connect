package org.apache.rocketmq.connect.http.sink.entity;

import org.apache.commons.lang3.builder.ToStringBuilder;

public class ApiDestionationEntity {

    private String code;
    private String message;
    private String apiDestinationName;
    private String connectionName;
    private String description;
    private HttpApiParameters httpApiParameters;

    public String getCode() {
        return code;
    }

    public void setCode(String code) {
        this.code = code;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public String getApiDestinationName() {
        return apiDestinationName;
    }

    public void setApiDestinationName(String apiDestinationName) {
        this.apiDestinationName = apiDestinationName;
    }

    public String getConnectionName() {
        return connectionName;
    }

    public void setConnectionName(String connectionName) {
        this.connectionName = connectionName;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public HttpApiParameters getHttpApiParameters() {
        return httpApiParameters;
    }

    public void setHttpApiParameters(HttpApiParameters httpApiParameters) {
        this.httpApiParameters = httpApiParameters;
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("code", code)
                .append("message", message)
                .append("apiDestinationName", apiDestinationName)
                .append("connectionName", connectionName)
                .append("description", description)
                .append("httpApiParameters", httpApiParameters)
                .toString();
    }
}
