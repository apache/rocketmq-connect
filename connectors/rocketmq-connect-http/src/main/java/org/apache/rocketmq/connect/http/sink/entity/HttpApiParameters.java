package org.apache.rocketmq.connect.http.sink.entity;


import java.util.List;

public class HttpApiParameters {

    private String endpoint;

    private String method;

    private List<ApiParameter> apiParameters;

    public String getEndpoint() {
        return endpoint;
    }

    public void setEndpoint(String endpoint) {
        this.endpoint = endpoint;
    }

    public String getMethod() {
        return method;
    }

    public void setMethod(String method) {
        this.method = method;
    }

    public List<ApiParameter> getApiParameters() {
        return apiParameters;
    }

    public void setApiParameters(List<ApiParameter> apiParameters) {
        this.apiParameters = apiParameters;
    }
}
