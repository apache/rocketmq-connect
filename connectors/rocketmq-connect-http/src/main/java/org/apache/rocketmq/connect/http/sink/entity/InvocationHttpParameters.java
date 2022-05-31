package org.apache.rocketmq.connect.http.sink.entity;


import java.util.List;

public class InvocationHttpParameters {

    private List<BodyParameter> bodyParameters;

    private List<HeaderParameter> headerParameters;

    private List<QueryStringParameter> queryStringParameters;

    public List<BodyParameter> getBodyParameters() {
        return bodyParameters;
    }

    public void setBodyParameters(List<BodyParameter> bodyParameters) {
        this.bodyParameters = bodyParameters;
    }

    public List<HeaderParameter> getHeaderParameters() {
        return headerParameters;
    }

    public void setHeaderParameters(List<HeaderParameter> headerParameters) {
        this.headerParameters = headerParameters;
    }

    public List<QueryStringParameter> getQueryStringParameters() {
        return queryStringParameters;
    }

    public void setQueryStringParameters(List<QueryStringParameter> queryStringParameters) {
        this.queryStringParameters = queryStringParameters;
    }
}
