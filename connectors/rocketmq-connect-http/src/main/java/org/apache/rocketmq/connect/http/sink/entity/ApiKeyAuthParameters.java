package org.apache.rocketmq.connect.http.sink.entity;

public class ApiKeyAuthParameters {

    private String apiKeyName;

    private String apiKeyValue;

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
