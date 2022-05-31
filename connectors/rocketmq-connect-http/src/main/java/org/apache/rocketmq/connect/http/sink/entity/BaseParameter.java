package org.apache.rocketmq.connect.http.sink.entity;


public class BaseParameter {

    private String isValueSecret;

    private String key;

    private String value;

    public String getIsValueSecret() {
        return isValueSecret;
    }

    public void setIsValueSecret(String isValueSecret) {
        this.isValueSecret = isValueSecret;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }
}
