package org.apache.rocketmq.connect.http.sink.constant;


public enum AuthTypeEnum {

    /**
     * BASIC
     */
    BASIC("BASIC_AUTH"),
    /**
     * OAUTH_CLIENT_CREDENTIALS
     */
    OAUTH_CLIENT_CREDENTIALS("OAUTH_AUTH"),
    /**
     * API_KEY
     */
    API_KEY("API_KEY_AUTH");
    private final String authType;

    AuthTypeEnum(String authType) {
        this.authType = authType;
    }

    public String getAuthType() {
        return authType;
    }
}
