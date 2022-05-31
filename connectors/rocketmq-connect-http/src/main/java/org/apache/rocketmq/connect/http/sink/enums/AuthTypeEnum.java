package org.apache.rocketmq.connect.http.sink.enums;


public enum AuthTypeEnum {

    /**
     * BASIC
     */
    BASIC("BASIC"),
    /**
     * OAUTH_CLIENT_CREDENTIALS
     */
    OAUTH_CLIENT_CREDENTIALS("OAUTH_CLIENT_CREDENTIALS"),
    /**
     * API_KEY
     */
    API_KEY("API_KEY");
    private final String authType;

    AuthTypeEnum(String authType) {
        this.authType = authType;
    }

    public String getAuthType() {
        return authType;
    }
}
